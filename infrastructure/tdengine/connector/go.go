// +build !windows

package connector

import (
	"bailongma/v2/blm_openfalcon/pool"
	"bailongma/v2/infrastructure/tdengine/common"
	tdengineConfig "bailongma/v2/infrastructure/tdengine/config"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/taosdata/driver-go/taosSql"
	"reflect"
	"sync"
	"time"
)

type GoConnector struct {
	db *sql.DB
}

func NewGoConnector(conf *tdengineConfig.TDengineGo) (*GoConnector, error) {
	fmt.Println(conf.Address)
	db, err := sql.Open("taosSql", conf.Address)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Second * time.Duration(conf.MaxLifetime))
	db.SetMaxIdleConns(conf.MaxIdle)
	db.SetMaxOpenConns(conf.MaxOpen)
	return &GoConnector{db: db}, err
}

func (g *GoConnector) Exec(ctx context.Context, sql string) (int64, error) {
	var err error
	r, err := g.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, g.changeError(err)
	}
	return r.RowsAffected()
}

var (
	nullInt8    = reflect.TypeOf(taosSql.NullInt8{})
	nullInt16   = reflect.TypeOf(taosSql.NullInt16{})
	nullInt32   = reflect.TypeOf(taosSql.NullInt32{})
	nullInt64   = reflect.TypeOf(taosSql.NullInt64{})
	nullUInt8   = reflect.TypeOf(taosSql.NullUInt8{})
	nullUInt16  = reflect.TypeOf(taosSql.NullUInt16{})
	nullUInt32  = reflect.TypeOf(taosSql.NullUInt32{})
	nullUInt64  = reflect.TypeOf(taosSql.NullUInt64{})
	nullFloat32 = reflect.TypeOf(taosSql.NullFloat32{})
	nullFloat64 = reflect.TypeOf(taosSql.NullFloat64{})
	nullTime    = reflect.TypeOf(taosSql.NullTime{})
	nullBool    = reflect.TypeOf(taosSql.NullBool{})
	nullString  = reflect.TypeOf(taosSql.NullString{})
)

func (g *GoConnector) Query(ctx context.Context, q string) (*Data, error) {
	var err error
	rows, err := g.db.QueryContext(ctx, q)
	if err != nil {
		return nil, g.changeError(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, g.changeError(err)
	}
	result := &Data{}
	result.Head = columns
	tt, err := rows.ColumnTypes()
	if err != nil {
		return nil, g.changeError(err)
	}
	types := make([]reflect.Type, len(columns))
	for i, columnType := range tt {
		switch columnType.DatabaseTypeName() {
		case "BOOL":
			types[i] = nullBool
		case "TINYINT":
			types[i] = nullInt8
		case "SMALLINT":
			types[i] = nullInt16
		case "INT":
			types[i] = nullInt32
		case "BIGINT":
			types[i] = nullInt64
		case "TINYINT UNSIGNED":
			types[i] = nullUInt8
		case "SMALLINT UNSIGNED":
			types[i] = nullUInt16
		case "INT UNSIGNED":
			types[i] = nullUInt32
		case "BIGINT UNSIGNED":
			types[i] = nullUInt64
		case "FLOAT":
			types[i] = nullFloat32
		case "DOUBLE":
			types[i] = nullFloat64
		case "BINARY":
			fallthrough
		case "NCHAR":
			types[i] = nullString
		case "TIMESTAMP":
			types[i] = nullTime
		}
	}
	var dbResult [][]interface{}
	for rows.Next() {
		scanValues := make([]interface{}, len(columns))
		for i := range scanValues {
			scanValues[i] = reflect.New(types[i]).Interface()
		}
		err = rows.Scan(scanValues...)
		if err != nil {
			return nil, g.changeError(err)
		}
		dbResult = append(dbResult, scanValues)
	}
	//处理速度耗时，分片处理
	result.Data = make([][]interface{}, len(dbResult))
	batch := 10000
	partitionCount := len(dbResult) / batch
	rest := len(dbResult) % batch
	if rest > 0 {
		partitionCount += 1
	}
	partitionChan := make(chan *info, partitionCount)
	finishChan := make(chan struct{})
	defer func() {
		finishChan <- struct{}{}
	}()
	wg := sync.WaitGroup{}
	go func() {
		for true {
			select {
			case partitionInfo := <-partitionChan:
				for rowIndex, items := range partitionInfo.result {
					result.Data[partitionInfo.index*batch+rowIndex] = items
				}
				wg.Done()
			case <-finishChan:
				return
			}
		}
	}()
	for i := 0; i < partitionCount; i++ {
		var d [][]interface{}
		if i == partitionCount-1 {
			d = dbResult[batch*i:]
		} else {
			d = dbResult[batch*i : batch*(i+1)]
		}
		index := i
		wg.Add(1)
		poolError := pool.GoroutinePool.Submit(func() {
			tmp, _ := g.dealResult(len(columns), d)
			partitionChan <- &info{
				index:  index,
				result: tmp,
			}
		})
		if poolError != nil {
			return nil, poolError
		}
	}
	wg.Wait()
	return result, nil
}

type info struct {
	index  int
	result [][]interface{}
}

func (g *GoConnector) dealResult(columnLen int, data [][]interface{}) ([][]interface{}, error) {
	result := make([][]interface{}, len(data))
	for rowIndex, scanValues := range data {
		tmp := make([]interface{}, columnLen)
		for i := 0; i < columnLen; i++ {
			v, err := scanValues[i].(driver.Valuer).Value()
			if err != nil {
				return nil, err
			}
			tmp[i] = v
		}
		result[rowIndex] = tmp
	}
	return result, nil
}

func (g *GoConnector) changeError(err error) error {
	if err == nil {
		return nil
	}
	var taosError *taosSql.TaosError
	if errors.As(err, &taosError) {
		return &common.TDengineError{
			Code: int(taosError.Code),
			Desc: taosError.ErrStr,
		}
	}
	return err
}
