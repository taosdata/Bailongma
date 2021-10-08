package tdengine

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/Bailongma/blm_nightingale/config"
	"github.com/taosdata/Bailongma/blm_nightingale/log"
	"github.com/taosdata/Bailongma/blm_nightingale/model"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/connector"
	tdengineExecutor "github.com/taosdata/go-utils/tdengine/executor"
	"sort"
	"strconv"
	"strings"
	"time"
)

const TableName = "metrics"

var e *Executor
var valueField = []*tdengineExecutor.FieldInfo{
	{
		Name: "value",
		Type: common.DOUBLEType,
	},
}
var tagField = []*tdengineExecutor.FieldInfo{
	{
		Name: "labels",
		Type: "json(512)",
	},
}

type Executor struct {
	executor   *tdengineExecutor.Executor
	insertChan chan []*model.MetricPoint
	ctx        context.Context
}

func (e *Executor) startTask() {
	for i := 0; i < config.Conf.StoreWorker; i++ {
		poolErr := pool.GoroutinePool.Submit(
			func() {
				for {
					select {
					case data := <-e.insertChan:
						e.doInsertData(data)
					}
				}
			},
		)
		if poolErr != nil {
			panic(poolErr)
		}
	}
}

func (e *Executor) doInsertData(points []*model.MetricPoint) {
	if len(points) == 0 {
		return
	}
	sqlList := make([]string, 0, len(points))
	for _, point := range points {
		tableName := getTableName(point)
		tags, err := json.Marshal(point.TagsMap)
		if err != nil {
			log.Logger.WithError(err).Error("json marshal")
			continue
		}
		sqlList = append(sqlList, e.generateInsertSql(tableName, TableName, string(tags), point.Time, point.Value))
	}
	b := pool.BytesPoolGet()
	b.WriteString("insert into")
	for _, s := range sqlList {
		b.WriteByte(' ')
		b.WriteString(s)
	}
	sql := b.String()
	pool.BytesPoolPut(b)
	err := e.doExec(sql)
	if err != nil {
		log.Logger.WithError(err).Error("insert data")
	}
}

func getTableName(point *model.MetricPoint) string {
	b := pool.BytesPoolGet()
	tags := make([]string, len(point.TagsMap))
	for k, v := range point.TagsMap {
		tags = append(tags, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(tags)
	b.WriteString(strings.Join(tags, "/"))
	return fmt.Sprintf("md5_%x", md5.Sum(b.Bytes()))
}

func (e *Executor) doExec(sql string) error {
	_, err := e.executor.DoExec(e.ctx, sql)
	return err
}

func (e *Executor) createStable() error {
	err := e.executor.CreateSTable(e.ctx, TableName, &tdengineExecutor.TableInfo{
		Fields: valueField,
		Tags:   tagField,
	})
	if err != nil {
		//返回错误
		log.Logger.WithError(err).Error("create stable error")
		return err
	}
	return nil
}

func (e *Executor) generateInsertSql(tableName string, stableName string, tagValues string, ts time.Time, value float64) string {
	//insert into table using stable () tags() values()
	b := pool.BytesPoolGet()
	b.WriteString(e.executor.WithDBName(tableName))
	b.WriteString(" using ")
	b.WriteString(e.executor.WithDBName(stableName))
	b.WriteString(" tags ('")
	b.WriteString(tagValues)
	b.WriteString("') values ('")
	b.WriteString(formatTime(ts))
	b.WriteString("',")
	b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
	b.WriteString(") ")
	sql := b.String()
	pool.BytesPoolPut(b)
	return sql
}

func formatTime(t time.Time) string {
	return t.In(time.Local).Format(time.RFC3339Nano)
}

func (e *Executor) createDB() {
	err := e.executor.CreateDatabase(e.ctx, config.Conf.DataKeep, config.Conf.Update)
	if err != nil {
		panic(err)
	}
	err = e.executor.AlterDatabase(e.ctx, "CACHELAST", config.Conf.CacheLast)
	if err != nil {
		panic(err)
	}
}

func (e *Executor) setPrecision() {
	precision, err := e.executor.GetPrecision(e.ctx)
	if err != nil {
		panic(err)
	}
	layout := "2006-01-02 15:04:05.999"
	switch precision {
	case "us":
		layout = "2006-01-02 15:04:05.999999"
	case "ns":
		layout = "2006-01-02 15:04:05.999999999"
	}
	e.executor.SetTimeLayout(layout)
}

func Query(query *prompb.Query) (*connector.Data, error) {
	b := pool.BytesPoolGet()
	b.WriteString("select * from ")
	b.WriteString(e.executor.WithDBName(TableName))
	b.WriteString(" where ts >= '")
	b.WriteString(ms2Time(query.StartTimestampMs))
	b.WriteString("' and ts <= '")
	b.WriteString(ms2Time(query.EndTimestampMs))
	b.WriteByte('\'')
	for _, matcher := range query.Matchers {
		b.WriteString(" and ")
		column := matcher.Name
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			b.WriteString("labels->'")
			b.WriteString(column)
			b.WriteString("'='")
			b.WriteString(matcher.Value)
			b.WriteByte('\'')
		case prompb.LabelMatcher_NEQ:
			b.WriteString("labels->'")
			b.WriteString(column)
			b.WriteString("'!='")
			b.WriteString(matcher.Value)
			b.WriteByte('\'')
		case prompb.LabelMatcher_RE:
			b.WriteString("labels->'")
			b.WriteString(column)
			b.WriteString("'match'")
			b.WriteString(matcher.Value)
			b.WriteByte('\'')
		case prompb.LabelMatcher_NRE:
			b.WriteString("labels->'")
			b.WriteString(column)
			b.WriteString("'nmatch'")
			b.WriteString(matcher.Value)
			b.WriteByte('\'')
		default:
			return nil, errors.New("not support match type")
		}
	}
	b.WriteString(" order by ts desc")
	sql := b.String()
	pool.BytesPoolPut(b)
	return e.executor.DoQuery(e.ctx, sql)
}
func ms2Time(ts int64) string {
	return time.Unix(0, ts*1e6).Local().Format(time.RFC3339Nano)
}
func InsertData(points []*model.MetricPoint) {
	if len(points) == 0 {
		return
	}
	e.insertChan <- points
}
func init() {
	tdengineConnector, err := connector.NewTDengineConnector(config.Conf.TDengineConnType, config.TDengineConfig)
	if err != nil {
		log.Logger.WithError(err).Panic("create TDengine connector error")
	}
	executor := tdengineExecutor.NewExecutor(tdengineConnector, config.Conf.DBName, config.Conf.ShowSQL, log.Logger)

	e = &Executor{
		executor:   executor,
		insertChan: make(chan []*model.MetricPoint, config.Conf.StoreWorker*2),
		ctx:        context.Background(),
	}
	e.createDB()
	e.setPrecision()
	err = e.createStable()
	if err != nil {
		panic(err)
	}
	e.startTask()
}
