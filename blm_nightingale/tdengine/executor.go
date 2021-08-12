package tdengine

import (
	"context"
	"errors"
	"github.com/taosdata/Bailongma/blm_nightingale/config"
	"github.com/taosdata/Bailongma/blm_nightingale/log"
	"github.com/taosdata/Bailongma/blm_nightingale/model"
	tdengineErrors "github.com/taosdata/driver-go/errors"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/connector"
	tdengineExecutor "github.com/taosdata/go-utils/tdengine/executor"
	"github.com/taosdata/go-utils/util"
	"strconv"
	"strings"
	"time"
)

const (
	EndpointTagKey = "_ident"
	MaxColLen      = 64
	MaxTableLen    = 192
)

var tagScheme = []*tdengineExecutor.FieldInfo{
	{
		Name:   EndpointTagKey,
		Type:   common.BINARYType,
		Length: config.Conf.MaxEndpointLength,
	},
}

var valueField = []*tdengineExecutor.FieldInfo{
	{
		Name: "value",
		Type: common.DOUBLEType,
	},
}

var ExecutorGroup []*Executor

type Executor struct {
	executor   *tdengineExecutor.Executor
	index      int
	insertChan chan []*model.MetricPoint
	ctx        context.Context
}

func (e *Executor) startTask() {
	for i := 0; i < config.Conf.InsertWorkerPreDBGroup; i++ {
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

func (e *Executor) InsertData(points []*model.MetricPoint) {
	e.insertChan <- points
}

func (e *Executor) doInsertData(points []*model.MetricPoint) {
	if len(points) == 0 {
		return
	}
	stableName := Escape(points[0].Metric, MaxTableLen)
	sqlList := make([]string, 0, len(points))
	for _, point := range points {
		escapeTagMap := make(map[string]struct{}, len(point.TagsList))

		tags := make([]string, 0, 1+len(point.TagsList))
		tags = append(tags, EndpointTagKey)
		tagValues := make([]string, 0, 1+len(point.TagsList))
		tagValues = append(tagValues, util.EscapeString(point.Ident))
		for _, s := range point.TagsList {
			escapeTag := Escape(s, MaxColLen)
			_, exist := escapeTagMap[escapeTag]
			if exist {
				//todo escape之后可能存在重复tag 先不处理直接过滤掉
				continue
			} else {
				escapeTagMap[escapeTag] = struct{}{}
				tags = append(tags, escapeTag)
			}
			tagValues = append(tagValues, util.EscapeString(point.TagsMap[s]))
		}
		sqlList = append(sqlList, e.generateInsertSql(point.TableName, stableName, tags, tagValues, point.Time, point.Value))
	}
	b := pool.BytesPoolGet()
	b.WriteString("insert into ")
	for _, s := range sqlList {
		b.WriteString(s)
	}
	sql := b.String()
	pool.BytesPoolPut(b)
	err := e.doExec(sql)
	if err != nil {
		var tdErr *common.TDengineError
		if errors.As(err, &tdErr) {
			tagMap := map[string]struct{}{}
			for _, point := range points {
				for _, s := range point.TagsList {
					tagMap[Escape(s, MaxColLen)] = struct{}{}
				}
			}
			switch int32(tdErr.Code) {
			case tdengineErrors.MND_INVALID_TABLE_NAME:
				//超级表不存在
				//创建超级表
				tagList := make([]string, 0, len(tagMap))
				for s := range tagMap {
					tagList = append(tagList, s)
				}
				err = e.createStable(stableName, tagList)
				if err != nil {
					return
				}
				err = e.modifyTag(stableName, tagMap)
				if err != nil {
					return
				}
			case tdengineErrors.TSC_INVALID_OPERATION:
				//tag 不存在
				err = e.modifyTag(stableName, tagMap)
				if err != nil {
					return
				}
			default:
				log.Logger.WithError(tdErr).WithField("sql", sql).Error("insert sql error")
				return
			}
		} else {
			log.Logger.WithError(err).WithField("sql", sql).Error("insert sql error")
			return
		}
		err = e.doExec(sql)
		if err != nil {
			log.Logger.WithError(err).WithField("sql", sql).Error("reinsert sql error")
			return
		}
	}
}

func (e *Executor) modifyTag(stableName string, tagMap map[string]struct{}) error {
	tableInfo, err := e.executor.DescribeTable(e.ctx, stableName)
	if err != nil {
		log.Logger.WithError(err).Errorf("describe stable %s error", stableName)
		return err
	}
	for _, tag := range tableInfo.Tags {
		delete(tagMap, tag.Name)
	}
	if len(tagMap) > 0 {
		for tag := range tagMap {
			err := e.executor.AddTag(e.ctx, stableName, &tdengineExecutor.FieldInfo{
				Name:   tag,
				Type:   common.BINARYType,
				Length: config.Conf.MaxTagLength,
			})
			if err != nil {
				var addTagErr *common.TDengineError
				if errors.As(err, &addTagErr) {
					if addTagErr.Code == int(tdengineErrors.TSC_INVALID_OPERATION) {
						//字段错误说明已存在
						continue
					} else {
						log.Logger.WithError(addTagErr).Errorf("stable %s add tag %s error", stableName, tag)
						return addTagErr
					}
				} else {
					log.Logger.WithError(err).Errorf("stable %s add tag %s error", stableName, tag)
					return err
				}
			}
		}
	}
	return nil
}

func (e *Executor) doExec(sql string) error {
	_, err := e.executor.DoExec(e.ctx, sql)
	return err
}

func (e *Executor) createStable(stableName string, tagList []string) error {
	tags := make([]*tdengineExecutor.FieldInfo, 0, 1+len(tagList))
	tags = append(tags, tagScheme...)
	for _, s := range tagList {
		tags = append(tags, &tdengineExecutor.FieldInfo{
			Name:   s,
			Type:   common.BINARYType,
			Length: config.Conf.MaxTagLength,
		})
	}
	err := e.executor.CreateSTable(e.ctx, stableName, &tdengineExecutor.TableInfo{
		Fields: valueField,
		Tags:   tags,
	})
	if err != nil {
		//返回错误
		var tdErr *common.TDengineError
		if errors.As(err, &tdErr) {
			switch int32(tdErr.Code) {
			case tdengineErrors.MND_TABLE_ALREADY_EXIST:
				//表已经创建,返回正常
			default:
				log.Logger.WithError(tdErr).Error("create stable error")
				return tdErr
			}
		} else {
			log.Logger.WithError(err).Error("create stable error")
			return err
		}
	}
	return nil
}

func (e *Executor) generateInsertSql(tableName string, stableName string, tagFields []string, tagValues []string, ts time.Time, value float64) string {
	//insert into table using stable () tags() values()
	b := pool.BytesPoolGet()
	b.WriteString(e.executor.WithDBName(tableName))
	b.WriteString(" using ")
	b.WriteString(e.executor.WithDBName(stableName))
	b.WriteString(" (")
	b.WriteString(strings.Join(tagFields, ","))
	b.WriteString(")")
	b.WriteString(" tags ('")
	b.WriteString(strings.Join(tagValues, "','"))
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

func Escape(s string, maxLen int) string {
	if len(s) == 0 {
		return ""
	}
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteByte('_')
	stableNameLen := maxLen - 1
	if len(s) < maxLen-1 {
		stableNameLen = len(s)
	}
	b.Grow(stableNameLen)
	for i := 0; i < stableNameLen; i++ {
		c := s[i]
		if (c <= 'Z' && c >= 'A') || (c <= 'z' && c >= 'a') || (c <= '9' && c >= '0') || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func Unescape(s string) string {
	if len(s) == 0 {
		return ""
	}
	return s[1:]
}

func init() {
	ExecutorGroup = make([]*Executor, len(config.TDengineConfig))
	for i, c := range config.TDengineConfig {
		tdengineConnector, err := connector.NewTDengineConnector(config.Conf.TDengineConnType, c)
		if err != nil {
			log.Logger.WithError(err).Panic("create TDengine connector error")
		}
		executor := tdengineExecutor.NewExecutor(tdengineConnector, config.Conf.DBName, config.Conf.ShowSQL, log.Logger)

		e := &Executor{
			executor:   executor,
			index:      i,
			insertChan: make(chan []*model.MetricPoint, config.Conf.InsertWorkerPreDBGroup*2),
			ctx:        context.Background(),
		}
		e.createDB()
		e.setPrecision()
		e.startTask()
		ExecutorGroup[i] = e
	}
}
