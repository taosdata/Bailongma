package tdengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	"github.com/taosdata/Bailongma/blm_openfalcon/config"
	"github.com/taosdata/Bailongma/blm_openfalcon/log"
	"github.com/taosdata/Bailongma/blm_openfalcon/metricgroup"
	"github.com/taosdata/Bailongma/blm_openfalcon/model"
	"github.com/taosdata/Bailongma/blm_openfalcon/pool"
	utilspool "github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/connector"
	tdengineExecutor "github.com/taosdata/go-utils/tdengine/executor"
	"github.com/taosdata/go-utils/util"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ctx      = context.Background()
	executor *tdengineExecutor.Executor
)

const (
	EndpointTagKey  = "openfalcon_endpoint"
	MetricTagKey    = "openfalcon_metric"
	TagMetricTagKey = "openfalcon_tag"
)

var tagScheme = []*tdengineExecutor.FieldInfo{
	{
		Name:   EndpointTagKey,
		Type:   common.BINARYType,
		Length: config.Conf.MaxEndpointLength,
	}, {
		Name:   MetricTagKey,
		Type:   common.BINARYType,
		Length: config.Conf.MaxMetricLength,
	}, {
		Name:   TagMetricTagKey,
		Type:   common.BINARYType,
		Length: config.Conf.MaxTagLength,
	},
}

func InsertSingleColumnMetric(metricSingleMap map[string][]*model.MetricValue) error {
	var returnErr error
	wg := sync.WaitGroup{}
	for tn, ml := range metricSingleMap {
		tableName := tn
		metricList := ml
		wg.Add(1)
		poolErr := utilspool.GoroutinePool.Submit(func() {
			defer wg.Done()
			sort.Slice(metricList, func(i, j int) bool {
				return metricList[i].Timestamp < metricList[j].Timestamp
			})
			var values []string
			for _, metric := range metricList {
				value, err := metric.ParseValue()
				if err != nil {
					log.Logger.WithError(err).Errorf("parse value error %#v", value)
					continue
				}
				values = append(values, fmt.Sprintf("%s,%f", second2String(metric.Timestamp), value))
			}
			b := utilspool.BytesPoolGet()
			b.WriteByte('\'')
			b.WriteString(util.EscapeString(metricList[0].Endpoint))
			b.WriteString("','")
			b.WriteString(util.EscapeString(metricList[0].Metric))
			b.WriteString("','")
			b.WriteString(util.EscapeString(metricList[0].Tags))
			b.WriteByte('\'')
			tags := b.String()
			utilspool.BytesPoolPut(b)
			err := executor.InsertUsingSTable(
				ctx,
				util.ToHashString(tableName),
				"openfalcon_single_stable",
				tags,
				values,
			)
			if err != nil {
				returnErr = err
				log.Logger.WithError(err).Error("insert data error")
			}
		})
		if poolErr != nil {
			wg.Done()
			returnErr = poolErr
		}
	}
	wg.Wait()
	return returnErr
}

func InsertCommonGroupMetric(metricGroupMap map[string][]*model.MetricValue) error {
	wg := sync.WaitGroup{}
	for sTableName, ms := range metricGroupMap {
		fields := metricgroup.GetMetricGroupField(sTableName)
		fieldMap := map[string]int{}
		for i, field := range fields {
			fieldMap[util.ToHashString(field)] = i + 1
		}
		fieldNum := len(fields) + 1
		wg.Add(1)
		metrics := ms
		tb := sTableName
		poolErr := utilspool.GoroutinePool.Submit(func() {
			process(tb, metrics, fieldNum, fieldMap)
			wg.Done()
		})
		if poolErr != nil {
			wg.Done()
		}
	}
	wg.Wait()
	return nil
}

func process(sTableName string, metrics []*model.MetricValue, fieldNum int, fieldMap map[string]int) {
	tableTagMap := map[string][]string{}
	tableValueMap := map[string]map[int64][]string{}
	for _, metric := range metrics {
		b := utilspool.BytesPoolGet()
		b.WriteString(metric.Endpoint)
		b.WriteByte('/')
		b.WriteString(sTableName)
		b.WriteByte('/')
		b.WriteString(metric.Tags)
		tableName := b.String()
		utilspool.BytesPoolPut(b)
		tableTagMap[tableName] = []string{util.EscapeString(metric.Endpoint), util.EscapeString(sTableName), util.EscapeString(metric.Tags)}
		value, err := metric.ParseValue()
		if err != nil {
			log.Logger.WithError(err).Errorf("parse value error %#v", value)
			continue
		}
		//根据时间聚合
		_, exist := tableValueMap[tableName]
		if !exist {
			tableValueMap[tableName] = map[int64][]string{}
		}
		_, exist = tableValueMap[tableName][metric.Timestamp]
		if exist {
			//todo field不在fieldMap
			index, exist := fieldMap[metric.HashMetric()]
			if !exist {
				log.Logger.Panicf("'%s' metric '%s' not define", sTableName, metric.Metric)
			}
			tableValueMap[tableName][metric.Timestamp][index] = float2String(value)
		} else {
			data := make([]string, fieldNum)
			data[0] = second2String(metric.Timestamp)
			data[fieldMap[metric.HashMetric()]] = float2String(value)
			tableValueMap[tableName][metric.Timestamp] = data
		}
	}

	for tableName, tsFieldMap := range tableValueMap {
		hashTable := util.ToHashString(tableName)
		var value []string
		var tsList []int64
		for ts := range tsFieldMap {
			tsList = append(tsList, ts)
		}
		sort.Slice(tsList, func(i, j int) bool {
			return tsList[i] < tsList[j]
		})
		for _, ts := range tsList {
			fieldValue := tsFieldMap[ts]
			// 以前添加上的值先查出来再把现在的空值填上
			data, err := executor.QueryOneFromTable(ctx,hashTable, time.Unix(ts, 0))
			if err == nil {
				if len(data.Data) == 1 && len(data.Data[0]) == len(fieldValue) {
					for index, v := range data.Data[0] {
						if v != nil && fieldValue[index] == "" {
							fieldValue[index] = float2String(v.(float64))
						}
					}
				}
			}
			value = append(value, slice2String(fieldValue))
		}
		err := executor.InsertUsingSTable(
			ctx,
			hashTable,
			sTableName,
			fmt.Sprintf("'%s'", strings.Join(tableTagMap[tableName], "','")),
			value,
		)
		if err != nil {
			log.Logger.WithError(err).Errorf("insert data error")
		}
	}
}

func InsertPatternGroupMetric(metricGroupMap map[string][]*model.MetricValue) error {
	wg := sync.WaitGroup{}
	for sTableName, ms := range metricGroupMap {
		//获取超级表定义
		tableInfo, err := executor.DescribeTable(ctx,sTableName)
		if err != nil {
			log.Logger.WithError(err).Errorf("describe STable '%s' error", sTableName)
			return err
		}
		//构建列模型
		fieldMap := map[string]int{}
		fieldNum := len(tableInfo.Fields)
		for i, field := range tableInfo.Fields {
			fieldMap[field.Name] = i
		}
		metrics := ms
		wg.Add(1)
		tn := sTableName
		poolErr := utilspool.GoroutinePool.Submit(func() {
			process(tn, metrics, fieldNum, fieldMap)
			wg.Done()
		})
		if poolErr != nil {
			wg.Done()
		}
	}
	wg.Wait()
	return nil
}

func ModifyTable(sTableName string, metrics []*model.MetricValue) error {
	//获取超级表信息
	tableInfo, err := executor.DescribeTable(ctx, sTableName)
	if err != nil {
		var tdError *common.TDengineError
		if errors.As(err, &tdError) {
			//TDengine 返回的错误
			switch tdError.Code {
			case 866:
				//Table does not exist
				//表不存在
				log.Logger.Warnf("STable '%s' does not exit", sTableName)
				//创建表
				tableInfo = &tdengineExecutor.TableInfo{
					Fields: []*tdengineExecutor.FieldInfo{
						//以第一个元素建表
						{
							Name:   metrics[0].HashMetric(),
							Type:   common.DOUBLEType,
							Length: 0,
						},
					},
					Tags: tagScheme,
				}
				err := executor.CreateSTable(ctx,sTableName, tableInfo)
				if err != nil {
					log.Logger.WithError(err).Errorf("create STable '%s' error", sTableName)
					return err
				}
			default:
				log.Logger.WithError(err).Errorf("describe STable '%s' error", sTableName)
				return err
			}
		} else {
			log.Logger.WithError(err).Errorf("describe STable '%s' error", sTableName)
			return err
		}

	}
	tableFieldMap := make(map[string]bool, len(tableInfo.Fields))
	for _, field := range tableInfo.Fields {
		tableFieldMap[field.Name] = true
	}
	fields := map[string]bool{}
	for _, metric := range metrics {
		fields[metric.Metric] = true
		//检查列是否存在
		if !tableFieldMap[metric.HashMetric()] {
			//列不存在创建列
			log.Logger.Infof("add column %s on STable %s", metric.Metric, sTableName)
			err := executor.AddColumn(ctx,common.STableType, sTableName, &tdengineExecutor.FieldInfo{
				Name: metric.HashMetric(),
				Type: common.DOUBLEType,
			})
			if err != nil {
				log.Logger.WithError(err).Errorf("%s add column %s error", sTableName, metric.Metric)
				return err
			}
			tableFieldMap[metric.HashMetric()] = true
		}
	}
	return nil
}

// QueryMetric 通过超级表查询数据
func QueryMetric(
	endpoints []string,
	singleCounterList []string,
	groupCounterMap map[string][]string,
	start, end time.Time,
	interval int, aggregation, fill string,
) (*common.QueryResponse, map[string]string, error) {
	req := common.NewQueryRequest()
	req.WithStart(start)
	req.WithEnd(end)
	req.WithInterval(fmt.Sprintf("%ds", interval))
	req.WithAggregation(aggregation)
	req.WithFill(fill)
	metricHashMap := map[string]string{}
	var singleMetricTagList []map[string]interface{}
	for _, counter := range singleCounterList {
		metric, tag := parseCounter(counter)
		singleTagMap := map[string]interface{}{MetricTagKey: metric}
		if tag != "" {
			singleTagMap[TagMetricTagKey] = tag
		}
		singleMetricTagList = append(singleMetricTagList, singleTagMap)
	}
	//聚合tag和metric
	for group, counters := range groupCounterMap {
		tagValueMaps := map[string]map[string]struct{}{}
		tagValueList := map[string][]string{}
		var tagList []string
		for _, counter := range counters {
			metric, tag := parseCounter(counter)
			hashedMetric := util.ToHashString(metric)
			metricHashMap[hashedMetric] = metric
			if tagValueMaps[tag] == nil {
				tagList = append(tagList, tag)
				tagValueMaps[tag] = map[string]struct{}{hashedMetric: {}}
				tagValueList[tag] = []string{hashedMetric}
			} else {
				if _, ok := tagValueMaps[tag][hashedMetric]; !ok {
					tagValueMaps[tag][hashedMetric] = struct{}{}
					tagValueList[tag] = append(tagValueList[tag], hashedMetric)
				}
			}
		}
		tagGroup := map[int][]int{}
		matchedMap := map[int]struct{}{}
		for i := 0; i < len(tagList); i++ {
			if _, exist := matchedMap[i]; exist {
				continue
			}
			tagGroup[i] = []int{i}
			if i == len(tagList)-1 {
				break
			}
			for j := i + 1; j < len(tagList); j++ {
				if _, exist := matchedMap[j]; exist {
					continue
				}
				source := tagList[i]
				compare := tagList[j]
				if len(tagValueList[source]) == len(tagValueList[compare]) {
					equal := true
					for k := 0; k < len(tagValueList[source]); k++ {
						tag := tagValueList[source][k]
						_, exist := tagValueMaps[compare][tag]
						if !exist {
							equal = false
							break
						}
					}
					if equal {
						tagGroup[i] = append(tagGroup[i], j)
						matchedMap[j] = struct{}{}
					}
				}
			}
		}
		for _, endpoint := range endpoints {
			tag := map[string]interface{}{
				EndpointTagKey: util.EscapeString(endpoint),
			}
			for i, g := range tagGroup {
				t := make([]map[string]interface{}, 0, len(g))
				if tagList == nil {
					panic("impossible")
				}
				for _, gi := range g {
					if tagList[gi] != "" {
						tag[TagMetricTagKey] = tagList[gi]
					}
					t = append(t, tag)
				}
				req.AddTable(&common.Table{
					TableName:  group,
					Tags:       t,
					ColumnList: tagValueList[tagList[i]],
				})
			}
		}
	}
	singleMetricTotalTagList := make([]map[string]interface{}, 0, len(endpoints)*len(singleMetricTagList))
	for _, endpoint := range endpoints {
		for _, s := range singleMetricTagList {
			t := make(map[string]interface{}, len(s)+1)
			for k, v := range s {
				t[k] = v
			}
			t[EndpointTagKey] = util.EscapeString(endpoint)
			singleMetricTotalTagList = append(singleMetricTotalTagList, t)
		}
	}
	if len(singleCounterList) != 0 {
		req.AddTable(&common.Table{
			TableName:  "openfalcon_single_stable",
			Tags:       singleMetricTotalTagList,
			ColumnList: []string{"value"},
		})
	}
	data, err := executor.Query(ctx,req)
	if err != nil {
		return nil, nil, err
	}
	return data, metricHashMap, err
}

func OrganizeResult2HistoryResponse(data *common.QueryResponse, metricHashMap map[string]string, interval int, dsType string) *model.HistoryResponse {
	if dsType == "" {
		dsType = "GAUGE"
	}
	resp := model.HistoryResponse{}
	for _, result := range data.Results {
		values := make([]*cmodel.RRDData, len(result.Values))
		for i, value := range result.Values {
			var v cmodel.JsonFloat
			if value.Value != nil {
				v = cmodel.JsonFloat(value.Value.(float64))
			} else {
				v = cmodel.JsonFloat(math.NaN())
			}
			r := pool.RRDPoolGet()
			r.Timestamp = value.Time.Unix()
			r.Value = v
			values[i] = r
		}
		column := result.Column
		if result.Table != "openfalcon_single_stable" {
			column = metricHashMap[column]
		}
		counter := buildCounter(column, result.Tags)
		resp = append(resp, &cmodel.GraphQueryResponse{
			Endpoint: result.Tags[EndpointTagKey].(string),
			Counter:  counter,
			DsType:   dsType,
			Step:     interval,
			Values:   values,
		})
	}
	return &resp
}

//QueryMetricDirectly 通过表名查询数据 一次只查一张表
func QueryMetricDirectly(
	tableName string,
	column string,
	start, end time.Time,
	interval int, aggregation, fill string,
) ([]*pool.FloatValueResponse, error) {
	req := common.NewQueryRequest()
	req.WithStart(start)
	req.WithEnd(end)
	req.WithInterval(fmt.Sprintf("%ds", interval))
	req.WithAggregation(aggregation)
	req.WithFill(fill)
	req.AddTable(&common.Table{
		TableName:  tableName,
		ColumnList: []string{column},
	})
	s1 := time.Now()
	data, err := executor.Query(ctx,req)
	if err != nil {
		return nil, err
	}
	fmt.Println("s1", time.Now().Sub(s1).Microseconds())
	s2 := time.Now()
	var result []*pool.FloatValueResponse
	if len(data.Results) == 1 {
		result = make([]*pool.FloatValueResponse, 0, len(data.Results[0].Values))
		for _, v := range data.Results[0].Values {
			r := pool.FloatValueResponsePoolGet()
			r.Timestamp = v.Time.Unix()
			if v.Value == nil {
				r.Value = nil
			} else {
				vv := v.Value.(float64)
				r.Value = &vv
			}
			result = append(result, r)
		}
	}
	fmt.Println("s2", time.Now().Sub(s2).Microseconds())
	return result, err
}

func parseCounter(counter string) (metric string, tag string) {
	s := strings.SplitN(counter, "/", 2)
	if len(s) == 2 {
		return util.EscapeString(s[0]), util.EscapeString(s[1])
	}
	return util.EscapeString(counter), ""
}

func buildCounter(metric string, tags map[string]interface{}) string {
	b := bytes.Buffer{}
	b.WriteString(metric)
	tag := ""
	t, exist := tags[TagMetricTagKey]
	if exist {
		b.WriteString("/")
		tag = t.(string)
		b.WriteString(tag)
	}
	return b.String()
}

func initSingleSTable() {
	//建立单列超级表
	err := executor.CreateSTable(ctx,"openfalcon_single_stable", &tdengineExecutor.TableInfo{
		Fields: []*tdengineExecutor.FieldInfo{
			{
				Name:   "value",
				Type:   common.DOUBLEType,
				Length: 0,
			},
		},
		Tags: tagScheme,
	})
	if err != nil {
		log.Logger.WithError(err).Panicf("init stable %s error", "openfalcon_single_stable")
	}
}

func initGroupStable(groups map[string]*metricgroup.Config) {
	//建立非正则组超级表
	for sTableName, groupDefine := range groups {
		if !groupDefine.EnablePattern {
			fields := make([]*tdengineExecutor.FieldInfo, 0, len(groupDefine.Metrics))
			for _, field := range groupDefine.Metrics {
				fields = append(fields, &tdengineExecutor.FieldInfo{
					Name:   util.ToHashString(field),
					Type:   common.DOUBLEType,
					Length: 0,
				})
			}
			err := executor.CreateSTable(ctx,sTableName, &tdengineExecutor.TableInfo{
				Fields: fields,
				Tags:   tagScheme,
			})
			if err != nil {
				log.Logger.WithError(err).Panicf("init stable %s error", sTableName)
			}
		}
	}
}

func createDB() {
	err := executor.CreateDatabase(ctx,config.Conf.DataKeep,1)
	if err != nil {
		panic(err)
	}
	err = executor.AlterDatabase(ctx,"CACHELAST", config.Conf.CacheLast)
	if err != nil {
		panic(err)
	}
}

func setPrecision() {
	precision, err := executor.GetPrecision(ctx)
	if err != nil {
		panic(err)
	}
	layout := "2006-01-02 15:04:05.000"
	switch precision {
	case "us":
		layout = "2006-01-02 15:04:05.000000"
	case "ns":
		layout = "2006-01-02 15:04:05.000000000"
	}
	executor.SetTimeLayout(layout)
}

func init() {
	tdengineConnector, err := connector.NewTDengineConnector(config.Conf.TDengineConnType, config.TDengineConfig)
	if err != nil {
		log.Logger.WithError(err).Panic("create TDengine connector error")
	}
	executor = tdengineExecutor.NewExecutor(tdengineConnector, config.Conf.DBName, config.Conf.ShowSQL, log.Logger)
	createDB()
	setPrecision()
	initSingleSTable()
	initGroupStable(metricgroup.GetMetricGroups())
}
