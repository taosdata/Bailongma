package remoteread

import (
	"errors"
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/Bailongma/blm_nightingale/config"
	"github.com/taosdata/Bailongma/blm_nightingale/model"
	"github.com/taosdata/Bailongma/blm_nightingale/tdengine"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/connector"
	"github.com/taosdata/go-utils/util"
	"regexp"
	"time"
)

func process(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var resp = &prompb.ReadResponse{}
	for _, query := range req.Queries {
		data, err := convertQuery(query)
		if err != nil {
			return nil, err
		}
		result := Query(data)
		resp.Results = append(resp.Results, result)
	}
	return resp, nil
}

type reqCondition struct {
	stable          map[string]int // metric: 分区ID
	start           time.Time
	end             time.Time
	commonCondition map[string]*commonCondition
	reCondition     map[string]*reCondition
	checkTag        bool
}
type reCondition struct {
	pattern *regexp.Regexp
	match   bool
}

type commonCondition struct {
	value string
	eq    bool
}

func conditionToSQL(source, value string, eq bool) string {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString(source)
	if eq {
		b.WriteString("=")
	} else {
		b.WriteString("!=")
	}
	b.WriteByte('\'')
	b.WriteString(value)
	b.WriteByte('\'')
	return b.String()
}

func convertQuery(query *prompb.Query) (*reqCondition, error) {
	//先筛出来超级表
	//组装tag查询条件
	//正则过滤
	//整理结果集
	var condition = &reqCondition{
		start:           time.Unix(0, query.StartTimestampMs*1e6),
		end:             time.Unix(0, query.EndTimestampMs*1e6),
		commonCondition: map[string]*commonCondition{},
		reCondition:     map[string]*reCondition{},
	}
	var metricFilter *prompb.LabelMatcher
	for _, matcher := range query.Matchers {
		switch matcher.Name {
		case model.LABEL_NAME:
			//查询metric
			metricFilter = matcher
		default:
			escapeName := tdengine.Escape(matcher.Name, tdengine.MaxColLen)
			escapeValue := util.EscapeString(matcher.Value)
			switch matcher.Type {
			case prompb.LabelMatcher_EQ:
				//相等
				condition.commonCondition[escapeName] = &commonCondition{
					value: escapeValue,
					eq:    true,
				}
			case prompb.LabelMatcher_NEQ:
				//不相等
				condition.commonCondition[escapeName] = &commonCondition{
					value: escapeValue,
					eq:    false,
				}
			case prompb.LabelMatcher_RE, prompb.LabelMatcher_NRE:
				//正则
				pattern, err := regexp.Compile(matcher.Value)
				if err != nil {
					return nil, err
				}
				condition.reCondition[escapeName] = &reCondition{
					pattern: pattern,
					match:   matcher.Type == prompb.LabelMatcher_RE,
				}
			}
		}
	}
	err := condition.handleMetricQuery(metricFilter)
	if err != nil {
		return nil, err
	}
	return condition, nil
}

//查询符合的 metric
//metric 限制 191字符以内 只能包含 数字 字符 下划线
func (c *reqCondition) handleMetricQuery(matcher *prompb.LabelMatcher) error {
	if matcher != nil {
		escapedMetric := tdengine.Escape(matcher.Value, tdengine.MaxTableLen)
		if matcher.Type == prompb.LabelMatcher_EQ {
			//相等
			if matcher.Value == "" {
				return errors.New("metric value is empty")
			}
			c.stable = map[string]int{escapedMetric: config.GetDBIndex(matcher.Value)}
		} else {
			//获取全部的stable需要下放到case里面，先检查正则合法再获取全部
			switch matcher.Type {
			case prompb.LabelMatcher_NEQ:
				allMetric := tdengine.GetAllSTables()
				//不相等
				delete(allMetric, escapedMetric)
				c.stable = allMetric
			case prompb.LabelMatcher_RE:
				//正则
				pattern, err := regexp.Compile(matcher.Value)
				if err != nil {
					return err
				}
				allMetric := tdengine.GetAllSTables()
				resultMap := make(map[string]int, len(allMetric))
				for metric, index := range allMetric {
					if pattern.Match([]byte(metric)[1:]) {
						resultMap[metric] = index
					}
				}
				c.stable = resultMap
			case prompb.LabelMatcher_NRE:
				//正则非
				pattern, err := regexp.Compile(matcher.Value)
				if err != nil {
					return err
				}
				allMetric := tdengine.GetAllSTables()
				resultMap := make(map[string]int, len(allMetric))
				for metric, index := range allMetric {
					if !pattern.Match([]byte(metric)[1:]) {
						resultMap[metric] = index
					}
				}
				c.stable = resultMap
			}
		}
	} else {
		//todo
		c.stable = tdengine.GetAllSTables()
		//return errors.New("need metric")
	}
	return nil
}

func (c *reqCondition) needCheckTagName(tagName string) bool {
	//if !c.checkTag {
	//	return true
	//}
	_, exist := c.commonCondition[tagName]
	if exist {
		return true
	} else {
		_, exist := c.reCondition[tagName]
		if exist {
			return true
		}
	}
	return false
}

func (c *reqCondition) neededTagValue(tagName string, value interface{}) bool {
	if !c.checkTag {
		return true
	}
	if value == nil {
		return false
	}
	info, exist := c.commonCondition[tagName]
	if exist {
		if info.eq {
			return info.value == value.(string)
		} else {
			return info.value != value.(string)
		}
	}
	reInfo, exist := c.reCondition[tagName]
	if exist {
		if reInfo.match {
			return reInfo.pattern.MatchString(value.(string))
		} else {
			return !reInfo.pattern.MatchString(value.(string))
		}
	}
	//按照逻辑不会到这
	return false
}

func Query(condition *reqCondition) *prompb.QueryResult {
	result := &prompb.QueryResult{}
	//todo 可能产生超大map
	labelsToSeries := map[string]*prompb.TimeSeries{}
	var data map[string]*connector.Data
	if len(condition.reCondition) == 0 {
		//带上where进行查询
		whereConditions := make([]string, 0, len(condition.commonCondition))
		for name, v := range condition.commonCondition {
			whereConditions = append(whereConditions, conditionToSQL(name, v.value, v.eq))
		}
		data = tdengine.GetBySTables(condition.start, condition.end, condition.stable, whereConditions)
	} else {
		//不带where进行查询
		data = tdengine.GetBySTables(condition.start, condition.end, condition.stable, nil)
		condition.checkTag = true
	}
	for metric, info := range data {
		if info == nil {
			continue
		}
		//构造一个hashID metric|tag1|tag2:tv1|tv2 作为一条时间线索引
		b := pool.BytesPoolGet()
		b.WriteString(metric)
		var checkTagIDList []int
		haveTag := len(info.Head) > 2
		if haveTag {
			for i := 2; i < len(info.Head); i++ {
				b.WriteByte('|')
				b.WriteString(info.Head[i])
				//检查tag是否需要检查
				if condition.needCheckTagName(info.Head[i]) {
					checkTagIDList = append(checkTagIDList, i)
				}
			}
		}
		if haveTag {
			b.WriteByte(':')
		}
		tagsString := b.String()
		pool.BytesPoolPut(b)
		for _, rowData := range info.Data {
			//检查值是否正确
			dropItem := false
			for _, id := range checkTagIDList {
				if !condition.neededTagValue(info.Head[id], rowData[id]) {
					dropItem = true
					break
				}
			}
			if dropItem {
				continue
			}
			if rowData[1] == nil {
				continue
			}
			tsValue := rowData[1].(float64)
			ts := rowData[0].(time.Time)
			tagB := pool.BytesPoolGet()
			tagB.WriteString(tagsString)
			if haveTag {
				for tagIndex := 2; tagIndex < len(info.Head); tagIndex++ {
					if rowData[tagIndex] == nil {
						tagB.WriteString("null")
					} else {
						tagB.WriteString(rowData[tagIndex].(string))
					}
					if tagIndex != len(info.Head)-1 {
						tagB.WriteByte('|')
					}
				}
			}
			//构造成 metric|tag1|tag2:tv1|tv2
			hashID := tagB.String()
			pool.BytesPoolPut(tagB)
			//检查时序数据
			timeSeries, exist := labelsToSeries[hashID]
			if !exist {
				//索引不存在
				labels := make([]*prompb.Label, 0, len(info.Head)-1) //metric + tag数量
				labels = append(labels, &prompb.Label{
					Name:  model.LABEL_NAME,
					Value: tdengine.Unescape(metric),
				})
				if haveTag{
					for i := 2; i < len(info.Head); i++ {
						if rowData[i] != nil {
							labels = append(labels, &prompb.Label{
								//去掉头部的下划线
								Name:  tdengine.Unescape(info.Head[i]),
								Value: rowData[i].(string),
							})
						}
					}
				}
				timeSeries = &prompb.TimeSeries{
					Labels: labels,
				}
				labelsToSeries[hashID] = timeSeries
			}
			timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
				Value:     tsValue,
				Timestamp: ts.UnixNano() / 1e6,
			})
		}
	}
	result.Timeseries = make([]*prompb.TimeSeries, 0, len(labelsToSeries))
	for _, series := range labelsToSeries {
		result.Timeseries = append(result.Timeseries, series)
	}
	return result
}
