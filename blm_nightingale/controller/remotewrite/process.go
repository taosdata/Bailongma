package remotewrite

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/Bailongma/blm_nightingale/config"
	"github.com/taosdata/Bailongma/blm_nightingale/model"
	"github.com/taosdata/Bailongma/blm_nightingale/tdengine"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/util"
	"sort"
	"time"
)

// 超级表名 _metric 超长截断
// 普通表名 "md5_"+md5("ep_metric_t1=v1,t2=v2")
// tag 第一列 ep 之后是排序过的 tagPairs
func enrich(point *model.MetricPoint) {
	// 根据tagsmap生成tagslst，sort
	count := len(point.TagsMap)
	point.TagsList = make([]string, 0, count)
	ret := pool.BytesPoolGet()
	ret.WriteString(point.Ident)
	ret.WriteString(point.Metric)
	if count != 0 {
		for tag := range point.TagsMap {
			point.TagsList = append(point.TagsList, tag)
		}
		sort.Strings(point.TagsList)
		ret.WriteByte('_')
		for i, s := range point.TagsList {
			ret.WriteString(s)
			ret.WriteByte('=')
			ret.WriteString(point.TagsMap[s])
			if i != len(point.TagsList)-1 {
				ret.WriteByte(',')
			}
		}
	}
	// ident metric tagslst 生成 tableName
	// "md5_"+md5("ep_metric_t1=v1,t2=v2")
	point.TableName = util.ToHashString(ret.String())
	pool.BytesPoolPut(ret)
}

func processReq(req *prompb.WriteRequest) error {
	//由夜莺来的数据每个里面只有一个时序数据
	//拼接组装一次
	dataGroup := map[string][]*model.MetricPoint{}
	for _, timeSeriesData := range req.Timeseries {
		p := &model.MetricPoint{TagsMap: map[string]string{}}
		for _, label := range timeSeriesData.Labels {
			switch label.Name {
			case model.LABEL_NAME:
				p.Metric = label.Value
			case model.LABEL_IDENT:
				p.Ident = label.Value
			default:
				p.TagsMap[label.Name] = label.Value
			}
		}
		for _, sample := range timeSeriesData.Samples {
			p.Time = time.Unix(0, sample.Timestamp*1e6)
			p.Value = sample.Value
		}
		enrich(p)
		dataGroup[p.Metric] = append(dataGroup[p.Metric], p)
	}
	for metric, metricPoint := range dataGroup {
		dbIndex := config.GetDBIndex(metric)
		tdengine.ExecutorGroup[dbIndex].InsertData(metricPoint)
	}
	return nil
}
