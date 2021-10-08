package remotewrite

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/Bailongma/blm_nightingale/model"
	"github.com/taosdata/Bailongma/blm_nightingale/tdengine"
	"time"
)

func processReq(req *prompb.WriteRequest) error {
	//由夜莺来的数据每个里面只有一个时序数据
	//拼接组装一次
	var dataGroup []*model.MetricPoint
	for _, timeSeriesData := range req.Timeseries {
		p := &model.MetricPoint{TagsMap: map[string]string{}}
		for _, label := range timeSeriesData.Labels {
			p.TagsMap[label.Name] = label.Value
		}
		for _, sample := range timeSeriesData.Samples {
			p.Time = time.Unix(0, sample.Timestamp*1e6)
			p.Value = sample.Value
		}
		dataGroup = append(dataGroup, p)
	}
	tdengine.InsertData(dataGroup)
	return nil
}
