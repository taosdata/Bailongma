package remoteread

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/taosdata/Bailongma/blm_nightingale/tdengine"
	"github.com/taosdata/go-utils/json"
	"github.com/taosdata/go-utils/pool"
	"sort"
	"strings"
	"time"
)

type Metric struct {
	TS     time.Time
	Value  float64
	Labels map[string]string
}

func process(req *prompb.ReadRequest) (resp *prompb.ReadResponse, err error) {
	resp = &prompb.ReadResponse{}
	for i, query := range req.Queries {
		data, err := tdengine.Query(query)
		if err != nil {
			return nil, err
		}
		//ts value labels time.Time float64 []byte
		group := map[string]*prompb.TimeSeries{}
		for _, d := range data.Data {
			if d[0] == nil || d[1] == nil || d[2] == nil {
				continue
			}
			ts := d[0].(time.Time)
			value := d[1].(float64)
			var tags map[string]string
			err = json.Unmarshal(d[2].([]byte), &tags)
			if err != nil {
				return nil, err
			}
			tmp := make([]string, 0, len(tags))
			for name, value := range tags {
				b := pool.BytesPoolGet()
				b.WriteString(name)
				b.WriteByte(':')
				b.WriteString(value)
				tmp = append(tmp, b.String())
				pool.BytesPoolPut(b)
			}
			sort.Strings(tmp)
			flag := strings.Join(tmp, ",")
			timeSeries, exist := group[flag]
			if exist {
				timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
					Value:     value,
					Timestamp: ts.UnixNano() / 1e6,
				})
			} else {
				timeSeries = &prompb.TimeSeries{
					Samples: []prompb.Sample{
						{
							Value:     value,
							Timestamp: ts.UnixNano() / 1e6,
						},
					},
				}
				timeSeries.Labels = make([]prompb.Label, 0, len(tags))
				for name, tagValue := range tags {
					timeSeries.Labels = append(timeSeries.Labels, prompb.Label{
						Name:  name,
						Value: tagValue,
					})
				}
				group[flag] = timeSeries
			}
		}
		if len(group) > 0 {
			resp.Results = append(resp.Results, &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(group))})
		}
		for _, series := range group {
			resp.Results[i].Timeseries = append(resp.Results[i].Timeseries, series)
		}
	}
	return resp, err
}
