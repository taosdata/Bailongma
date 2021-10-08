package main

import (
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	//remoteWrite()
	remoteRead()
	//wg := sync.WaitGroup{}
	//wg.Add(100)
	//for i := 0; i < 1000; i++ {
	//	go func() {
	//
	//		wg.Done()
	//	}()
	//}
	//wg.Wait()
}

func remoteWrite() {
	now := time.Now()
	data := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "ident",
						Value: "127.0.0.1",
					}, {
						Name:  "__name__",
						Value: "cpu.test1",
					}, {
						Name:  "tagn",
						Value: "n",
					}, {
						Name:  "tag1",
						Value: "1",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     rand.Float64(),
						Timestamp: now.UnixNano() / 1e6,
					},
				},
			},
			{
				Labels: []prompb.Label{
					{
						Name:  "ident",
						Value: "127.0.0.1",
					}, {
						Name:  "__name__",
						Value: "cpu.test2",
					}, {
						Name:  "tag1",
						Value: "1",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     rand.Float64(),
						Timestamp: now.UnixNano() / 1e6,
					},
				},
			},
			{
				Labels: []prompb.Label{
					{
						Name:  "ident",
						Value: "127.0.0.1",
					}, {
						Name:  "__name__",
						Value: "nn",
					}, {
						Name:  "tag1",
						Value: "中文测试",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     rand.Float64(),
						Timestamp: now.UnixNano() / 1e6,
					},
				},
			},
		},
	}
	b, err := proto.Marshal(&data)
	if err != nil {
		panic(err)
	}
	buf := snappy.Encode(nil, b)
	r := bytes.NewReader(buf)
	resp, err := http.Post("http://127.0.0.1:9090/api/v1/write", "application/x-protobuf", r)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode, string(d))
}
func remoteRead() {
	data := prompb.ReadRequest{Queries: []*prompb.Query{
		{
			StartTimestampMs: 1633661132000,
			EndTimestampMs:   1633661133000,
			Matchers: []*prompb.LabelMatcher{
				//{
				//	Type:  prompb.LabelMatcher_RE,
				//	Name:  "__name__",
				//	Value: "cpu.*",
				//},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "tag1",
					Value: "中文测试",
				},
				//{
				//	Type:  prompb.LabelMatcher_NRE,
				//	Name:  "__name__",
				//	Value: ".*a.*|.*e.*",
				//},
			},
		},
	}}
	b, err := proto.Marshal(&data)
	if err != nil {
		panic(err)
	}
	buf := snappy.Encode(nil, b)
	r := bytes.NewReader(buf)
	resp, err := http.Post("http://127.0.0.1:9090/api/v1/read", "application/x-protobuf", r)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode == http.StatusAccepted {
		rd, err := snappy.Decode(nil, d)
		if err != nil {
			panic(err)
		}
		var respData prompb.ReadResponse
		err = proto.Unmarshal(rd, &respData)
		if err != nil {
			panic(err)
		}
		fmt.Println(respData)
	} else {
		fmt.Println(resp.StatusCode, string(d))
	}

}
