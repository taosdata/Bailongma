package model

import "time"

type MetricPoint struct {
	TagsMap map[string]string `json:"tags_map"`
	Time    time.Time         `json:"time"`
	Value   float64           `json:"value"`
}
