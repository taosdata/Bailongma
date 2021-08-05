package model

import "time"

type MetricPoint struct {
	TableName   string
	Ident       string            `json:"ident"`  // 资源标识，跟资源无关的监控数据，该字段为空
	Metric      string            `json:"metric"` // 监控指标名称
	TagsMap     map[string]string `json:"tags"`   // 监控数据标签
	TagsList    []string          `json:"-"`      //["tag1","tag2"] 排序过
	Time        time.Time         `json:"time"`
	Value       float64           `json:"-"` // 内部字段，最终转换之后的float64数值
}

const (
	LABEL_IDENT  = "ident"
	LABEL_NAME   = "__name__"
	DEFAULT_QL   = `{__name__=~".*a.*|.*e.*"}`
	DEFAULT_STEP = 15
)
