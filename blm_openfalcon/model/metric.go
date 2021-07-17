package model

import (
	"bailongma/v2/infrastructure/util"
	"errors"
	"github.com/open-falcon/falcon-plus/common/model"
	"strconv"
)

type MetricValue struct {
	model.MetricValue
	MetricHash string `json:"-"`
}

func (m *MetricValue) ParseValue() (float64, error) {
	switch cv := m.Value.(type) {
	case string:
		return strconv.ParseFloat(cv, 64)
	case float64:
		return cv, nil
	case int64:
		return float64(cv), nil
	default:
		return 0, errors.New("parse value error")
	}
}

func (m *MetricValue) HashMetric() string {
	if m.MetricHash != "" {
		return m.MetricHash
	}
	m.MetricHash = util.ToHashString(m.Metric)
	return m.MetricHash
}
