/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package read

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/taosdata/Bailongma/blm_prometheus/pkg/log"
	"github.com/taosdata/Bailongma/blm_prometheus/pkg/tdengine/write"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type ReaderProcessor struct {
}

func NewProcessor() *ReaderProcessor {
	processor := &ReaderProcessor{}
	return processor
}

func (p *ReaderProcessor) Process(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	db, err := sql.Open(write.DriverName, write.DbUser+":"+write.DbPassword+"@/tcp("+write.DaemonIP+")/"+write.DbName)
	if err != nil {
		log.ErrorLogger.Printf("Open database error: %s\n", err)
	}
	defer db.Close()
	labelsToSeries := map[string]*prompb.TimeSeries{}
	for _, q := range req.Queries {
		metricFilter, condition, labelFilters, err := buildCommand(q)
		if err != nil {
			return nil, err
		}

		metrics, err := fetchMetricTables(db, metricFilter)
		if err != nil {
			return nil, err
		}

		// metricLabelAdd := len(metrics) > 1

		for _, tableName := range metrics {
			command := fmt.Sprintf("SELECT * FROM %s %s", tableName, condition)
			//tableName, command, err := buildCommand(q)

			log.InfoLogger.Printf("Executed query：%s\n", command)

			rows, err := db.Query(command)
			if err != nil {
				return nil, err
			}

			columns, err := rows.Columns()
			if err != nil {
				return nil, err
			}

			columnLength := len(columns)
			// temp for cache
			cache := make([]interface{}, columnLength)
			// init for row
			for index := range cache {
				var a interface{}
				cache[index] = &a
			}

			for rows.Next() {
				var (
					value    float64
					dataTime time.Time
				)

				error := rows.Scan(cache...)
				if error != nil {
					return nil, error
				}

				row := make(map[string]string)
				//set data
				rowOk := true

				for i, data := range cache {
					if columns[i] == "ts" {
						dataTime = (*data.(*interface{})).(time.Time)
						// timeStr := (*data.(*interface{})).(string)
						// dataTime, err = time.Parse("2006-01-02 15:04:05.000", timeStr)
						// if err != nil {
						// 	continue
						// }
					} else if columns[i] == "value" {
						value = (*data.(*interface{})).(float64)
					} else if columns[i] != "taghash" {
						labelName := strings.Replace(columns[i], "t_", "", 1)
						labelFilter, ok := labelFilters[labelName]
						value := (*data.(*interface{})).(string)
						if ok {
							for _, filter := range labelFilter {
								isMatch := filter.Pattern.MatchString(value)
								if filter.Type == prompb.LabelMatcher_RE && !isMatch {
									rowOk = false
									break
								}
								if filter.Type == prompb.LabelMatcher_NRE && isMatch {
									rowOk = false
									break
								}
							}
						}
						if !rowOk {
							break
						}

						row[columns[i]] = value
					}
				}
				if !rowOk {
					continue

				}
				keys := make([]string, len(row))
				i := 0
				for k, _ := range row {
					keys[i] = k
					i++
				}
				sort.Strings(keys)
				buffer := bytes.NewBufferString("")
				buffer.WriteString(tableName)
				buffer.WriteString("|")
				for _, k := range keys {
					buffer.WriteString(k)
					buffer.WriteString(":")
					buffer.WriteString(row[k])
					buffer.WriteString("|")
				}
				labelsKey := buffer.String()
				ts, ok := labelsToSeries[labelsKey]
				if !ok {
					labelPairs := make([]*prompb.Label, 0, columnLength-2)
					labelPairs = append(labelPairs, &prompb.Label{
						Name:  model.MetricNameLabel,
						Value: tableName,
					})

					for _, k := range columns {
						if k == "ts" || k == "value" || k == "taghash" {
							continue
						}
						labelPairs = append(labelPairs, &prompb.Label{
							Name:  strings.Replace(k, "t_", "", 1),
							Value: row[k],
						})
					}

					ts = &prompb.TimeSeries{
						Labels:  labelPairs,
						Samples: make([]prompb.Sample, 0, 100),
					}
					labelsToSeries[labelsKey] = ts
				}

				ts.Samples = append(ts.Samples, prompb.Sample{
					Timestamp: dataTime.UnixNano() / 1000000,
					Value:     value,
				})
			}

			err = rows.Err()
			if err != nil {
				return nil, err
			}

			rows.Close()
		}
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries)),
			},
		},
	}
	for _, ts := range labelsToSeries {
		// log.InfoLogger.Printf("ts size: %d\n", ts.Size())
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	log.InfoLogger.Printf("Returned response #timeseries: %d\n", len(labelsToSeries))
	return &resp, nil
}

func fetchMetricTables(db *sql.DB, filter *MetricFilter) (metrics []string, err error) {
	if filter.Type == prompb.LabelMatcher_EQ {
		metrics = append(metrics, filter.Pattern)
		return metrics, err
	}
	rows, err := db.Query("show stables")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var ts time.Time
		var columns int
		var tags int
		var tables int
		err = rows.Scan(&name, &ts, &columns, &tags, &tables)
		if err != nil {
			return nil, err
		}
		switch filter.Type {
		case prompb.LabelMatcher_NEQ:
			if name != filter.Pattern {
				metrics = append(metrics, name)
			}
		case prompb.LabelMatcher_RE:
			pattern, err := regexp.Compile(filter.Pattern)
			if err != nil {
				return nil, err
			}
			if pattern.MatchString(name) {
				metrics = append(metrics, name)
			}
		case prompb.LabelMatcher_NRE:
			pattern, err := regexp.Compile(filter.Pattern)
			if err != nil {
				return nil, err
			}
			if !pattern.MatchString(name) {
				metrics = append(metrics, name)
			}
		default:
			return nil, fmt.Errorf("unreachable filter: %v", filter)
		}
	}
	return metrics, err
}

func buildCommand(q *prompb.Query) (*MetricFilter, string, map[string][]*LabelFilter, error) {
	return buildQuery(q)
}

type LabelFilter struct {
	Type    prompb.LabelMatcher_Type
	Pattern *regexp.Regexp
}

type MetricFilter struct {
	Type    prompb.LabelMatcher_Type
	Pattern string
}

func buildQuery(q *prompb.Query) (*MetricFilter, string, map[string][]*LabelFilter, error) {
	matchers := make([]string, 0, len(q.Matchers))
	var metricFilter MetricFilter
	filters := make(map[string][]*LabelFilter)
	hasName := false
	for _, m := range q.Matchers {
		escapedName := escapeValue(m.Name)
		escapedValue := escapeValue(m.Value)
		if m.Name == model.MetricNameLabel {
			if len(escapedValue) == 0 {
				return nil, "", nil, fmt.Errorf("unknown metric name match type %v", m.Type)
			}
			metricFilter.Type = m.Type
			metricFilter.Pattern = escapedValue
			hasName = true
		} else {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					// From the PromQL docs: "Label matchers that match
					// empty label values also select all time series that
					// do not have the specific label set at all."
					matchers = append(matchers, fmt.Sprintf("(t_%s = '' or t_%s is null)", escapedName, escapedName))
				} else {
					matchers = append(matchers, fmt.Sprintf("t_%s = '%s'", escapedName, escapedValue))
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("t_%s <> '%s'", escapedName, escapedValue))
			default:
				pattern, err := regexp.Compile(m.Value)
				log.InfoLogger.Println("pattern: ", m.Value)
				if err != nil {
					return nil, "", nil, fmt.Errorf("regex pattern is invalid: %v", escapedValue)
				}
				var filter = &LabelFilter{
					Type:    m.Type,
					Pattern: pattern,
				}
				if labelFilter, exists := filters[escapedName]; exists {
					filters[escapedName] = append(labelFilter, filter)
				} else {
					filters[escapedName] = make([]*LabelFilter, 1)
					filters[escapedName][0] = filter
				}
			}
		}
	}

	if !hasName {
		return nil, "", nil, fmt.Errorf("tableName not setted")
	}

	log.InfoLogger.Printf("startTime：%d ,endTime:%d\n", q.StartTimestampMs, q.EndTimestampMs)
	matchers = append(matchers, fmt.Sprintf("ts >= %d", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("ts <= %d", q.EndTimestampMs))

	return &metricFilter, fmt.Sprintf("WHERE %s ORDER BY ts",
		strings.Join(matchers, " AND ")), filters, nil
}

func escapeValue(str string) string {
	return strings.Replace(str, `'`, `''`, -1)
}
