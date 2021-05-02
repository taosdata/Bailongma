package tdengine

import (
	"blm_prometheus/pkg/log"
	"database/sql"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"strings"
	"time"
)

type sampleLabels struct {
	JSON        []byte
	Map         map[string]string
	OrderedKeys []string
}

// Config for the database
type Config struct {
	Table      string
	DaemonIP   string
	DaemonName string
	DbName     string
	DbUser     string
	DbPassword string
}
type Client struct {
	DB  *sql.DB
	cfg *Config
}

func NewClient(cfg *Config) *Client {
	client := &Client{
		cfg: cfg,
	}
	return client
}

func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	db, err := sql.Open("taosSql", c.cfg.DbUser+":"+c.cfg.DbPassword+"@/tcp("+c.cfg.DaemonIP+")/"+c.cfg.DbName)
	if err != nil {
		log.Errorf("Open database error: %s\n", err)
	}
	defer db.Close()
	c.DB = db
	labelsToSeries := map[string]*prompb.TimeSeries{}
	for _, q := range req.Queries {
		tableName, command, err := c.buildCommand(q)

		if err != nil {
			return nil, err
		}
		log.Infof("Executed query：%s\n", command)

		rows, err := c.DB.Query(command)
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
		for index, _ := range cache {
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
			for i, data := range cache {
				if columns[i] == "ts" {
					timeStr := (*data.(*interface{})).(string)
					dataTime, err = time.Parse("2006-01-02 15:04:05.000", timeStr)
					if err != nil {
						continue
					}
				} else if columns[i] == "value" {
					value = (*data.(*interface{})).(float64)
				} else if columns[i] != "taghash" {
					row[columns[i]] = (*data.(*interface{})).(string)
				}
			}

			ts, ok := labelsToSeries[tableName]
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
						Name:  k,
						Value: row[k],
					})
				}

				ts = &prompb.TimeSeries{
					Labels:  labelPairs,
					Samples: make([]prompb.Sample, 0, 100),
				}
				labelsToSeries[tableName] = ts
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

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries)),
			},
		},
	}
	for _, ts := range labelsToSeries {
		log.Infof("ts size: %d\n", ts.Size())
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	log.Infof("Returned response #timeseries: %d\n", len(labelsToSeries))
	return &resp, nil
}

func (c *Client) buildCommand(q *prompb.Query) (string, string, error) {
	return c.buildQuery(q)
}

func (c *Client) buildQuery(q *prompb.Query) (string, string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	var tableName = ""
	for _, m := range q.Matchers {
		escapedName := escapeValue(m.Name)
		escapedValue := escapeValue(m.Value)
		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					return "", "", fmt.Errorf("unknown metric name match type %v", m.Type)
				} else {
					tableName = escapedValue
				}
			case prompb.LabelMatcher_NEQ:
			case prompb.LabelMatcher_RE:
			case prompb.LabelMatcher_NRE:
				return "", "", fmt.Errorf("no support metric name type %v", m.Type)
			default:
				return "", "", fmt.Errorf("unknown metric name match type %v", m.Type)
			}
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
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("t_%s like '%s'", escapedName, anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				return "", "", fmt.Errorf("no support match type %v", m.Type)
			default:
				return "", "", fmt.Errorf("unknown match type %v", m.Type)
			}
		}
	}

	if len(tableName) == 0 {
		return "", "", fmt.Errorf("unknown tableName")
	}

	log.Infof("startTime：%d ,endTime:%d\n", q.StartTimestampMs, q.EndTimestampMs)
	matchers = append(matchers, fmt.Sprintf("ts >= %v", q.StartTimestampMs))
	matchers = append(matchers, fmt.Sprintf("ts <= %v", q.EndTimestampMs))

	return tableName, fmt.Sprintf("SELECT * FROM %s WHERE %s ORDER BY ts",
		tableName, strings.Join(matchers, " AND ")), nil
}

func escapeValue(str string) string {
	return strings.Replace(str, `'`, `''`, -1)
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 || (str[0] == '^' && str[l-1] == '$') {
		str = strings.Replace(str, "$", "", 1)
		return strings.Replace(str, "^", "", 1)
	}

	if str[0] == '^' {
		str = strings.Replace(str, "^", "", 1)
		return fmt.Sprintf("%s%", str)
	}

	if str[l-1] == '$' {
		str = strings.Replace(str, "$", "", 1)
		return fmt.Sprintf("%s", "%"+str)
	}

	return fmt.Sprintf("%s%", "%"+str)
}

// Name identifies the client as a TDengine client.
func (c Client) Name() string {
	return "TDengine"
}

func toTimestamp(milliseconds int64) time.Time {
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC()
}

// HealthCheck implements the healtcheck interface
func (c *Client) HealthCheck() error {
	rows, err := c.DB.Query("SELECT 1")

	if err != nil {
		log.Errorf("Health check error %v\n", err)
		return err
	}

	rows.Close()
	return nil
}

func (l sampleLabels) String() string {
	return string(l.JSON)
}

func (l sampleLabels) key(extra string) string {
	// 0xff cannot cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	separator := "\xff"
	pairs := make([]string, 0, len(l.Map)+1)
	pairs = append(pairs, extra+separator)

	for _, k := range l.OrderedKeys {
		pairs = append(pairs, k+separator+l.Map[k])
	}
	return strings.Join(pairs, separator)
}

func (l *sampleLabels) len() int {
	return len(l.OrderedKeys)
}
