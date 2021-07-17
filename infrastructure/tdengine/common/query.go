package common

import (
	"time"
)

type EmptyResponse struct{}

type Device struct {
	DeviceID    string
	PointIDList []string
}

type Table struct {
	TableName  string
	Tags       []map[string]interface{}
	ColumnList []string
}

type DataItem struct {
	Value interface{} `json:"value"`
	Time  time.Time   `json:"time"`
}

type QueryResult struct {
	Table  string                 `json:"deviceID"`
	Tags   map[string]interface{} `json:"tags"`
	Column string                 `json:"column"`
	Values []*DataItem            `json:"values"`
}

type QueryResponse struct {
	Results []*QueryResult `json:"results"`
}

type QueryRequest struct {
	Tables      map[string]*Table
	Start       time.Time
	End         time.Time
	Aggregation string
	Interval    string
	Fill        string
	Offset      int
	Limit       int
}

func NewQueryRequest() *QueryRequest {
	return &QueryRequest{Tables: map[string]*Table{}}
}

func (request *QueryRequest) WithStart(start time.Time) *QueryRequest {
	request.Start = start
	return request
}
func (request *QueryRequest) WithEnd(end time.Time) *QueryRequest {
	request.End = end
	return request
}
func (request *QueryRequest) WithOffset(offset int) *QueryRequest {
	request.Offset = offset
	return request
}
func (request *QueryRequest) WithLimit(limit int) *QueryRequest {
	request.Limit = limit
	return request
}
func (request *QueryRequest) WithAggregation(aggregation string) *QueryRequest {
	request.Aggregation = aggregation
	return request
}
func (request *QueryRequest) WithInterval(interval string) *QueryRequest {
	request.Interval = interval
	return request
}
func (request *QueryRequest) WithFill(fill string) *QueryRequest {
	request.Fill = fill
	return request
}
func (request *QueryRequest) AddTable(table *Table) *QueryRequest {
	request.Tables[table.TableName] = table
	return request
}
