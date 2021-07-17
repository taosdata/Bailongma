package connector

import "context"

type Data struct {
	Head []string        `json:"head"`
	Data [][]interface{} `json:"data"`
}

type TDengineConnector interface {
	Exec(ctx context.Context, sql string) (int64, error)
	Query(ctx context.Context, sql string) (*Data, error)
}
