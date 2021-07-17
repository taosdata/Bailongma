package common

import "fmt"

const (
	BasicAuthType = "Basic"
	TaosdAuthType = "Taosd"
)
const (
	TDengineRestfulConnectorType = "restful"
	TDengineGoConnectorType      = "go"
)

type TDengineError struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func (t *TDengineError) Error() string {
	return fmt.Sprintf("TDengine return error code: %d, desc: %s", t.Code, t.Desc)
}

const (
	TableType  = "table"
	STableType = "stable"
)

const (
	NCHARType  = "NCHAR"
	DOUBLEType = "DOUBLE"
	BINARYType = "BINARY"
)
