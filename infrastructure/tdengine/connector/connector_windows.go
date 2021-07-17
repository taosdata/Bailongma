// +build windows

package connector

import (
	"bailongma/v2/infrastructure/tdengine/common"
	"bailongma/v2/infrastructure/tdengine/config"
	"errors"
	"fmt"
)

func NewTDengineConnector(connectorType string, conf interface{}) (TDengineConnector, error) {
	switch connectorType {
	case common.TDengineRestfulConnectorType:
		restfulConfig := conf.(*config.TDengineRestful)
		return NewRestfulConnector(restfulConfig)
	case common.TDengineGoConnectorType:
		return nil, errors.New("not support on windows")
	default:
		return nil, fmt.Errorf("unsupported TDengine connector type %s", connectorType)
	}
}
