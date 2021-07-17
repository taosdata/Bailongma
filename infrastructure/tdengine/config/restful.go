package config

import (
	"bailongma/v2/infrastructure/tdengine/common"
	"os"
	"strconv"
)

type TDengineRestful struct {
	Address         string
	AuthType        string
	Username        string
	Password        string
	MaxConnsPerHost int
}

func (conf *TDengineRestful) Init() {
	if conf.Address == "" {
		if val := os.Getenv("TDengineAddress"); val != "" {
			conf.Address = val
		} else {
			conf.Address = "http://127.0.0.1:6041"
		}
	}
	if conf.AuthType == "" {
		if val := os.Getenv("TDengineAuthType"); val != "" {
			conf.AuthType = val
		} else {
			conf.AuthType = common.BasicAuthType
		}
	}
	if conf.Username == "" {
		if val := os.Getenv("TDengineUsername"); val != "" {
			conf.Username = val
		} else {
			conf.Username = "root"
		}
	}
	if conf.Password == "" {
		if val := os.Getenv("TDenginePassword"); val != "" {
			conf.Password = val
		} else {
			conf.Password = "taosdata"
		}
	}
	if conf.MaxConnsPerHost == 0 {
		if val := os.Getenv("TdengineMaxConnsPerHost"); val != "" {
			v, err := strconv.Atoi(val)
			if err != nil {
				panic(err)
			}
			conf.MaxConnsPerHost = v
		} else {
			conf.MaxConnsPerHost = -1
		}
	}
}
