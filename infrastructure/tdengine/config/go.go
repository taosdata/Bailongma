package config

import (
	"os"
	"strconv"
)

type TDengineGo struct {
	Address     string
	MaxIdle     int
	MaxOpen     int
	MaxLifetime int
}

func (t *TDengineGo) Init() {
	if t.Address == "" {
		if addr := os.Getenv("TDENGINE_ADDR"); addr != "" {
			t.Address = addr
		}
		if t.Address == "" {
			t.Address = "root:taosdata@/tcp(127.0.0.1:6030)/"
		}
	}

	if t.MaxIdle == 0 {
		if val := os.Getenv("TDENGINE_MAX_IDLE"); val != "" {
			i, err := strconv.Atoi(val)
			if err != nil {
				panic(val)
			}
			t.MaxIdle = i
		}
		if t.MaxIdle == 0 {
			t.MaxIdle = 20
		}
	}

	if t.MaxOpen == 0 {
		if val := os.Getenv("TDENGINE_MAX_OPEN"); val != "" {
			i, err := strconv.Atoi(val)
			if err != nil {
				panic(err)
			}
			t.MaxOpen = i
		}
		if t.MaxOpen == 0 {
			t.MaxOpen = 200
		}
	}
	if t.MaxLifetime == 0 {
		if val := os.Getenv("TDENGINE_MAX_LIFE_TIME"); val != "" {
			i, err := strconv.Atoi(val)
			if err != nil {
				panic(err)
			}
			t.MaxLifetime = i
		}
		if t.MaxLifetime == 0 {
			t.MaxLifetime = 30
		}
	}
}
