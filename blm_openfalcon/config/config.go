package config

import (
	"bailongma/v2/infrastructure/tdengine/common"
	tdengineConfig "bailongma/v2/infrastructure/tdengine/config"
	"bailongma/v2/infrastructure/util"
	"bailongma/v2/infrastructure/web"
	"github.com/BurntSushi/toml"
	"log"
)

type Config struct {
	Cors              web.CorsConfig
	Debug             bool
	Port              int
	DBName            string
	TDengineConnType  string
	TDengineRestful   tdengineConfig.TDengineRestful
	MaxEndpointLength int
	MaxMetricLength   int
	MaxTagLength      int
	DataKeep          int //数据保留天数
	MetricGroupPath   string
	ShowSQL           bool
	EnableGzip        bool
	CacheLast         int
	TDengineGo        tdengineConfig.TDengineGo
	LogLevel          string
	GoPoolSize        int
}

var (
	Conf           *Config
	configPath     = "./config/config.toml"
	TDengineConfig interface{}
)

func init() {
	var conf Config
	if util.PathExist(configPath) {
		if _, err := toml.DecodeFile(configPath, &conf); err != nil {
			log.Fatal(err)
		}
	}
	conf.Cors.Init()
	if conf.Port == 0 {
		conf.Port = 8090
	}
	if conf.TDengineConnType == "" {
		conf.TDengineConnType = common.TDengineRestfulConnectorType
	}
	switch conf.TDengineConnType {
	case common.TDengineRestfulConnectorType:
		conf.TDengineRestful.Init()
		TDengineConfig = &conf.TDengineRestful
	case common.TDengineGoConnectorType:
		conf.TDengineGo.Init()
		TDengineConfig = &conf.TDengineGo
	}
	if conf.DBName == "" {
		conf.DBName = "openfalcon"
	}
	if conf.MaxEndpointLength == 0 {
		conf.MaxEndpointLength = 256
	}
	if conf.MetricGroupPath == "" {
		conf.MetricGroupPath = "./config/metric_group.toml"
	}
	if conf.DataKeep == 0 {
		conf.DataKeep = 3650
	}
	if conf.LogLevel == "" {
		conf.LogLevel = "info"
	}
	if conf.MaxMetricLength == 0 {
		conf.MaxMetricLength = 256
	}
	if conf.MaxTagLength == 0 {
		conf.MaxTagLength = 256
	}
	if conf.GoPoolSize == 0 {
		conf.GoPoolSize = 50000
	}
	Conf = &conf
}
