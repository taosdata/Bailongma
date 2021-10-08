package config

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	poolConfig "github.com/taosdata/go-utils/pool/config"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/config"
	"github.com/taosdata/go-utils/util"
	"github.com/taosdata/go-utils/web"
	"log"
)

type Config struct {
	Cors             web.CorsConfig
	Debug            bool
	Port             int
	DBName           string
	TDengineConnType string
	TDengineRestful  *config.TDengineRestful
	DataKeep         int //数据保留天数
	Update           int //历史数据覆盖策略
	ShowSQL          bool
	EnableGzip       bool
	CacheLast        int
	TDengineGo       *config.TDengineGo
	LogLevel         string
	GoPoolSize       int
	StoreWorker      int
}

var (
	Conf           *Config
	configPath     = "./config/blm_n9e_v5.toml"
	TDengineConfig interface{}
)

func init() {
	cp := flag.String("c", "", "default ./config/blm_n9e_v5.toml")
	flag.Parse()
	if *cp != "" {
		configPath = *cp
	}
	fmt.Println("load config :", configPath)
	var conf Config
	if util.PathExist(configPath) {
		if _, err := toml.DecodeFile(configPath, &conf); err != nil {
			log.Fatal(err)
		}
	}
	conf.Cors.Init()
	if conf.Port == 0 {
		conf.Port = 9090
	}
	if conf.TDengineConnType == "" {
		conf.TDengineConnType = common.TDengineRestfulConnectorType
	}
	switch conf.TDengineConnType {
	case common.TDengineRestfulConnectorType:
		if conf.TDengineRestful == nil {
			panic("TDengineRestful is empty")
		}
		TDengineConfig = conf.TDengineRestful
	case common.TDengineGoConnectorType:
		if conf.TDengineGo == nil {
			panic("TDengineGo is empty")
		}
		TDengineConfig = conf.TDengineGo
	}

	if conf.DBName == "" {
		conf.DBName = "n9e"
	}

	if conf.DataKeep == 0 {
		conf.DataKeep = 3650
	}
	if conf.LogLevel == "" {
		conf.LogLevel = "info"
	}
	if conf.GoPoolSize == 0 {
		conf.GoPoolSize = 50000
	}
	if conf.StoreWorker <= 0 {
		conf.StoreWorker = 20
	}
	poolConfig.GoPoolSize = conf.GoPoolSize
	Conf = &conf

}
