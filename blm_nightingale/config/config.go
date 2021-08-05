package config

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/taosdata/Bailongma/blm_nightingale/trie"
	poolConfig "github.com/taosdata/go-utils/pool/config"
	"github.com/taosdata/go-utils/tdengine/common"
	tdengineConfig "github.com/taosdata/go-utils/tdengine/config"
	"github.com/taosdata/go-utils/util"
	"github.com/taosdata/go-utils/web"
	"log"
)

type Config struct {
	Cors                   web.CorsConfig
	Debug                  bool
	Port                   int
	DBName                 string
	TDengineConnType       string
	TDengineRestfulGroup   []*TDengineRestful
	MaxEndpointLength      int
	MaxMetricLength        int
	MaxTagLength           int
	DataKeep               int //数据保留天数
	Update                 int //历史数据覆盖策略
	ShowSQL                bool
	EnableGzip             bool
	CacheLast              int
	TDengineGoGroup        []*TDengineGo
	LogLevel               string
	GoPoolSize             int
	InsertWorkerPreDBGroup int //每个db连接的worker
}

type TDengineRestful struct {
	Address         string
	AuthType        string
	Username        string
	Password        string
	MaxConnsPerHost int
	Metrics         []string
}

type TDengineGo struct {
	Address     string
	MaxIdle     int
	MaxOpen     int
	MaxLifetime int
	Metrics     []string
}

var (
	Conf                 *Config
	configPath           = "./config/blm_n9e_v5.toml"
	TDengineConfig       []interface{}
	MetricGroups         [][]string
	DefaultMetricGroupID int
	searchTree           *trie.Trie
)

func GetDBIndex(metric string) int {
	index, found := searchTree.Find(metric)
	if !found {
		return DefaultMetricGroupID
	}
	if index == nil {
		return DefaultMetricGroupID
	}
	return *index
}

func init() {
	cp := flag.String("config", "", "default ./config/blm_n9e_v5.toml")
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
		if len(conf.TDengineRestfulGroup) == 0 {
			panic("TDengineRestfulGroup is empty")
		}
		TDengineConfig = make([]interface{}, 0, len(conf.TDengineRestfulGroup))
		MetricGroups = make([][]string, len(conf.TDengineRestfulGroup))
		for i, c := range conf.TDengineRestfulGroup {
			if c != nil {
				TDengineConfig = append(TDengineConfig, &tdengineConfig.TDengineRestful{
					Address:         c.Address,
					AuthType:        c.AuthType,
					Username:        c.Username,
					Password:        c.Password,
					MaxConnsPerHost: c.MaxConnsPerHost,
				})
			} else {
				panic("invalid TDengineRestfulGroup")
			}
			MetricGroups[i] = c.Metrics
		}
	case common.TDengineGoConnectorType:
		if len(conf.TDengineGoGroup) == 0 {
			panic("TDengineGoGroup is empty")
		}
		TDengineConfig = make([]interface{}, 0, len(conf.TDengineRestfulGroup))
		MetricGroups = make([][]string, len(conf.TDengineGoGroup))
		for i, c := range conf.TDengineGoGroup {
			if c != nil {
				TDengineConfig = append(TDengineConfig, &tdengineConfig.TDengineGo{
					Address:     c.Address,
					MaxIdle:     c.MaxIdle,
					MaxOpen:     c.MaxOpen,
					MaxLifetime: c.MaxLifetime,
				})
			} else {
				panic("invalid TDengineGoGroup")
			}
			MetricGroups[i] = c.Metrics
		}
	}
	searchTree = trie.New()
	DefaultMetricGroupID = -1
	for i, group := range MetricGroups {
		for _, s := range group {
			_, err := searchTree.Add(s, i)
			if err != nil {
				panic(err)
			}
			if s == "*" {
				DefaultMetricGroupID = i
			}
		}
	}
	if DefaultMetricGroupID == -1 {
		panic("There must be a '*' in the metric group")
	}
	if conf.DBName == "" {
		conf.DBName = "n9e"
	}
	if conf.MaxEndpointLength == 0 {
		conf.MaxEndpointLength = 256
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
	if conf.InsertWorkerPreDBGroup <= 0 {
		conf.InsertWorkerPreDBGroup = 5
	}
	poolConfig.GoPoolSize = conf.GoPoolSize
	Conf = &conf

}
