package metricgroup

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/taosdata/Bailongma/blm_openfalcon/config"
	"github.com/taosdata/Bailongma/blm_openfalcon/log"
	"github.com/taosdata/go-utils/util"
	"regexp"
)

var (
	metricGroupConfig        = make(map[string]string)
	metricPatternGroupConfig = make(map[*regexp.Regexp]string)
	configGroups             = map[string]*Config{}
)

type Config struct {
	Metrics       []string
	EnablePattern bool
}

func loadMetricGroup(metricGroupPath string) (map[string]*Config, error) {
	metricGroups := map[string]*Config{}
	if util.PathExist(metricGroupPath) {
		if _, err := toml.DecodeFile(metricGroupPath, &metricGroups); err != nil {
			return nil, err
		}
	}
	patternMap := map[string]string{}
	for groupName, group := range metricGroups {
		if !group.EnablePattern {
			for _, metric := range group.Metrics {
				preMetricGroup, exist := metricGroupConfig[metric]
				if exist {
					return nil, fmt.Errorf("metric : %s redefined in %s and %s", metric, preMetricGroup, groupName)
				}
				metricGroupConfig[metric] = groupName
			}
		} else {
			for _, metric := range group.Metrics {
				preMetricGroup, exist := patternMap[metric]
				if exist {
					return nil, fmt.Errorf("pattern metric : %s redefined in %s and %s", metric, preMetricGroup, groupName)
				} else {
					patternMap[metric] = groupName
					re, err := regexp.Compile(metric)
					if err != nil {
						return nil, err
					}
					metricPatternGroupConfig[re] = groupName
				}
			}
		}
	}
	return metricGroups, nil
}

type Info struct {
	Group          string
	PatternMatched bool
}

func GetMetricGroupInfo(metric string) (*Info, bool) {
	//优先匹配非正则
	group, isCommonMetric := metricGroupConfig[metric]
	if isCommonMetric {
		return &Info{
			Group:          group,
			PatternMatched: false,
		}, true
	} else {
		for re, group := range metricPatternGroupConfig {
			if re.MatchString(metric) {
				return &Info{
					Group:          group,
					PatternMatched: true,
				}, true
			}
		}
	}
	return nil, false
}

func GetMetricGroupField(group string) []string {
	info, ok := configGroups[group]
	if !ok {
		return nil
	}
	return info.Metrics
}

func GetMetricGroups() map[string]*Config {
	return configGroups
}
func init() {
	groups, err := loadMetricGroup(config.Conf.MetricGroupPath)
	if err != nil {
		log.Logger.WithError(err).Panic("load metric group config error")
	}
	configGroups = groups
}
