package log

import (
	"github.com/taosdata/Bailongma/blm_openfalcon/config"
	"github.com/taosdata/go-utils/log"
)

var Logger = log.NewLogger("openfalcon")

func init() {
	log.SetLevel(config.Conf.LogLevel)
}
