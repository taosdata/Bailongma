package log

import (
	"github.com/taosdata/Bailongma/blm_nightingale/config"
	"github.com/taosdata/go-utils/log"
)

var Logger = log.NewLogger("n9e")

func init() {
	log.SetLevel(config.Conf.LogLevel)
}
