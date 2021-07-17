package log

import (
	"bailongma/v2/blm_openfalcon/config"
	"bailongma/v2/infrastructure/log"
)

var Logger = log.NewLogger("openfalcon")

func init() {
	log.SetLevel(config.Conf.LogLevel)
}
