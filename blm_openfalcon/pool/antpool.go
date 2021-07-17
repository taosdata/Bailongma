package pool

import (
	"bailongma/v2/blm_openfalcon/config"
	"github.com/panjf2000/ants/v2"
)

var GoroutinePool *ants.Pool

func init() {
	var err error
	GoroutinePool, err = ants.NewPool(config.Conf.GoPoolSize)
	if err != nil {
		panic(err)
	}
}
