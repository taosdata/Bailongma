package pool

import (
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	"sync"
)

var rrdPool sync.Pool

func init() {
	rrdPool.New = func() interface{} {
		return &cmodel.RRDData{}
	}
}

func RRDPoolGet() *cmodel.RRDData {
	return rrdPool.Get().(*cmodel.RRDData)
}

func RRDPoolPut(b *cmodel.RRDData) {
	rrdPool.Put(b)
}
