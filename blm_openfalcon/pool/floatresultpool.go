package pool

import (
	"sync"
)

type FloatValueResponse struct {
	Timestamp int64    `json:"timestamp"`
	Value     *float64 `json:"value"`
}

var floatValueResponsePool sync.Pool

func init() {
	floatValueResponsePool.New = func() interface{} {
		return &FloatValueResponse{}
	}
}

func FloatValueResponsePoolGet() *FloatValueResponse {
	return floatValueResponsePool.Get().(*FloatValueResponse)
}

func FloatValueResponsePoolPut(b *FloatValueResponse) {
	floatValueResponsePool.Put(b)
}
