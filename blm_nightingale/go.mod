module github.com/taosdata/Bailongma/blm_nightingale

go 1.14

replace github.com/taosdata/go-utils => ../../go-utils

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/gin-gonic/gin v1.7.2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/prometheus/prometheus v1.8.2-0.20210827082440-752c4f11ae86
	github.com/taosdata/go-utils v0.0.0-20210811075537-2ce661ccb743
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)
