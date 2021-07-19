module bailongma/v2

replace (
	github.com/Sirupsen/logrus v1.8.1 => github.com/sirupsen/logrus v1.8.1
	github.com/sirupsen/logrus v1.8.1 => github.com/Sirupsen/logrus v1.8.1
)

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/Sirupsen/logrus v1.8.1 // indirect
	github.com/emirpasic/gods v1.12.0
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-contrib/gzip v0.0.3
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.7.2
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/jinzhu/gorm v1.9.16 // indirect
	github.com/json-iterator/go v1.1.11
	github.com/open-falcon/falcon-plus v0.2.2
	github.com/panjf2000/ants/v2 v2.4.6
	github.com/prometheus/common v0.29.0
	github.com/prometheus/prometheus v2.5.0+incompatible
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.8.1 // indirect
	github.com/taosdata/driver-go v0.0.0-20210706085040-976d33c51c3d
	github.com/toolkits/cache v0.0.0-20190218093630-cfb07b7585e5 // indirect
	github.com/toolkits/conn_pool v0.0.0-20170512061817-2b758bec1177 // indirect
	github.com/toolkits/consistent v0.0.0-20150827090850-a6f56a64d1b1 // indirect
	github.com/toolkits/container v0.0.0-20151219225805-ba7d73adeaca // indirect
	github.com/toolkits/str v0.0.0-20160913030958-f82e0f0498cb // indirect
	github.com/toolkits/time v0.0.0-20160524122720-c274716e8d7f // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)
