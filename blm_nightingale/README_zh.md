# Nightingale V5 对接

夜莺是新一代国产智能监控系统。对云原生场景、传统物理机虚拟机场景，都有很好的支持  
Nightingale V5 使用 remote_read 和 remote_write

## 配置文件
配置相对执行目录 `./config/blm_n9e_v5.toml`  
或 `-c` 参数指定配置文件路径  
样例
```toml
#开启调试
Debug = true
#http端口
Port = 9090
#db名称
DBName = "n9e"
#tdengine连接方式 restful go
TDengineConnType = "go"
#最长 endpoint 长度
MaxEndpointLength = 256
#最长标签值长度
MaxTagLength = 256
#最长指标长度
MaxMetricLength = 256
#数据保存天数
DataKeep = 3650
#历史数据插入策略
Update = 1
#是否显示 SQL
ShowSQL = true
#是否启用 GZip
EnableGzip = false
#日志等级 "panic" "fatal" "error" "warn" "info" "debug" "trace"
LogLevel = "info"
#goroutinue 池大小
GoPoolSize = 50000
#每个db连接的worker
InsertWorkerPreDBGroup = 5

[[TDengineGoGroup]]
#地址
Address = "root:taosdata@/tcp(127.0.0.1:6030)/"
#最大空闲连接
MaxIdle = 20
#最大连接数
MaxOpen = 200
#连接最长生命周期（秒）
MaxLifetime = 30
Metrics = ['cpu.*']
[[TDengineGoGroup]]
#地址
Address = "root:taosdata@/tcp(node159:6030)/"
#最大空闲连接
MaxIdle = 20
#最大连接数
MaxOpen = 200
#连接最长生命周期（秒）
MaxLifetime = 30
#指标分组 以 '_' 分割, '?' 代表一个字符串 '*' 代表0个或多个字符串 
#"A_?" 可以匹配 "A_A" "A_B" "A_C" 不可以匹配 "A" "A_B_C"
#"A_*" 可以匹配 "A_A" "A_B" "A_C" "A" "A_B_C"
#'?' 匹配优先级大于 '*'
#所有分组汇总后一定要存在一个 '*' 作为未匹配到的默认项
metrics = ['*']

[[TDengineRestfulGroup]]
#连接地址
Address = "http://127.0.0.1:6041"
#验证方式 Basic Taosd
AuthType = "Basic"
#用户名
Username = "root"
#密码
Password = "taosdata"
#每个host最大连接数 -1代表无限制
MaxConnsPerHost = 10
Metrics = ['cpu_*']

[[TDengineRestfulGroup]]
#连接地址
Address = "http://192.168.1.159:6041"
#验证方式 Basic Taosd
AuthType = "Basic"
#用户名
Username = "root"
#密码
Password = "taosdata"
#每个host最大连接数 -1代表无限制
MaxConnsPerHost = 10
metrics = ['*']
```

`TDengineConnType` 如果为 go 会按顺序读取 TDengineGoGroup,如果为 restful 会按顺序读取 TDengineRestfulGroup

所有程序需要读取同一份配置文件

## 编译方法
1. linux 下先安装 TDengine 客户端 [TDengine 文档](https://www.taosdata.com/cn/getting-started/)
2. 准备 golang 环境 [安装golang](https://golang.google.cn/doc/install) 1.14以上版本
3. 编译
    1. 设置 go mod proxy `go env -w GOPROXY=https://goproxy.cn,direct`
    2. 执行 `go build -o blm_n9e`

## docker 构建 image
修改`Dockerfile`中 tdengine_ver 为 TDengine 版本, extension 为后缀
如
```dockerfile
ARG tdengine_ver=2.1.5.0
# 如果是beta版本写 -beta 其他留空
ARG extension=-beta
```
构建
```shell
docker build -t blm_n9e:latest
```
运行
```shell
docker run -d --name=blm_n9e_node1 -p 9090:9090 -v /path_to_config:/root/config blm_n9e:latest
```
`/path_to_config`填写主机上对应配置文件夹的绝对路径

## 手动安装指令
见 [安装脚本](./install_shell.sh)