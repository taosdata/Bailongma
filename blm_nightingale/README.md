# blm integration with n9e
using remote_read and remote_write  

[中文文档](./README_zh.md)
## Configure
Configure relative execution directory `./config/blm_n9e_v5.toml`  
Or `-c` parameter to specify the configuration file path
Sample
```toml
# dubug mod
Debug = true
#http port
Port = 9090
#db name
DBName = "n9e"
#tdengine connection type [restful go]
TDengineConnType = "go"
#max endpoint length
MaxEndpointLength = 256
#max tag length
MaxTagLength = 256
#max metric length
MaxMetricLength = 256
#data storage days
DataKeep = 3650
#historical data insertion strategy
Update = 1
#whether to display SQL
ShowSQL = true
#whether to enable GZip
EnableGzip = false
#log level "panic" "fatal" "error" "warn" "info" "debug" "trace"
LogLevel = "info"
#goroutinue pool size
GoPoolSize = 50000
#insert workers pre db connection
InsertWorkerPreDBGroup = 5

[[TDengineGoGroup]]
#address
Address = "root:taosdata@/tcp(127.0.0.1:6030)/"
#max idle
MaxIdle = 20
#maximum number of connections
MaxOpen = 200
#Maximum connection lifetime (seconds)
MaxLifetime = 30
Metrics = ['cpu.*']
[[TDengineGoGroup]]
Address = "root:taosdata@/tcp(node159:6030)/"
MaxIdle = 20
MaxOpen = 200
MaxLifetime = 30
#metric grouping is divided by'_','?' represents a string and'*' represents 0 or more strings
#"A_?" can match "A_A" "A_B" "A_C" cannot match "A" "A_B_C"
#"A_*" can match "A_A" "A_B" "A_C" "A" "A_B_C"
#'?' match priority is greater than'*'
#After all grouping and summarization, there must be a'*' as the default item that is not matched
metrics = ['*']

[[TDengineRestfulGroup]]
#address
Address = "http://127.0.0.1:6041"
#auth type [Basic Taosd]
AuthType = "Basic"
#username
Username = "root"
#password
Password = "taosdata"
#Maximum number of connections per host -1 means unlimited
MaxConnsPerHost = 10
Metrics = ['cpu_*']

[[TDengineRestfulGroup]]
Address = "http://192.168.1.159:6041"
AuthType = "Basic"
Username = "root"
Password = "taosdata"
MaxConnsPerHost = 10
metrics = ['*']
```

`TDengineConnType` If it is "go", it will read TDengineGoGroup in order, if it is "restful", it will read TDengineRestfulGroup in order
All programs need to read the same configuration file

## Compilation
1. Install TDengine client first under linux [TDengine DOC](https://www.taosdata.com/cn/getting-started/)
2. Prepare golang environment [install golang](https://golang.org/doc/install) 1.14 or later version
3. Compile
   1. execute `go build -o blm_n9e`

## Build docker image
Modify `tdengine_ver` in `Dockerfile` to TDengine version, and `extension` to suffix

```dockerfile
ARG tdengine_ver=2.1.5.0
# If it is a beta version, write `-beta`, otherwise leave it blank
ARG extension=-beta
```
build
```shell
docker build -t blm_n9e:latest
```
run
```shell
docker run -d --name=blm_n9e_node1 -p 9090:9090 -v /path_to_config:/root/config blm_n9e:latest
```
`/path_to_config` fill in the absolute path of the corresponding configuration folder on the host

## Manual installation instructions
check [install_shell.sh](./install_shell.sh)