# Bailongma 对接 open-falcon 3.0 监控数据

修改 open-falcon transfer 模块将监控数据发到 Bailongma 后将数据存储到 TDengine

## 思路

### 数据接入

open-falcon transfer 模块负责将 agent 上报数据转发到各个下游模块,对 transfer 进行修改添加对 Bailongma 的支持。  
Bailongma 使用 HTTP 方式接收数据。

### 建表思路

#### 指标组

由于监控指标和标签过多如果 （设备 + 指标 + 标签）对应一张表，表的数量会非常多,因此支持配置指标组的方式来将一组指标合并到一张超级表内来减少表的数量  
配置示例如下

```toml
[sys]
Metrics = ['sys.fs.files.used', 'sys.net.netfilter.nf_conntrack_count.percent', 'sys.fs.files.max', 'sys.fs.files.used.percent', 'sys.ps.process.total', 'sys.net.netfilter.nf_conntrack_count', 'sys.fs.files.free', 'sys.net.netfilter.nf_conntrack_max', 'sys.ps.entity.total']
[disk]
Metrics = ['disk.io.read.request', 'disk.io.read.bytes', 'disk.io.util', 'disk.io.write.request', 'disk.io.svctm', 'disk.io.write.bytes', 'disk.io.avgqu_sz', 'disk.io.avgrq_sz', 'disk.io.await']
```

如上示例将会创建两张超级表 表名为 `sys` 和 `disk`  
列分别为对应 metrics 内元素的 md5 值加前缀 'md5_', 如 `sys.fs.files.used` 对应为 `md5_1579002970d373b5f439f82daee04b25`  
同时创建三个 tag 标签 `openfalcon_endpoint` `openfalcon_metric` `openfalcon_tag`

```sql
create
stable if not exists openfalcon.disk (ts timestamp,md5_70c582558bfb79754de258207fff45cb DOUBLE,md5_b103d7f8adf11867ebc77dfd8f064fdb DOUBLE,md5_6eebf4677cd4c079bd8701286eefc65b DOUBLE,md5_c52946e5ded5735d27e1bb38d4d6158e DOUBLE,md5_6b07e8c602cc488dab0cae5f6b8645a9 DOUBLE,md5_ae318eb8e0d9c733f3b19d5614047821 DOUBLE,md5_b738cdd339e014a1ea70917ee736edfd DOUBLE,md5_ffd35e17c160c08ae2c5fa4a876a7e04 DOUBLE,md5_e78a3b28c9c9706c205707235781a0ee DOUBLE) tags (openfalcon_endpoint NCHAR(256),openfalcon_metric NCHAR(256),openfalcon_tag NCHAR(256))
```

当插入数据为

```json
{
  "endpoint": "node1",
  "metric": "sys.fs.files.used.percent",
  "value": 1,
  "step": 30,
  "timestamp": 1623310671,
  "counterType": "GAUGE",
  "tags": "device=sda"
}
```

语句为

```sql
insert into openfalcon.md5_1cd2e0d56175a5d0d3166e36d68001e4 using openfalcon.sys tags ('node1','sys','device=sda')
values ('2021-06-10T15:37:51+08:00', null, null, null, 1, null, null, null, null, null)
```

表名为 "md5_"+MD5(endpoint+"/"+metric+"/"+tags)

#### 单列表

如果未配置指标组则会（设备 + 指标 + 标签）对应一张表  
默认创建一个超级表`openfalcon_single_stable`

```sql
create
stable if not exists openfalcon.openfalcon_single_stable (ts timestamp,value DOUBLE) tags (openfalcon_endpoint NCHAR(256),openfalcon_metric NCHAR(256),openfalcon_tag NCHAR(256))
```

所有单列表均由此超级表产生  
传入数据如

```json
{
  "endpoint": "node1",
  "metric": "qps",
  "timestamp": 1623310674,
  "step": 10,
  "tags": "site=1",
  "value": 4,
  "counterType": "GAUGE"
}
```

```sql
insert into openfalcon.md5_23ca9bd06bcc2d28b9b3d30042587b86 using openfalcon.openfalcon_single_stable tags ('node1','qps','site=1') values ('2021-06-10T15:37:54+08:00',4.000000)
```

### 配置文件

```toml
#开启调试
Debug = true
#http端口
Port = 8090
#db名称
DBName = "openfalcon"
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
#metric组配置文件路径
MetricGroupPath = "./config/metric_group.toml"
#是否显示 SQL
ShowSQL = true
#是否启用 GZip
EnableGzip = false
#日志等级 "panic" "fatal" "error" "warn" "info" "debug" "trace"
LogLevel = "info"
#goroutinue 池大小
GoPoolSize = 50000

[TDengineRestful]
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

[TDengineGo]
#地址
Address = "root:taosdata@/tcp(127.0.0.1:6030)/"
#最大空闲连接
MaxIdle = 20
#最大连接数
MaxOpen = 200
#连接最长生命周期（秒）
MaxLifetime = 30
```

## 数据查询

修改 open-falcon graph 组件将查询通过 HTTP 请求发送到 Bailongma

## 优化过程

1. 插入和查询 sql 语句 使用 bytes.Buffer 组装
2. 查询结果数据量大进行分片异步处理
3. 对需要大量创建的对象构建对象池减少创建对象耗时
4. 查询从超级表改为直接查询对应表
5. 统一封装连接器统一 restful 与 driver-go
6. 添加 jsoniter 可替换原生 json
