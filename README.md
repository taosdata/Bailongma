# 通过Bailongma API服务程序接入Prometheus和Telegraf

TDengine在原生连接器通过TAOS SQL写入数据之外，还支持通过API服务程序来接入Prometheus和Telegraf的数据，仅需在Prometheus和Telegraf添加相关配置，即可将数据直接写入TDengine中，并按规则自动创建库和相关表项，无需任何代码，以及提前在TDengine做任何配置。
这篇博文[用Docker容器快速搭建一个Devops监控Demo](https://www.taosdata.com/blog/2020/02/03/1189.html)即是采用bailongma将Prometheus和Telegraf的数据写入TDengine中的示例，可以参考。

### Bailongma 
[Bailongma](https://github.com/taosdata/Bailongma)是TDengine团队开发的开源数据接入API服务，目前支持Prometheus和Telegraf通过增加配置后直接将数据写入TDengine中。

## 从源代码编译Bailongma

目前bailongma需要从源码进行编译后运行，因此需要从github上下载Bailongma的源码后，使用Golang语言编译器编译生成可执行文件。
在开始编译前，需要准备好以下条件：
- Linux操作系统的服务器
- 安装好Golang，1.10版本以上
- 对应的TDengine版本。因为用到了TDengine的客户端动态链接库，因此需要安装好和服务端相同版本的TDengine程序；比如服务端版本是TDengine 2.0.0,则在bailongma所在的linux服务器（可以与TDengine在同一台服务器，或者不同服务器）
Bailongma项目中有两个文件夹blm_prometheus和blm_telegraf，分别存放了prometheus和Telegraf的写入API程序，编译方法都相同。以prometheus写入程序为例，编译过程如下
```
go mod init bailongma/v2 

cd blm_prometheus
go build
```
一切正常的情况下，就会在对应的目录下生成一个blm_prometheus的可执行程序。同样的操作步骤编译成blm_telegraf的可执行文件。

## Prometheus
[Prometheus](https://www.prometheus.io/)作为Cloud Native Computing Fundation毕业的项目，在性能监控以及K8S性能监控领域有着非常广泛的应用。TDengine可以通过bailongma API服务程序实现无代码的快速接入，高效的将数据写入TDengine中。并通过Grafana来查询TDengine中的数据

### 安装Prometheus
通过Prometheus的官网下载安装。[下载地址](https://prometheus.io/download/)

### 配置Prometheus
参考Prometheus的[配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/),在Prometheus的配置文件中的<remote_write>部分，增加以下配置
- url: bailongma API服务提供的URL, 参考下面的blm_prometheus启动示例章节
启动Prometheus后，可以通过taos客户端查询确认数据是否成功写入。

### 启动blm_prometheus程序
blm_prometheus程序有以下选项，在启动blm_prometheus程序时可以通过设定这些选项来设定blm_prometheus的配置。
```sh
--tdengine-ip 
TDengine服务端的IP地址，缺省值为127.0.0.1

--tdengine-name
如果TDengine安装在一台具备域名的服务器上，也可以通过配置TDengine的域名来访问TDengine。在K8S环境下，可以配置成TDengine所运行的service name

--batch-size 
blm_prometheus会将收到的prometheus的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，blm_prometheus会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'

--port
blm_prometheus对prometheus提供服务的端口号。
```
### 启动示例
通过以下命令启动一个blm_prometheus的API服务
```
./blm_prometheus -port 8088
```
则在prometheus的配置文件中,假设blm_prometheus所在服务器的IP地址为"10.1.2.3"，<remote_write>部分增加url为
```yaml
remote_write:
  - url: "http://10.1.2.3:8088/receive"
```
### 查询prometheus写入数据
prometheus产生的数据格式如下：
```
{
Timestamp: 1576466279341,
Value: 37.000000, 
apiserver_request_latencies_bucket {
component="apiserver", 
instance="192.168.99.116:8443", 
job="kubernetes-apiservers", 
le="125000", 
resource="persistentvolumes", s
cope="cluster",
verb="LIST", 
version=“v1" 
}
```
其中，apiserver_request_latencies_bucket为prometheus采集的时序数据的名称，后面{}中的为该时序数据的标签。blm_prometheus会以时序数据的名称在TDengine中自动创建一个超级表，并将{}中的标签转换成TDengine的tag值，Timestamp作为时间戳，value作为该时序数据的值。
因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。
```
use prometheus;
select * from apiserver_request_latencies_bucket;
```

## Telegraf

TDengine能够与开源数据采集系统[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/)快速集成，整个过程无需任何代码开发。

### 安装Telegraf

目前TDengine支持Telegraf 1.7.4以上的版本。用户可以根据当前的操作系统，到Telegraf官网下载安装包，并执行安装。下载地址如下：https://portal.influxdata.com/downloads

### 配置Telegraf

修改Telegraf配置文件/etc/telegraf/telegraf.conf中与TDengine有关的配置项。 

在output plugins部分，增加[[outputs.http]]配置项： 

- url： bailongma API服务提供的URL, 参考下面的启动示例章节
- data_format: "json"
- json_timestamp_units:      "1ms"

在agent部分：

- hostname: 区分不同采集设备的机器名称，需确保其唯一性
- metric_batch_size: 100，允许Telegraf每批次写入记录最大数量，增大其数量可以降低Telegraf的请求发送频率。

关于如何使用Telegraf采集数据以及更多有关使用Telegraf的信息，请参考Telegraf官方的[文档](https://docs.influxdata.com/telegraf/v1.11/)。

### 启动blm_telegraf程序
blm_telegraf程序有以下选项，在启动blm_telegraf程序时可以通过设定这些选项来设定blm_telegraf的配置。
```sh
--host 
TDengine服务端的IP地址，缺省值为空

--batch-size 
blm_telegraf会将收到的telegraf的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，blm_telegraf会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'

--port
blm_telegraf对telegraf提供服务的端口号。
```
### 启动示例
通过以下命令启动一个blm_telegraf的API服务
```
./blm_telegraf -host 127.0.0.1 -port 8089
```
则在telegraf的配置文件中,假设blm_telegraf所在服务器的IP地址为"10.1.2.3"，在output plugins部分，增加[[outputs.http]]配置项： 
```yaml
url = "http://10.1.2.3:8089/telegraf"
```
### 查询telegraf写入数据
telegraf产生的数据格式如下：
```
{
  "fields": {
    "usage_guest": 0, 
    "usage_guest_nice": 0,
    "usage_idle": 89.7897897897898, 
    "usage_iowait": 0,
    "usage_irq": 0,
    "usage_nice": 0,
    "usage_softirq": 0,
    "usage_steal": 0,
    "usage_system": 5.405405405405405, 
    "usage_user": 4.804804804804805
  },
  "name": "cpu", 
  "tags": {
    "cpu": "cpu2",
    "host": "bogon" 
    },
  "timestamp": 1576464360 
}
```
其中，name字段为telegraf采集的时序数据的名称，tags字段为该时序数据的标签。blm_telegraf会以时序数据的名称在TDengine中自动创建一个超级表，并将tags字段中的标签转换成TDengine的tag值，Timestamp作为时间戳，fields字段中的值作为该时序数据的值。
因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。
```
use telegraf;
select * from cpu;
```
