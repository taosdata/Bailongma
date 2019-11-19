## TDengine Remote Write Adapter 

This is an adapter to support Prometheus remote write into TDengine.

## prerequisite

before running the software, you need to install the `golang-1.10` or later version in your environment and install [TDengine][] so the program can use the lib of TDengine.

To use it:

```
go build
./blm_prometheus
```
During the go build process, there maybe some errors arised because of lacking some needed packages. You can use `go get` the package to solve it
```
go get github.com/gogo/protobuf/proto
go get github.com/golang/snappy
go get github.com/prometheus/common/model
go get github.com/taosdata/TDengine/src/connector/go/src/taosSql
go get github.com/prometheus/prometheus/prompb

```


...and then add the following to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://localhost:1234/receive"
```

Then start Prometheus:

```
 prometheus
```
There are several options can be set:

```sh
--host 
set the host of TDengine, IP:port, for example "192.168.0.1:0"

--batch-size 
set the size of how many records in one SQL cmd line writing into TDengine. There is a limitation that TDengine could only accept SQL line small than 64000 bytes, so usually the batch size should not exceed 200. Default is 10.

--http-workers
set the number of workers who process the HTTP request. default is 10

--sql-workers
set the number of workers who process the database request. default is 10 

--dbname
set the database name in TDengine, if not exists, a database will be created after this dbname. default is "prometheus".

--dbuser
set the user name that have the right to access the TDengine. default is "root"

--dbpassword
set the password of dbuser. default is "taosdata"

--port
set the port that prometheus configuration remote_write. as showed above, in the prometheus.yaml
```


## Running in background

Using following command to run the program in background

```
nohup ./blm_prometheus > /dev/null 2>&1 &
```

Then you can check the TDengine if there is super table and tables.

## Check the result

Use the taos client shell to query the result.
```
Welcome to the TDengine shell from linux, client version:1.6.4.0 server version:1.6.4.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

This is the trial version and will expire at 2019-12-11 14:25:31.

taos> use prometheus;
Database changed.

taos> show stables;
                              name                              |     created_time     |columns| tags  |  tables   |
====================================================================================================================
prometheus_sd_kubernetes_cache_watch_events_sum                 | 19-11-15 17:45:07.594|      2|      3|          1|
prometheus_sd_kubernetes_cache_watch_events_count               | 19-11-15 17:45:07.596|      2|      3|          1|
prometheus_sd_kubernetes_cache_watches_total                    | 19-11-15 17:45:07.598|      2|      3|          1|
prometheus_sd_kubernetes_events_total                           | 19-11-15 17:45:07.600|      2|      5|         15|
prometheus_target_scrape_pool_reloads_total                     | 19-11-15 17:45:07.672|      2|      3|          1|
prometheus_sd_received_updates_total                            | 19-11-15 17:45:07.674|      2|      4|          1|
prometheus_target_scrape_pool_reloads_failed_total              | 19-11-15 17:45:07.730|      2|      3|          1|
prometheus_sd_updates_total                                     | 19-11-15 17:45:07.732|      2|      4|          1|
prometheus_target_scrape_pool_sync_total                        | 19-11-15 17:45:07.734|      2|      4|          1|
......

go_memstats_gc_cpu_fraction                                     | 19-11-15 17:45:06.599|      2|      3|          1|
Query OK, 211 row(s) in set (0.004891s)


taos> select * from prometheus_sd_updates_total;

......

 19-11-16 14:24:00.271|              1.000000000|localhost:9090                                    |prometheus                                        |codelab-monitor                                   |scrape                                            |
 19-11-16 14:24:05.271|              1.000000000|localhost:9090                                    |prometheus                                        |codelab-monitor                                   |scrape                                            |
Query OK, 3029 row(s) in set (0.060828s)


```




## Limitations

The TDengine limits the length of super table name, so if the name of prometheus metric exceeds 60 byte, it will be truncated to first 60 bytes. And the length of label name is limited within 50 byte.  


[TDengine]:https://www.github.com/Taosdata/TDengine