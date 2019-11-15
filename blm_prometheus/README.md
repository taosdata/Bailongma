## TDengine Remote Write Adapter 

This is an adapter to support Prometheus remote write into TDengine.

To use it:

```
go build
./server.go
```

...and then add the following to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://localhost:1234/receive"
```

Then start Prometheus:

```
./bailongma 
```
There are several options can be set:

```
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

## Limitations

The TDengine limits the length of super table name, so if the name of prometheus exceeds 60 byte, it will be processed by MD5 and use the digested name. and the length of label name is limited within 50 byte.  