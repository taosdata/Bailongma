# API for Open-Falcon

This is an API to support Open-Falcon (v0.3 and above) writing data into TDengine.

## prerequisite

before running the software, you need to install the `golang-1.10` or later version in your environment and install [TDengine][] so the program can use the lib of TDengine.

To use it:

```
go build
```
During the go build process, there maybe some errors arised because of lacking some needed packages. You can use `go get` the package to solve it
```
go get github.com/taosdata/driver-go/taosSql

```
After successful build, there will be a blm_openfalcon in the same directory. 

## Running in background

Using following command to run the program in background

```
nohup ./blm_openfalcon --host 112.102.3.69:0 --batch-size 200 --http-workers 2 --sql-workers 2 --dbname openfalcon --port 1234 > /dev/null 2>&1 &
```
The API url is `http://ipaddress:port/openfalcon`

There are several options can be set:

```
--host 
set the host of TDengine, IP:port, for example "192.168.0.1:0"

--batch-size 
set the size of how many records in one SQL cmd line writing into TDengine. There is a limitation that TDengine could only accept SQL line small than 64000 bytes, so usually the batch size should not exceed 800. Default is 10.

--http-workers
set the number of workers who process the HTTP request. default is 10

--sql-workers
set the number of workers who process the database request. default is 10 

--dbname
set the database name in TDengine, if not exists, a database will be created after this dbname. default is "openfalcon".

--dbuser
set the user name that have the right to access the TDengine. default is "root"

--dbpassword
set the password of dbuser. default is "taosdata"

--port
set the port that open-falcon configuration. as showed below, in the transfer/config/cfg.json

```

## Configure the Open-Falcon

To write into blm_openfalcon API, you should configure the Open-Falcon as below
In the Open-Falcon transfer module's configuration file:

```json
    "tdengineblm": {
        "enabled": true,
        "maxConns": 32,
        "username": "root",
        "password": "taosdata",
        "precision": "ms",
        "db": "foo",
        "address": "http://127.0.0.1",
        "port": "10203"
    }
```
In the Agent part, the hostname should be unique among all the Open-Falcon which report to the TDengine.

## Check the TDengine tables and datas

Use the taos client shell to query the result.
```
Welcome to the TDengine shell from linux, client version:1.6.4.0 server version:2.0.20.0
Copyright (c) 2017 by TAOS Data, Inc. All rights reserved.

This is the trial version and will expire at 2019-12-11 14:25:31.

taos> use openfalcon;
Database changed.

taos> show stables;

// TODO add more detail examples here.

```

## Support Kubernates liveness probe
The blm_openfalcon support the liveness probe.

When the service is running, GET the url`http://ip:port/health` will return 200 OK response which means the service is running healthy. If no response, means the service is dead and need to restart it.


## Limitations

The TDengine limits the length of super table name, so if the name of Open-Falcon measurement name exceeds 60 byte, it will be truncated to first 60 bytes. And the length of tags name is limited within 50 byte.

[TDengine]:https://www.github.com/Taosdata/TDengine
