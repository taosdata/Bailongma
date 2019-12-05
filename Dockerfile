## Builder image
FROM tdengine/tdengine:latest as builder1
FROM golang:latest 



WORKDIR /root

COPY --from=builder1 /root/taos.h /usr/include/
COPY --from=builder1 /root/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so

RUN git config --global http.sslVerify false
RUN git config --global http.postbuffer 524288000


RUN go get -v -u -insecure github.com/taosdata/TDengine/src/connector/go/src/taosSql
RUN go get -v -u -insecure github.com/gogo/protobuf/proto
RUN go get -v -u -insecure github.com/golang/snappy
RUN go get -v -u -insecure github.com/prometheus/common/model
RUN go get -v -u -insecure github.com/prometheus/prometheus/prompb
RUN go get github.com/taosdata/TDengine/src/connector/go/src/taosSql





