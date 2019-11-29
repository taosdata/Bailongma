## Builder image
FROM tdengine/tdengine:latest as builder1
FROM golang:latest as builder


COPY server.go /root/blm_prometheus/

WORKDIR /root/blm_prometheus

# build enterprise version

COPY --from=builder1 /root/taos.h /usr/include/
COPY --from=builder1 /root/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
RUN git config --global http.sslVerify false
RUN git config --global http.postbuffer 524288000
RUN go get github.com/taosdata/TDengine/src/connector/go/src/taosSql
RUN go get github.com/gogo/protobuf/proto
RUN go get github.com/golang/snappy
RUN go get github.com/prometheus/common/model
RUN go get github.com/prometheus/prometheus/prompb

RUN go build
# # build community version
# RUN cmake .. -DVERSION=lite && cmake --build .

## Target image


FROM centos:7

WORKDIR /root/

# COPY --from=builder /root/build/build/lib/libtaos.so /usr/lib/libtaos.so.1
# RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
COPY --from=builder1 /root/taos.h /usr/include/
COPY --from=builder1 /root/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so


COPY --from=builder /root/blm_prometheus/blm_prometheus .

#COPY community/packaging/cfg/taos.cfg /etc/taos/

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8

VOLUME [ "/var/lib/taos", "/var/log/taos" ]

CMD [ "/root/blm_prometheus" ]