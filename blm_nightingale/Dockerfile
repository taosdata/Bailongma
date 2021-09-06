ARG tdengine_ver=2.1.5.0
# 如果是beta版本写 -beta 其他留空
ARG extension=-beta
FROM golang:latest as builder
ARG tdengine_ver
ARG extension

ENV GO111MODULE=on \
    GOPROXY=http://goproxy.cn,direct
WORKDIR /root
COPY . /root/project
RUN mkdir -p /root/tmp && \
    wget https://www.taosdata.com/assets-download/TDengine-client-${tdengine_ver}${extension}-Linux-x64.tar.gz -O client.tar.gz && \
    tar -zxf client.tar.gz -C /root/tmp && \
    cd tmp/TDengine-client-${tdengine_ver} && \
    /bin/bash install_client.sh && \
    cd /root/project && \
    ls -al && \
    go build -o blm_n9e && \
    chmod +x blm_n9e

FROM ubuntu:18.04
WORKDIR /root
ARG tdengine_ver
COPY --from=builder /root/project/blm_n9e /root/
COPY --from=builder /usr/local/taos/driver/libtaos.so.${tdengine_ver} /lib/libtaos.so.1
WORKDIR /root
VOLUME [ "/root/config" ]
CMD [ "./blm_n9e" ]