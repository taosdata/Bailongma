## Builder image
#FROM tdengine/tdengine:dev as builder1
FROM golang:latest as builder
#FROM tdengine/tdengine:2.0.0.0 as builder1



WORKDIR /root

#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so /usr/lib/libtaos.so
#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so.1 /usr/lib/
COPY TDengine-server-2.0.0.0-Linux-x64.tar.gz /root/
RUN tar -zxf TDengine-server-2.0.0.0-Linux-x64.tar.gz
WORKDIR /root/TDengine-server/
RUN /root/TDengine-server/install.sh -e no
COPY blm_telegraf/server.go /root/blm_telegraf/
COPY blm_prometheus/server.go /root/blm_prometheus/

#RUN mkdir /usr/lib/ld

WORKDIR /root/
RUN go mod init bailongma.com/m/v2

WORKDIR /root/blm_telegraf/
RUN go build 

WORKDIR /root/blm_prometheus/
RUN go build
#RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
FROM tdengine/tdengine:2.0.0.0


WORKDIR /root

COPY --from=builder /root/blm_telegraf/blm_telegraf /root/
COPY --from=builder /root/blm_prometheus/blm_prometheus /root/

