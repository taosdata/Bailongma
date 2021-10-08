## Builder image
#FROM tdengine/tdengine:dev as builder1
ARG tdengine_ver

FROM golang:latest as builder
ARG tdengine_ver
#FROM tdengine/tdengine:2.0.0.0 as builder1


ENV GO111MODULE=on \
  GOPROXY=http://goproxy.cn,direct


WORKDIR /root

#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so /usr/lib/libtaos.so
#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so.1 /usr/lib/
COPY TDengine-server-$tdengine_ver-Linux-x64.tar.gz /root/
RUN tar -zxf TDengine-server-$tdengine_ver-Linux-x64.tar.gz
WORKDIR /root/TDengine-server-$tdengine_ver/
RUN /root/TDengine-server-$tdengine_ver/install.sh -e no
COPY blm_telegraf /root/blm_telegraf/
COPY blm_prometheus /root/blm_prometheus/

#RUN mkdir /usr/lib/ld

WORKDIR /root/blm_telegraf/
RUN go build 

WORKDIR /root/blm_prometheus/
RUN go build
#RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so

FROM tdengine/tdengine:$tdengine_ver


WORKDIR /root

COPY --from=builder /root/blm_telegraf/blm_telegraf /root/
COPY --from=builder /root/blm_prometheus/blm_prometheus /root/
