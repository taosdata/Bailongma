## Builder image
FROM tdengine:latest as builder1
FROM golang:latest as builder


COPY server.go /root/blm_telegraf/

WORKDIR /root/blm_telegraf

# build enterprise version

COPY --from=builder1 /root/taos.h /usr/include/
COPY --from=builder1 /root/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
RUN go get github.com/taosdata/TDengine/src/connector/go/src/taosSql

RUN go build
# # build community version
# RUN cmake .. -DVERSION=lite && cmake --build .

## Target image


FROM centos:7

WORKDIR /root

# COPY --from=builder /root/build/build/lib/libtaos.so /usr/lib/libtaos.so.1
# RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
COPY --from=builder1 /root/taos.h /usr/include/
COPY --from=builder1 /root/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so


COPY --from=builder /root/blm_telegraf/blm_telegraf .

#COPY community/packaging/cfg/taos.cfg /etc/taos/

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8

VOLUME [ "/var/lib/taos", "/var/log/taos" ]

CMD [ "/root/blm_telegraf" ]