## Builder image
FROM bailongma:latest as builder
FROM tdengine/tdengine:latest 


WORKDIR /root/

# COPY --from=builder /root/build/build/lib/libtaos.so /usr/lib/libtaos.so.1
# RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so

COPY --from=builder /root/blm_telegraf /root/

#COPY community/packaging/cfg/taos.cfg /etc/taos/

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8
EXPOSE 10202
EXPOSE 6030 6031 6032 6033 6034 6035 6036 6037 6038 6039 6040 6041

VOLUME [ "/var/lib/taos", "/var/log/taos","/etc/taos" ]

ENTRYPOINT  [ "/root/blm_telegraf" ]