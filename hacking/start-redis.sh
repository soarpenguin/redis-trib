#!/bin/sh

# install redis use brew
# brew install redis@3.2

# export redis-server cmd path
export PATH=${PATH}:/usr/local/opt/redis@3.2/bin/
CURDIR=$(cd "$(dirname "$0")"; pwd);

redis-server ${CURDIR}/config/redis-6378.conf &
redis-server ${CURDIR}/config/redis-6379.conf &
redis-server ${CURDIR}/config/redis-6380.conf &

redis-server ${CURDIR}/config/redis-7378.conf &
redis-server ${CURDIR}/config/redis-7379.conf &
redis-server ${CURDIR}/config/redis-7380.conf &

#./redis-trib create --replicas 1 127.0.0.1:6380 127.0.0.1:6379 127.0.0.1:6378 127.0.0.1:7380 127.0.0.1:7379 127.0.0.1:7378
# for i in `ps aux | grep redis | grep -v grep | awk '{print $2}'`; do echo $i; kill $i; done
