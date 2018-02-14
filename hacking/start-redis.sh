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
