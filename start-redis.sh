#!/bin/sh

export PATH=${PATH}:/usr/local/opt/redis@3.2/bin/
CURDIR=$(cd "$(dirname "$0")"; pwd);

redis-server ${CURDIR}/config/redis-6378.conf &
redis-server ${CURDIR}/config/redis-6379.conf &
redis-server ${CURDIR}/config/redis-6380.conf &
