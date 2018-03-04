#!/bin/sh

export PS4='+ [`basename ${BASH_SOURCE[0]}`:$LINENO ${FUNCNAME[0]} \D{%F %T} $$ ] '

# install redis use brew
# brew install redis@3.2

# export redis-server cmd path
export PATH=${PATH}:/usr/local/opt/redis@3.2/bin/

MYNAME="${0##*/}"
CURDIR=$(cd "$(dirname "$0")"; pwd)
ACTION=""

## redis instance data store path
DBCACHEPATH="/usr/local/var/db/redis"

usage() {
    cat << USAGE
Usage: bash ${MYNAME} [-h]
                 action {startall|stopall}

Redis start/stop control scripts.

Optional arguments:
    -h, --help            show this help message and exit

Require:
    action  {startall|stopall}

USAGE

    exit 1
}

#
# Parses command-line options.
#  usage: parse_options "$@" || exit $?
#
parse_options() {
    declare -a argv

    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
            	usage
            	exit
            	;;
            --)
            	shift
                argv=("${argv[@]}" "${@}")
            	break
            	;;
            -*)
            	echo "command line: unrecognized option $1" >&2
            	return 1
            	;;
            *)
                argv=("${argv[@]}" "${1}")
                shift
                ;;
        esac
    done

    case ${#argv[@]} in
        1)
            ACTION="${argv[0]}"
            ;;
        0|*)
            usage 1>&2
            return 1
	        ;;
    esac
}

startall() {
    for i in `ls $CURDIR/config | grep conf`; do
        cfgFile="${CURDIR}/config/$i"
        redis-server $cfgFile &
    done
}

stopall() {
    for i in `ps aux | grep redis-server | grep -v grep | awk '{print $2}'`; do
        echo "Starting to stop redis: $i"
        kill $i;
    done
}

remove() {
    echo "Starting Stop all instance."
    stopall

    sleep 20
    echo "Starting remove redis history data."
    if [ x$DBCACHEPATH != "x" ]; then
        for f in `ls $DBCACHEPATH/*.rdb`; do
            rm -rf "$f"
        done
    fi
}

################################## main route #################################
parse_options "${@}" || usage

case "${ACTION}" in
    startall)
        #rh_status_q && exit 0
        ${ACTION}
        ;;
    stopall)
        #rh_status_q || exit 0
        ${ACTION}
        #pstatus && forcestop -9
        ;;
    remove)
        ${ACTION}
        ;;
    *)
        echo "Usage: $0 {startall|stopall}"
        exit 2
esac
exit $?

#./redis-trib create --replicas 1 127.0.0.1:6380 127.0.0.1:6379 127.0.0.1:6378 127.0.0.1:7380 127.0.0.1:7379 127.0.0.1:7378
