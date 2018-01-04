#!/bin/bash

[ "$FE_DIR" = "" ] && FE_DIR=${HOME}/src/fe
LOGROT=logrot

[ "$BIG" = "" ] && BIG=0
[ "$BIG" = "0" ] && CONFIG=${FE_DIR}/misc/fe-config.json || CONFIG=${FE_DIR}/misc/fe-config-100.json
[ "$BIG" = "0" ] && MAX=5 || MAX=100

function start ()
{
    if [ ! -f ${CONFIG} ]; then
	echo "Cannot find ${CONFIG}"
	return
    fi

    if [ ! -d ${FE_DIR}/build/logs ]; then
	mkdir -p ${FE_DIR}/build/logs
    fi

    ${FE_DIR}/build/bin/fe run ${CONFIG} $1 2>&1 | ${LOGROT} ${FE_DIR}/build/logs/$1.log 10M 5 &
}

function start_all ()
{
    for ((i=1; i <= $MAX; i++)); do
	start id-$i
    done
}

function usage ()
{
    echo "Usage: `basename $0` [start [<id>] | stop [<id>] | resatrt [<id>]]"
}

case "$1" in
"stop")
    if [ ! "$2" = "" ]; then
        ID=$2
    else
        ID=
    fi
    for i in {1..10}; do
        ps axww | grep -v grep | grep -q "fe.*fe-config.*json.*${ID}"
        if [ ! $? = 0 ]; then
            break
        else
            ps axww | grep -v grep | grep "fe.*fe-config.*json.*${ID}" | awk '{print $1}' | xargs -L1 sudo kill
            sleep 1;
        fi
    done
    ps axww | grep -v grep | grep -q "fe.*fe-config.*json.*${ID}"
    if [ $? = 0 ]; then
        ps axww | grep -v grep | grep "fe.*fe-config.*json.*${ID}" | awk '{print $1}' | xargs -L1 sudo kill -9
    fi
    ;;

"start")
    if [ ! "$2" = "" ]; then
        start $2
    else
        start_all
    fi
    ;;

"restart")
    if [ ! "$2" = "" ]; then
	$0 stop $2
        start $2
    else
	$0 stop
        start_all
    fi
    ;;

*)
    usage;
    ;;
esac

# EOF
