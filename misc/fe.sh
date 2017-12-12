#!/bin/bash

DIR=${HOME}/src/fe
LOGROT=logrot

function start ()
{
    if [ ! -f ${DIR}/fe-config.json ]; then
	echo "Cannot find fe-config.json"
	return
    fi

    if [ ! -d ${DIR}/build/logs ]; then
	mkdir -p ${DIR}/build/logs
    fi

    ${DIR}/build/bin/fe run ${DIR}/fe-config.json $1 | ${LOGROT} ${DIR}/build/logs/$1.log 10M 5 &
}

function start_all ()
{
    LIST="id-1 id-2 id-3 id-4 id-5"

    for i in ${LIST}; do
	start $i
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
        ps axww | grep -v grep | grep -q "fe.*fe-config.json.*${ID}"
        if [ ! $? = 0 ]; then
            break
        else
            ps axww | grep -v grep | grep "fe.*fe-config.json.*${ID}" | awk '{print $1}' | xargs -L1 sudo kill
            sleep 3;
        fi
    done
    ps axww | grep -v grep | grep -q "fe.*fe-config.json.*${ID}"
    if [ $? = 0 ]; then
        ps axww | grep -v grep | grep "fe.*fe-config.json.*${ID}" | awk '{print $1}' | xargs -L1 sudo kill -9
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
