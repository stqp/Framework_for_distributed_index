#! /usr/bin/env bash

if [ ${HOSTNAME} != "salvia" ]
then
    echo "ERROR: not salvia"
    exit 1
fi

OPT_L="${HOME}/a.txt"
OPT_N=0
while getopts "l:n:" opt
do
    case ${opt} in
	l) OPT_L=${OPTARG}
	    ;;
	n) OPT_N=${OPTARG}
	    ;;
	\?)
	    echo "ERROR: invalid args"
	    exit 1
	    ;;
    esac
done
shift $(($OPTIND - 1))

if [ ! -f ${OPT_L} ]
then
    echo "ERROR: -l"
    exit 1
fi

if [ ${OPT_N} == "all" ]
then
    OPT_N=$(wc -l ${OPT_L} | awk "{print \$1}")
fi

if [ $((${OPT_N} <= 0)) != 0 ]
then
    echo "ERROR: -n"
    exit 1
fi

CNT=0
for x in $(cat ${OPT_L})
do
    CMD=${@//NUMBER/${CNT}}
    echo "====" ${CNT} adisk${x} ${CMD} "===="
    ~/ssh-adisk.sh ${x} ${CMD}
    CNT=$((CNT + 1))
    if [ $((${CNT} < ${OPT_N})) == 0 ]
    then
	break
    fi
done
