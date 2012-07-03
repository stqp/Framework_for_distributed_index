#! /usr/bin/env bash

if [ ${HOSTNAME} != "salvia" ]
then
    echo "ERROR: not salvia"
    exit 1
fi


OPT_F=false
while getopts "f" opt
do
    case ${opt} in
	f) OPT_F=true
	    ;;
	\?)
	    echo "ERROR: invalid args"
	    exit 1
	    ;;
    esac
done


MAIN_DIR=${HOME}/impl5
SRC_DIR=${MAIN_DIR}/src
SCRIPT_DIR=${MAIN_DIR}/script

IMPL_NAME=impl
MAKE_CMD="javac Main.java FatBtree.java SkipGraph.java PRing.java AlphanumericID.java"

ADISK_HOME=/exp/home/naoki
ADISK_MAIN_DIR=${ADISK_HOME}
ADISK_SRC_DIR=${ADISK_MAIN_DIR}/${IMPL_NAME}
ADISK_SCRIPT_DIR=${ADISK_MAIN_DIR}/script


if [ -d ${ADISK_SRC_DIR} ]
then
    rm -rf ${ADISK_SRC_DIR}
fi
cp -rf ${SRC_DIR} ${ADISK_SRC_DIR}
( cd ${ADISK_SRC_DIR} ; find . -name "*.class" -exec rm {} \; ; ${MAKE_CMD} )


if [ ${OPT_F} == "true" ]
then
    PGSQL_CAR=postgresql-8.2.21
    PGSQL_CDR=.tar.bz2
    PGSQL_NAME=${PGSQL_CAR}${PGSQL_CDR}
    PGSQL_ARCHIVE=${MAIN_DIR}/${PGSQL_NAME}
    ADISK_PGSQL_DIR=${ADISK_MAIN_DIR}/postgresql
    if [ -d ${ADISK_PGSQL_DIR} ]
    then
	rm -rf ${ADISK_PGSQL_DIR}
    fi
    cp -f ${PGSQL_ARCHIVE} ${ADISK_MAIN_DIR}
    ( cd ${ADISK_MAIN_DIR} ; tar jxf ${PGSQL_NAME} ; mv ${PGSQL_CAR} postgresql ; rm ${PGSQL_NAME} )
    ( cd ${ADISK_PGSQL_DIR} ; ./configure --prefix=/exp/naoki/pgsql ; make ) 1>/dev/null 2>&1
fi


if [ -d ${ADISK_SCRIPT_DIR} ]
then
    rm -rf ${ADISK_SCRIPT_DIR}
fi
cp -rf ${SCRIPT_DIR} ${ADISK_SCRIPT_DIR}

DATA_NAME=data_text_text.tar.bz2
ADISK_DATA_DIR=${ADISK_MAIN_DIR}/data
if [ -d ${ADISK_DATA_DIR} ]
then
    rm -rf ${ADISK_DATA_DIR}
fi
cp -f ${MAIN_DIR}/${DATA_NAME} ${ADISK_MAIN_DIR}
( cd ${ADISK_MAIN_DIR} ; tar jxf ${DATA_NAME} ; rm ${DATA_NAME} )
