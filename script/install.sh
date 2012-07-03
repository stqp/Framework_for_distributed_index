#! /usr/bin/env bash

if [ ${HOSTNAME:0:5} != "adisk" ]
then
    echo "ERROR: not adisk"
    exit 1
fi


MAIN_DIR=${HOME}

IMPL_NAME=impl
SRC_DIR=${MAIN_DIR}/${IMPL_NAME}

PGSQL_DIR=${MAIN_DIR}/postgresql

EXP_DIR=/exp/naoki
EXP_PGSQL_DIR=${EXP_DIR}/pgsql


if [ -d ${EXP_DIR}/postgresql ]
then
    rm -rf ${EXP_DIR}/postgresql
fi

if [ -d ${EXP_PGSQL_DIR} ]
then
    rm -rf ${EXP_PGSQL_DIR}
fi
( cd ${PGSQL_DIR} ; make install ) 1>/dev/null 2>&1

EXP_DATA_DIR=${EXP_PGSQL_DIR}/data
mkdir ${EXP_DATA_DIR}
chmod 700 ${EXP_DATA_DIR}


EXP_HOME_DIR=/exp/home/naoki
JAVA_NAME=jdk1.6.0_27
JAVA_DIR=${MAIN_DIR}/${JAVA_NAME}

if [ -d ${EXP_HOME_DIR}/${JAVA_NAME} ]
then
    rm -rf ${EXP_HOME_DIR}/${JAVA_NAME}
fi
cp -rf ${JAVA_DIR} ${EXP_HOME_DIR}
