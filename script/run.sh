#! /usr/bin/env bash

if [ ${HOSTNAME:0:5} != "adisk" ]
then
    echo "ERROR: not adisk"
    exit 1
fi

if [ $# == 0 ]
then
    echo "ERROR: no arg"
    exit 1
fi

METHOD_NAME=$1
shift

MAIN_DIR=${HOME}

IMPL_NAME=impl
SRC_DIR=${MAIN_DIR}/${IMPL_NAME}

EXP_DIR=/exp/naoki

PGSQL_DIR=${EXP_DIR}/pgsql
DATA_DIR=${MAIN_DIR}/data
if [ -d ${PGSQL_DIR}/data ] && [ -d ${DATA_DIR} ]
then
    rm -rf ${PGSQL_DIR}/data/*
    cp -rf ${DATA_DIR}/* ${PGSQL_DIR}/data
    ${PGSQL_DIR}/bin/pg_ctl -D ${PGSQL_DIR}/data -w start
else
    echo "ERROR: directories not found"
    exit 1
fi

if [ -d ${EXP_DIR}/log ]
then
    rm -rf ${EXP_DIR}/log
fi
mkdir ${EXP_DIR}/log

cd ${SRC_DIR}
CLASSPATH=${SRC_DIR}/postgresql-8.1-415.jdbc3.jar:${CLASSPATH} java Main ${METHOD_NAME} AlphanumericID 8081 ${HOSTNAME}${HOSTNAME}
cd -

${PGSQL_DIR}/bin/pg_ctl -D ${PGSQL_DIR}/data stop
sleep 2
ps
