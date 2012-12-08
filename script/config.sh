#! /usr/bin/env bash

##################################################
# Java Settings
##################################################

# If you have to set a path to java explicitly, comment out the following lines.
#
#JAVA_HOME=/usr/local
#PATH=${PATH}:${JAVA_HOME}/bin


##################################################
# ST-Framework Settings
##################################################
MAIN_DIR=`dirname $0`/..
SRC_DIR=${MAIN_DIR}/src
BIN_DIR=${MAIN_DIR}/bin
LIB_DIR=${MAIN_DIR}/lib
DOC_DIR=${MAIN_DIR}/doc
IMPL_NAME=st-framework

##################################################
# Jar Settings
##################################################
JAR_FILE=${IMPL_NAME}.jar
MAIN_CLASS="main.Main"


##################################################
# Deployment Settings
##################################################
NODE_PREFIX="edn"
NODE_LIST=${MAIN_DIR}/nodelist.txt

#for my test
#PUT_DATA_DIR=${MAIN_DIR}/testset/testset2/put
#GET_DATA_DIR=${MAIN_DIR}/testset/testset2/get
#RANGE_DATA_DIR=${MAIN_DIR}/testset/testset2/range

PUT_DATA_DIR=${MAIN_DIR}/testset/testset2/fakeput
GET_DATA_DIR=${MAIN_DIR}/testset/testset2/fakeget
RANGE_DATA_DIR=${MAIN_DIR}/testset/testset2/fakerange


DEPLOY_DIR=/home/${USER}/${IMPL_NAME}
DEPLOY_DATA_DIR=${DEPLOY_DIR}/testset

DEPLOY_JAVA="/usr/local/bin/java"
DEPLOY_CLASSPATH=${DEPLOY_DIR}/${JAR_FILE}:${DEPLOY_DIR}/postgresql-9.1-902.jdbc4.jar
DEPLOY_PORT=18085

DEPLOY_PSQL="psql -c "
DEPLOY_TABLE="CREATE TABLE data (key VARCHAR(30), value VARCHAR(35));"


##################################################
# MY Settings
##################################################
METHOD_NAME="FatBtree"
METHOD_PACKAGE_NAME="distributedIndex"
TARGET_NODE_TO_SEND_LOG="dahlianame"
LOGS=logs
LOG_DIR=${DEPLOY_DIR}/${LOGS}
SCRIPT_DIR=${DEPLOY_DIR}/script

