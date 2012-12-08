#! /usr/bin/env bash

# load setting
source `dirname $0`/config.sh


mkdir -p ${IMPL_NAME}/${LOGS}

  echo ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_NAME} AlphanumericID ${DEPLOY_PORT}  ${NODE_PREFIX}${i} > ${LOG_DIR}/log_`date +%m_%d_%Y`.txt 2>&1
 ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_PACKAGE_NAME}.${METHOD_NAME} util.AlphanumericID ${DEPLOY_PORT}  ${NODE_PREFIX}${i} > ${LOG_DIR}/log_`date +%m_%d_%Y`.txt 2>&1

#${METHOD_PACKAGE_NAME}