#! /usr/bin/env bash

# load settings
source `dirname $0$`/config.sh

# copy jar files to each node
for i in `cat ${NODE_LIST}`; do
	echo "ssh ${NODE_PREFIX}${i} mkdir -p ${DEPLOY_DIR}"
	ssh ${NODE_PREFIX}${i} mkdir -p ${DEPLOY_DIR}

	echo "scp ${LIB_DIR}/*.jar  ${NODE_PREFIX}${i}:${DEPLOY_DIR}/"
	scp ${LIB_DIR}/*.jar  ${NODE_PREFIX}${i}:${DEPLOY_DIR}/
done



# copy script files to each node
for i in `cat ${NODE_LIST}`; do
	echo ssh ${NODE_PREFIX}${i} mkdir -p ${SCRIPT_DIR}
	ssh ${NODE_PREFIX}${i} mkdir -p ${SCRIPT_DIR}

	echo scp ${SCRIPT_DIR}/*.sh ${NODE_PREFIX}${i}:${SCRIPT_DIR}/
	scp ${SCRIPT_DIR}/*.sh ${NODE_PREFIX}${i}:${SCRIPT_DIR}/

	echo scp ${MAIN_DIR}/nodelist.txt ${NODE_PREFIX}${i}:${DEPLOY_DIR}/
	scp ${MAIN_DIR}/nodelist.txt ${NODE_PREFIX}${i}:${DEPLOY_DIR}/
done


# create a table if it does not exist.
for i in `cat ${NODE_LIST}`; do
	echo "ssh ${NODE_PREFIX}${i}  ${DEPLOY_PSQL} ${DEPLOY_TABLE}"
	ssh ${NODE_PREFIX}${i}  ${DEPLOY_PSQL} "\"${DEPLOY_TABLE}\""
done

