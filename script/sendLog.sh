# this shell not working correctly

#! /usr/bin/env bash
# this shell is run at each remote computer.

# load setting
source `dirname $0`/config.sh

echo scp -r  ${LOG_DIR}/* tokuda@dahlianame:${IMPL_NAME}/${LOGS}/${NODE_PREFIX}${i}
scp -r  ${LOG_DIR}/* tokuda@dahlianame:${IMPL_NAME}/${LOGS}/${NODE_PREFIX}${i}






# TODO delete! for debug
#exit
#echo  scp -r ${LOG_DIR}/* ${USER}@${TARGET_NODE_TO_SEND_LOG}:${MAIN_DIR}/${IMPLE_NAME}/${NODE_PREFIX}${i}
# scp -r ${LOG_DIR}/* ${USER}@${TARGET_NODE_TO_SEND_LOG}:${MAIN_DIR}/${IMPLE_NAME}/${NODE_PREFIX}${i}

