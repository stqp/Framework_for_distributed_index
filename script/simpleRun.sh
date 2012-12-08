#! /usr/bin/env bash
#このフェーズでは待って行ったほうが良いです。
source `dirname $0`/config.sh

${SCRIPT_DIR}/compile.sh
sleep 10
${SCRIPT_DIR}/deploy.sh
sleep 10
${SCRIPT_DIR}/run.sh 2