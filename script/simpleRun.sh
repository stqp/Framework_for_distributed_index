#! /usr/bin/env bash
#���̃t�F�[�Y�ł͑҂��čs�����ق����ǂ��ł��B
source `dirname $0`/config.sh

${SCRIPT_DIR}/compile.sh
sleep 3
${SCRIPT_DIR}/deploy.sh
sleep 3
#number of computers   you want to run.
${SCRIPT_DIR}/run.sh 4 #> scriptLog.txt 2>&1