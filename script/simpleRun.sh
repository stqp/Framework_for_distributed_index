#! /usr/bin/env bash
#���̃t�F�[�Y�ł͑҂��čs�����ق����ǂ��ł��B
source `dirname $0`/config.sh

${SCRIPT_DIR}/compile.sh
sleep 10
${SCRIPT_DIR}/deploy.sh
sleep 10
${SCRIPT_DIR}/run.sh 2