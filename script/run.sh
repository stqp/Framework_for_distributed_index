#! /usr/bin/env bash


echo "===== YOU_NEED_SCREEN ====="
if [ ${TERM} != "screen" ]
then
    echo "Please use this command in 'screen' environment"
    exit 1
fi


echo "===== ARGUMENT_VALIDATION ====="
#if [ $# != 2 ]
#then
#    echo "Usage: ${0} method_name  number_of_nodes"
#    exit 1
#fi
# i changed usage of this shell script.

if [ $# != 1 ]
then
    echo "Usage: ${0} number_of_nodes"
    exit 1
fi

#method name is defined at config.sh.
#METHOD_NAME=$1
#NUM_NODES=$2

NUM_NODES=$1
if [ $((${NUM_NODES} <= 0)) != 0 ]
then
    echo "The number of nodes must be greater than 0"
    exit 1
fi



echo "===== LOAD_SETTING_FILE ====="
# load settings
source `dirname $0`/config.sh




NODES=""
NODE_CNT=0
for node in `cat ${NODE_LIST}`
do
    NODES="${NODES}${NODES:+ }${node}"
    NODE_CNT=$((${NODE_CNT} + 1))
    if [ $((${NODE_CNT} < ${NUM_NODES})) == 0 ]
    then
	break
    fi
done


#echo "========  METHOD_PACKAGE SETTING HERE ========"
#echo METHOD_PACKAGE_NAME=distributedIndex
#METHOD_PACKAGE_NAME=distributedIndex





echo "======== EXPERIMENT_START========"
echo ${METHOD_NAME} ${NUM_NODES}




echo "======== START ========"
LANG=C date
for i in ${NODES}
do
    echo "== ${NODE_PREFIX}${i} =="
#echo screen -X screen ssh ${NODE_PREFIX}${i}  ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_PACKAGE_NAME}${METHOD_NAME} util.AlphanumericID $DEPLOY_PORT  ${NODE_PREFIX}${i} 
#screen -X screen ssh ${NODE_PREFIX}${i}  ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_PACKAGE_NAME}${METHOD_NAME} util.AlphanumericID $DEPLOY_PORT  ${NODE_PREFIX}${i} >> log${i}.txt 2>&1
#echo ssh ${NODE_PREFIX}${i} ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_PACKAGE_NAME}${METHOD_NAME} util.AlphanumericID ${DEPLOY_PORT}  ${NODE_PREFIX}${i}  '> log'${i} 2>&1    
#ssh ${NODE_PREFIX}${i} ${DEPLOY_JAVA} -cp ${DEPLOY_CLASSPATH} ${MAIN_CLASS} ${METHOD_PACKAGE_NAME}${METHOD_NAME} util.AlphanumericID ${DEPLOY_PORT}  ${NODE_PREFIX}${i}  '> log'${i} 2>&1
	echo ssh ${NODE_PREFIX}${i} ${SCRIPT_DIR}/runJavaWithLog.sh 
	screen -X screen ssh ${NODE_PREFIX}${i} ${SCRIPT_DIR}/runJavaWithLog.sh

      sleep 1
done
sleep 10



###################################
# range partitioning for testset2

# 1
id[100]=user1000053778378872380

# 2
id[200]=user1000053778378872380
id[201]=user5142835197696697420

# 4
id[400]=user1000053778378872380
id[401]=user3073737695004966866
id[402]=user5142835197696697420
id[403]=user7219633036887726303

# 8
id[800]=user1000053778378872380
id[801]=user203430598756149354
id[802]=user3073737695004966866
id[803]=user4112219055717379615
id[804]=user5142835197696697420
id[805]=user6180478631055660009
id[806]=user7219633036887726303
id[807]=user8258314668002023487

# 16
id[1600]=user1000053778378872380
id[1601]=user1516853963787215215
id[1602]=user203430598756149354
id[1603]=user2556621763115467225
id[1604]=user3073737695004966866
id[1605]=user3589717997875816989
id[1606]=user4112219055717379615
id[1607]=user4628961980974013895
id[1608]=user5142835197696697420
id[1609]=user5663370553163344679
id[1610]=user6180478631055660009
id[1611]=user6697084008587543446
id[1612]=user7219633036887726303
id[1613]=user7741766550621848605
id[1614]=user8258314668002023487
id[1615]=user8782122600747142096

# 32
id[3200]=user1000053778378872380
id[3201]=user1253919243834003090
id[3202]=user1516853963787215215
id[3203]=user1776615109552025114
id[3204]=user203430598756149354
id[3205]=user2294948339078950126
id[3206]=user2556621763115467225
id[3207]=user2810715789257383403
id[3208]=user3073737695004966866
id[3209]=user3334189930705387758
id[3210]=user3589717997875816989
id[3211]=user3850259810842368493
id[3212]=user4112219055717379615
id[3213]=user4366807928222543375
id[3214]=user4628961980974013895
id[3215]=user4889974177186575235
id[3216]=user5142835197696697420
id[3217]=user5405410227539653532
id[3218]=user5663370553163344679
id[3219]=user5921398384407687966
id[3220]=user6180478631055660009
id[3221]=user64414080959597372
id[3222]=user6697084008587543446
id[3223]=user6962004111089390136
id[3224]=user7219633036887726303
id[3225]=user7478973205561050610
id[3226]=user7741766550621848605
id[3227]=user800188244881830174
id[3228]=user8258314668002023487
id[3229]=user8521531381626066348
id[3230]=user8782122600747142096
id[3231]=user9037658521914755638




echo "======== INIT ========"
LANG=C date
INIT_MACHINE=""
N=$((${NUM_NODES} * 100))
for node in ${NODES}
do
    if [ -z ${INIT_MACHINE} ]
    then
	INIT_MACHINE=${node}
	echo "== init ${NODE_PREFIX}${node} ${id[${N}]} =="
	(echo "init ${id[${N}]}" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    else
	echo "== init ${NODE_PREFIX}${node} ${id[${N}]} TO ${NODE_PREFIX}${INIT_MACHINE} =="
	(echo "init ${id[${N}]} ${NODE_PREFIX}${INIT_MACHINE} ${DEPLOY_PORT}" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    fi
    N=$((${N} + 1))
done

sleep 10




echo "======== STATUS ========"
LANG=C date
for node in ${NODES}
do
    echo "== status ${NODE_PREFIX}${node} =="
    (echo -e "interactive\r\nstatus\r\nquit" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
done

# for debug
# exit 0




echo "======== PUT ========"
LANG=C date
N=0
for node in ${NODES}
do
    echo ssh ${NODE_PREFIX}${node} mkdir -p ${DEPLOY_DATA_DIR}
    ssh ${NODE_PREFIX}${node} mkdir -p ${DEPLOY_DATA_DIR}

    echo scp ${PUT_DATA_DIR}/${NUM_NODES}/put${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/
    scp ${PUT_DATA_DIR}/${NUM_NODES}/put${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/

    echo "== source ${DEPLOY_DATA_DIR}/put${N}.dat TO ${NODE_PREFIX}${node} =="
    (echo "source ${DEPLOY_DATA_DIR}/put${N}.dat" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT} &
    N=$((N + 1))
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 250

    for node in ${NODES}
    do
    # echo "== source _status_ TO ${NODE_PREFIX}${node} =="
    (echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done > status.dump

    echo "========"
    LANG=C date
    grep "SOURCE: isSourceRun:" status.dump

    if [ $(grep "SOURCE: isSourceRun: false" status.dump | wc -l) == ${NUM_NODES} ]
    then
    FLAG_ALL_DONE=1
    break
    fi
done
if [ ${FLAG_ALL_DONE} == 0 ]
then
    for node in ${NODES}
    do
    echo "== source _abort_ TO ${NODE_PREFIX}${node} =="
    (echo "source _abort_" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done
fi












# status${METHOD_NAME}.dump 




echo "======== GET ========"
LANG=C date
N=0
for node in ${NODES}
do
    echo scp ${GET_DATA_DIR}/32/get${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/
    scp ${GET_DATA_DIR}/32/get${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/
    echo "== source ${DEPLOY_DATA_DIR}/get${N}.dat TO ${NODE_PREFIX}${node} =="
    (echo "source ${DEPLOY_DATA_DIR}/get${N}.dat" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT} &
    N=$((N + 1))
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 250

    for node in ${NODES}
    do
    # echo "== source _status_ TO ${NODE_PREFIX}${node} =="
    (echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done > status.dump

    echo "========"
    LANG=C date
    grep "SOURCE: isSourceRun:" status.dump

    if [ $(grep "SOURCE: isSourceRun: false" status.dump | wc -l) == ${NUM_NODES} ]
    then
    FLAG_ALL_DONE=1
    break
    fi
done
if [ ${FLAG_ALL_DONE} == 0 ]
then
    for node in ${NODES}
    do
    echo "== source _abort_ TO ${NODE_PREFIX}${node} =="
    (echo "source _abort_" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done
fi




echo "======== COLLECT_LOG_FILE_START ========"
LANG=C date
for i in ${NODES}
do
    echo  mkdir -p ${LOG_DIR}/${NODE_PREFIX}${i}
   mkdir -p ${LOG_DIR}/${NODE_PREFIX}${i}	
done

sleep 1

for node in ${NODES}
do
	echo scp ${USER}@${NODE_PREFIX}${node}:${LOG_DIR}/*.txt ${LOG_DIR}/${NODE_PREFIX}${node}
	scp ${USER}@${NODE_PREFIX}${node}:${LOG_DIR}/*.txt ${LOG_DIR}/${NODE_PREFIX}${node}
	sleep 1
done

sleep 1
echo "======== COLLECT _LOG_FILE_END ========"





echo "======== STOP JAVA START ========"
${SCRIPT_DIR}/stop.sh
sleep 1
exit
echo "======== STOP JAVA END ========"









echo "======== DEBUG FOR CHECK DATALOAD  ADN  DATA_NODE MOVEMENT START ========"
echo "before movement" 
for node in ${NODES}
do
	echo "== check dataLoad =="
	(echo "check dataLoad" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
done
sleep 5


echo "data_node movement"
echo "== FORCE_MOVE_DATA=="
(echo "FORCE_MOVE_DATA" ; sleep 10) | telnet ${NODE_PREFIX}1 ${DEPLOY_PORT}
sleep 5


echo "after movement"
for node in ${NODES}
do
	echo "== check dataLoad =="
	(echo "check dataLoad" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
done
sleep 5


${SCRIPT_DIR}/stop.sh
sleep 1
echo "======== END_DEBUG FOR CHECK DATALOAD  ADN  DATA_NODE MOVEMENT========"













echo "======== RANGE ========"
LANG=C date
N=0
for node in ${NODES}
do
    echo scp ${RANGE_DATA_DIR}/32/range${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/
    scp ${RANGE_DATA_DIR}/32/range${N}.dat ${NODE_PREFIX}${node}:${DEPLOY_DATA_DIR}/
    echo "== source ${DEPLOY_DATA_DIR}/range${N}.dat TO ${NODE_PREFIX}${node} =="
    (echo "source ${DEPLOY_DATA_DIR}/range${N}.dat" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT} &
    N=$((N + 1))
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 300

    for node in ${NODES}
    do
	# echo "== source _status_ TO ${NODE_PREFIX}${node} =="
	(echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done >status${METHOD_NAME}.dump 

    echo "========"
    LANG=C date
    grep "SOURCE: isSourceRun:"status${METHOD_NAME}.dump 

    if [ $(grep "SOURCE: isSourceRun: false" status${METHOD_NAME}.dump | wc -l) == ${NUM_NODES} ]
    then
	FLAG_ALL_DONE=1
	break
    fi
done
if [ ${FLAG_ALL_DONE} == 0 ]
then
    for node in ${NODES}
    do
	echo "== source _abort_ TO ${NODE_PREFIX}${node} =="
	(echo "source _abort_" ; sleep 10) | telnet ${NODE_PREFIX}${node} ${DEPLOY_PORT}
    done
fi

echo "======== END ========"





















# for debug
exit




echo  "======== END process. return logfiles ========"

#echo mkdir -p logs
#mkdir -p ${LOG_DIR}
for i in ${NODES}
do
    echo  mkdir -p ${LOG_DIR}/${NODE_PREFIX}${i}
   mkdir -p ${LOG_DIR}/${NODE_PREFIX}${i}	
done

for i in ${NODES}
do
    echo "== ${NODE_PREFIX}${i} =="
   	
	#to make log file, we should run shell script on the remote computer.
	echo screen -X screen ssh ${NODE_PREFIX}${i} ${SCRIPT_DIR}/sendLog.sh	
	screen -X screen ssh ${NODE_PREFIX}${i} ${SCRIPT_DIR}/sendLog.sh

	sleep 1
done
