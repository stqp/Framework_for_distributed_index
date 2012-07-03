#! /usr/bin/env bash

if [ ${HOSTNAME} != "salvia" ]
then
    echo "ERROR: not salvia"
    exit 1
fi

if [ ${TERM} != "screen" ]
then
    echo "ERROR: not screen"
    exit 1
fi

if [ $# != 2 ]
then
    echo "ERROR: args"
    exit 1
fi

METHOD_NAME=$1
NUM_NODES=$2

if [ $((${NUM_NODES} <= 0)) != 0 ]
then
    echo "ERROR: number of nodes"
    exit 1
fi

NODE_FILE="${HOME}/node.txt"
NODE_LIST=""
NODE_CNT=0
for node in $(cat ${NODE_FILE})
do
    NODE_LIST="${NODE_LIST}${NODE_LIST:+ }${node}"
    NODE_CNT=$((${NODE_CNT} + 1))
    if [ $((${NODE_CNT} < ${NUM_NODES})) == 0 ]
    then
	break
    fi
done

echo "======== EXPERIMENT ========"
echo ${METHOD_NAME} ${NUM_NODES}

echo "======== START ========"
LANG=C date
for node in ${NODE_LIST}
do
    echo "== adisk${node} =="
    screen -X screen ~/ssh-adisk.sh ${node} ". .bash_profile && ~/script/run.sh ${METHOD_NAME}"
done

sleep 10


# ####
# # testset1

# # 1
# id[100]=user1000053778378872380

# # 2	   
# id[200]=user1000053778378872380
# id[201]=user5150756240885596906

# # 4	   
# id[400]=user1000053778378872380
# id[401]=user3077411578082041711
# id[402]=user5150756240885596906
# id[403]=user7224432303514507081

# # 8	   
# id[800]=user1000053778378872380
# id[801]=user203738491240121849
# id[802]=user3077411578082041711
# id[803]=user4112413863593839013
# id[804]=user5150756240885596906
# id[805]=user6185758526397394208
# id[806]=user7224432303514507081
# id[807]=user826349051565622304

# # 16	   
# id[1600]=user1000053778378872380
# id[1601]=user1518053216608683138
# id[1602]=user203738491240121849
# id[1603]=user2554497397146177804
# id[1604]=user3077411578082041711
# id[1605]=user3595637185526878663
# id[1606]=user4112413863593839013
# id[1607]=user4629293380799368875
# id[1608]=user5150756240885596906
# id[1609]=user5666857482361144437
# id[1610]=user6185758526397394208
# id[1611]=user6705985989380068972
# id[1612]=user7224432303514507081
# id[1613]=user7741766550621848605
# id[1614]=user826349051565622304
# id[1615]=user8782908730474308738

# # 32	   
# id[3200]=user1000053778378872380
# id[3201]=user126041372390069927
# id[3202]=user1518166301216196235
# id[3203]=user177507778583081204
# id[3204]=user2037949279766002922
# id[3205]=user2295061423686463223
# id[3206]=user2554944273050805155
# id[3207]=user281551086169446142
# id[3208]=user3077971538594182159
# id[3209]=user3335184949826457771
# id[3210]=user3595865746213664131
# id[3211]=user385381541063394234
# id[3212]=user4112973824105979461
# id[3213]=user4371859262877492106
# id[3214]=user4629853341311509323
# id[3215]=user489198679434732978
# id[3216]=user5151760685830605431
# id[3217]=user5410543285463548564
# id[3218]=user566790567674456114
# id[3219]=user5930770748446223328
# id[3220]=user6186649886734889636
# id[3221]=user6445203925681047301
# id[3222]=user6707105910404349868
# id[3223]=user6967445061497257663
# id[3224]=user7224773948808805646
# id[3225]=user7484440054782059896
# id[3226]=user7741987257311449762
# id[3227]=user8002221177794028771
# id[3228]=user8263676286612703987
# id[3229]=user8522227934087102378
# id[3230]=user8783021815081821835
# id[3231]=user9040789724300812858


####
# testset2

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


####
# DEBUG

# 4
# id[400]=a
# id[401]=b
# id[402]=c
# id[403]=d


echo "======== INIT ========"
LANG=C date
INIT_MACHINE=""
N=$((${NUM_NODES} * 100))
for node in ${NODE_LIST}
do
    if [ -z ${INIT_MACHINE} ]
    then
	INIT_MACHINE=${node}
	echo "== init adisk${node} ${id[${N}]} =="
	(echo "init ${id[${N}]}" ; sleep 10) | telnet adisk${node} 8081
    else
	echo "== init adisk${node} ${id[${N}]} TO adisk${INIT_MACHINE} =="
	(echo "init ${id[${N}]} adisk${INIT_MACHINE} 8081" ; sleep 10) | telnet adisk${node} 8081
    fi
    N=$((${N} + 1))
done

sleep 10

echo "======== ADJUST ========"
LANG=C date
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done
for node in ${NODE_LIST}
do
    echo "== adjust adisk${node} =="
    (echo -e "interactive\r\nadjust\r\nquit" ; sleep 10) | telnet adisk${node} 8081 &
    sleep 1
done

sleep 10

echo "======== STATUS ========"
LANG=C date
for node in ${NODE_LIST}
do
    echo "== status adisk${node} =="
    (echo -e "interactive\r\nstatus\r\nquit" ; sleep 10) | telnet adisk${node} 8081
done

# for debug
# exit 0

echo "======== PUT ========"
LANG=C date
N=0
for node in ${NODE_LIST}
do
    echo "== source /home/naoki/testset/put/${NUM_NODES}/put${N}.dat TO adisk${node} =="
    (echo "source /home/naoki/testset/put/${NUM_NODES}/put${N}.dat" ; sleep 10) | telnet adisk${node} 8081 &
    N=$((N + 1))
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 300

    for node in ${NODE_LIST}
    do
	# echo "== source _status_ TO adisk${node} =="
	(echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet adisk${node} 8081
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
    for node in ${NODE_LIST}
    do
	echo "== source _abort_ TO adisk${node} =="
	(echo "source _abort_" ; sleep 10) | telnet adisk${node} 8081
    done
fi

# echo "DEBUG"
# exit 0

echo "======== GET ========"
LANG=C date
N=0
for node in ${NODE_LIST}
do
    echo "== source /home/naoki/testset/get/32/get${N}.dat TO adisk${node} =="
    (echo "source /home/naoki/testset/get/32/get${N}.dat" ; sleep 10) | telnet adisk${node} 8081 &
    N=$((N + 1))
    # sleep 0.5
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 300

    for node in ${NODE_LIST}
    do
	# echo "== source _status_ TO adisk${node} =="
	(echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet adisk${node} 8081
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
    for node in ${NODE_LIST}
    do
	echo "== source _abort_ TO adisk${node} =="
	(echo "source _abort_" ; sleep 10) | telnet adisk${node} 8081
    done
fi


echo "======== RANGE ========"
LANG=C date
N=0
for node in ${NODE_LIST}
do
    echo "== source /home/naoki/testset/range/32/range${N}.dat TO adisk${node} =="
    (echo "source /home/naoki/testset/range/32/range${N}.dat" ; sleep 10) | telnet adisk${node} 8081 &
    N=$((N + 1))
done

# wait
FLAG_ALL_DONE=0
for i in $(seq 30) ; do
    sleep 300

    for node in ${NODE_LIST}
    do
	# echo "== source _status_ TO adisk${node} =="
	(echo -e "interactive\r\nsource _status_\r\nquit" ; sleep 10) | telnet adisk${node} 8081
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
    for node in ${NODE_LIST}
    do
	echo "== source _abort_ TO adisk${node} =="
	(echo "source _abort_" ; sleep 10) | telnet adisk${node} 8081
    done
fi

echo "======== END ========"
LANG=C date
# for node in ${NODE_LIST}
# do
#     echo "== status TO adisk${node} =="
#     (echo -e "interactive\r\nstatus\r\nquit" ; sleep 10) | telnet adisk${node} 8081
# done
