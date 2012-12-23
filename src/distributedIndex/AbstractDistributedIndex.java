package distributedIndex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import com.google.gson.Gson;

import loadBalance.LoadInfoTable;
import log_analyze.AnalyzerManager;
import main.Main;
import message.DataMessage;
import message.DataNodeMessage;
import message.LoadMessage;
import message.Message;
import message.UpdateInfoMessage;
import node.DataNode;

import util.MessageSender;
import util.MyUtil;

public abstract class AbstractDistributedIndex extends MyUtil implements DistributedIndex{

	protected InetSocketAddress myAddress;

	protected static final double errorRangeRate = 1.01;
	//protected static final int maxDataSizeCanBeMoved = 100;
	protected static final int ifOverThisNumberThenMoveDataHappen = 10;
	protected int counterForLoadCheck = 0;
	protected static final char returnChar='\n';
	protected LoadInfoTable lit= new LoadInfoTable();




	/*
	 * このクラスの機能
	 */
	public InetSocketAddress getMyAddress(){
		return myAddress;
	}
	public String getMyAddressIPString(){
		if(this.getMyAddress() == null){return "";}
		return getMyAddress().getAddress().toString();
	}
	public MessageSender getSender(){
		return Main._handler.getMessageReceiver().getMessageSender();
	}
	public void setMyAddress(InetSocketAddress address){
		myAddress = address;
	}
	protected String getNextMachineIPString(){
		if(getNextMachine()== null){return "";}
		return trimAddressString(getNextMachine().getAddress().toString());
	}
	public void setLoad(String master, LoadInfoTable load){
		synchronized (this.lit) {
			this.lit.updateLoadInfoList(
					master,
					load.getLoadList(),
					load.getTimeCard(),
					load.getDataSizeList());
		}
	}


	/*
	 * 中位クラスで実装してもらう処理
	 */
	protected abstract boolean sendLoadInfo(LoadDataBox loadDataBox, LoadInfoTable loadInfoTable,  MessageSender sender);
	protected abstract LoadDataBox moveData(LoadDataBox loadDataBox);
	protected abstract String fromStatusToString();


	/*
	 * 下位クラスで実装してもらう処理
	 */
	//データ移動での送り手側の更新
	protected abstract Message updateIndex(DataNode[] dataNodesToBeRemoved,	InetSocketAddress target);
	//データ移動での受けて側の更新
	protected abstract String updateIndexWhenReceivingData(DataMessage dataMessage);
	//移動の更新情報を受け取ったときの更新
	protected abstract String updateIndexWhenReceivingUpdateInfo(UpdateInfoMessage updateInfoMessage);
	//データ移動の情報を転送する
	protected abstract String sendUpdateInfo(Message message);


	/*
	 * shell.javaから呼び出される
	 */
	public void receiveLoadInfo(String someMessage){
		pri("===== 負荷情報を受け取りました。 ======");
		String temp = someMessage.substring("LOAD_INFO".length());
		LoadMessage loadmes = new Gson().fromJson(temp, LoadMessage.class);
		priJap("受け取ったメッセージ:"+someMessage);
		priJap("トリミングした後のメッセージ:"+temp);
		priJap("送り主:"+loadmes.getSender().toString());
		priJap("データ:"+loadmes.getLoadInfoTable().toJson());

		if(getMyAddressIPString().length() == 0 || loadmes.getLoadInfoTable() != null){
			setLoad(getMyAddressIPString(),loadmes.getLoadInfoTable());
		}
	}
	/*
	 * shell.javaから呼び出される
	 */
	public String receiveUpdateInfo(String someMessage){
		priJap("負荷分散のための更新情報受け取り");
		pri("メッセージ"+ someMessage);
		return updateIndexWhenReceivingUpdateInfo(
				(new Gson()).fromJson(someMessage, UpdateInfoMessage.class));
	}
	/*
	 * shell.javaから呼び出される
	 */
	public String  receiveData(String someMessage){
		priJap("負荷分散のためのデータ受け取り");
		String substring = someMessage.substring("LOAD_MOVE_DATA_NODES".length());
		DataMessage dm = (new Gson()).fromJson(substring, DataMessage.class);
		priJap("データノードを受け取りました");
		priJap("受け取ったメッセージは"+someMessage);
		if(dm.getDataNodeMessage().getSenderAddress()!=null){
			priJap("送り主は"+dm.getDataNodeMessage().getSenderAddress().toString());
		}else{
			priJap("送り主はエラーが出るので取得できません");
		}

		return updateIndexWhenReceivingData(dm);
	}



	public void startLoadBalance(){
		priJap("startLoadBalance");
		// ##### 計算機の状態をログに出力 #####
		pri(fromStatusToString());
		// ##### /計算機の状態をログに出力 #####
		pri("THREADCOUNT :" +Thread.activeCount());

		// ##### 時間測定用変数 #####
		long checkStartTime_msec = getElapsedTimeFromQueryStart();
		Long moveStartTime_msec;
		Long updateStartTime_msec;
		Long checkEndTime_msec;
		// ##### /時間測定用変数 #####

		counterForLoadCheck++;
		pri("データ移動から集計回数は:"+counterForLoadCheck);

		priJap("負荷集計フェーズ");
		LoadDataBox ldb = checkLoad();priJap("/負荷集計フェーズ");
		pri("集計結果転送フェーズ");
		sendLoadInfo(ldb,lit,  getSender());priJap("/集計結果転送フェーズ");


		moveStartTime_msec = getElapsedTimeFromQueryStart();
		int beforemove = getTotalDataSizeByB_link();
		if(counterForLoadCheck > ifOverThisNumberThenMoveDataHappen){
			priJap("データ移動フェーズ");
			ldb = moveData(ldb);priJap("/データ移動フェーズ");
		}
		int aftermove = getTotalDataSizeByB_link();


		DataNode right = getRightMostDataNode();
		int num=0;
		while(right!=null){
			num+=right.size();
			right=right.getPrev();
		}
		priJap("右端から左端へデータ数を調べた結果は:"+num);

		priJap("データボックスの状態を見てみる");
		pri(ldb.toString());
		updateStartTime_msec = getElapsedTimeFromQueryStart();


		if(counterForLoadCheck > ifOverThisNumberThenMoveDataHappen){
			if(ldb.getIsMoved() == true){
				priJap("インデックスフェーズ");
				Message mes = updateIndex(ldb.getDataNodesToBeMoved(), ldb.getTarget());priJap("/インデックスフェーズ");
				sendUpdateInfo(mes);
			}
			counterForLoadCheck=0;
		}

		if(ldb.getIsMoved() == true){
			int afterupdate = getTotalDataSizeByB_link();
			priJap("データ移動と更新の前後で整合性があるか調べます");
			pri("移動前:"+beforemove);
			pri("移動後"+aftermove);
			pri("更新後"+afterupdate);
		}

		int rightToLeft =0;
		DataNode rightMost= getRightMostDataNode();
		while(rightMost!=null){
			rightToLeft+=rightMost.size();
			rightMost=rightMost.getPrev();
		}
		priJap("右端から調べた時のデータの数は:"+rightToLeft);


		//アクセス負荷とデータ容量をログに出力
		log( AnalyzerManager.getLogLoadTag()
				+" "+checkStartTime_msec
				+" "+ldb.getMyLoad()
				+" "+ldb.getMyDataSize()
				+" "+ldb.getThreshold()
				+" "+ldb.getPrevLoad()
				+" "+ldb.getNextLoad());
		checkEndTime_msec = getElapsedTimeFromQueryStart();
		//負荷転送フェーズにかかった時間
		log("LOG-LOADBLANCE-CHECKLOAD-TIME"
				+" "+checkStartTime_msec
				+" "+(moveStartTime_msec-checkStartTime_msec)
				+" "+(updateStartTime_msec-moveStartTime_msec)
				+" "+(checkEndTime_msec-updateStartTime_msec));
	}




	synchronized protected LoadDataBox checkLoad() {
		LoadDataBox ldb = new LoadDataBox();
		if(this.getNextMachine() != null && lit.getLoadList() != null
				&& lit.getLoadList().get(this.getNextMachineIPString()) != null){
			ldb.setNextLoad(lit.getLoadList().get(this.getNextMachineIPString()));
			ldb.setNextDataSize(lit.getDataSizeList().get(this.getNextMachineIPString()));
		}
		int myLoad = 0;
		int myDataSize = 0;
		pri("====== 負荷を集計 ======");
		synchronized (this) {
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				myLoad += dataNode.getLoad();
				myDataSize += dataNode.size();
				dataNode = dataNode.getNext();
			}
		}
		pri("====== /負荷を集計 ======");
		pri("====== 自分の負荷更新と負荷平均値を再計算 ======");
		lit.setLoad(this.getMyAddressIPString(), myLoad);
		lit.setDataSize(this.getMyAddressIPString(), myDataSize);
		pri("====== /自分の負荷更新と負荷平均値を再計算 ======");

		ldb.setMyLoad(myLoad);
		ldb.setMyDataSize(myDataSize);
		ldb.setThreshold( (int) (lit.getAverage() * errorRangeRate));
		ldb.setLoadInfoTable(lit);
		return ldb;
	}













	/*
	 * ユーティリティ
	 */
	public DataNode getRightMostDataNode(){
		DataNode dn = getFirstDataNode();
		DataNode rightMost=null;
		while(dn != null){
			rightMost = dn;
			if(dn.getNext()!=null){
				dn.getNext().setPrev(dn);
			}
			dn = dn.getNext();
		}
		return rightMost;
	}
	public void resetLoadCounter(){
		DataNode dataNode = getFirstDataNode();
		while(dataNode != null){
			dataNode.resetLoadCounter();
			dataNode = dataNode.getNext();
		}
	}
	public int getTotalDataSizeByB_link(){
		int dataSize =0;
		DataNode dataNode = getFirstDataNode();
		while(dataNode != null){
			dataSize += dataNode.size();
			dataNode = dataNode.getNext();
		}
		return dataSize ;
	}

	public String removeTag(String str, String tag){
		return  str.substring(tag.length());
	}

	/*
	 * 二つのアドレスが等しいかどうか調べます。
	 * 調べる場所はIPアドレスのみです。
	 * ちなみに 192.168.0.102ではなく　/192.168.0.102となる必要があります。
	 */
	public boolean equalsAddress(InetSocketAddress addr1, InetSocketAddress addr2){
		if(addr1 == null || addr2 == null)return false;
		if(addr1.getAddress() == null || addr2.getAddress() == null){return false;}

		String adrStr1 = addr1.getAddress().toString();
		String adrStr2 = addr2.getAddress().toString();
		String TrimedAddr1 = trimAddressString(adrStr1);
		String TrimedAddr2 = trimAddressString(adrStr2);

		return TrimedAddr1.equals(TrimedAddr2);
	}
	public String trimAddressString(String str){
		return str.indexOf('/') > 0? str.substring(str.indexOf('/')): str;
	}




}
