package distributedIndex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import loadBalance.LoadInfoTable;
import log_analyze.AnalyzerManager;
import main.Main;
import message.LoadMessage;
import node.DataNode;

import util.MessageSender;
import util.MyUtil;

public abstract class AbstractDistributedIndex extends MyUtil implements DistributedIndex{

	protected InetSocketAddress myAddress;

	protected static final double errorRangeRate = 1.01;
	protected static final int maxDataSizeCanBeMoved = 100;
	protected static final int ifOverThisNumberThenMoveDataHappen = 10;
	protected int counterForLoadCheck = 0;

	protected static final char returnChar='\n';


	public void setMyAddress(InetSocketAddress address){
		myAddress = address;
	}

	/*
	 * getter
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



	public void startLoadBalance(LoadInfoTable lit){
		// ##### 計算機の状態をログに出力 #####
		pri(fromStatusToString());
		// ##### /計算機の状態をログに出力 #####

		// ##### 時間測定用変数 #####
		long checkStartTime_msec = getCurrentTime();
		Long moveStartTime_msec;
		Long updateStartTime_msec;
		Long checkEndTime_msec;
		// ##### /時間測定用変数 #####
		
		counterForLoadCheck++;

		pri("##### 負荷集計フェーズ #####");
		LoadDataBox ldb = checkLoad(lit,getSender());
		pri("##### /負荷集計フェーズ #####");



		pri(" ##### 集計結果転送フェーズ  ##### ");
		sendLoadInfo(ldb, getSender());
		pri("##### /集計結果転送フェーズ  #####");


		moveStartTime_msec = getCurrentTime();
		boolean isMoveHappend = false;
		if(counterForLoadCheck < ifOverThisNumberThenMoveDataHappen){
			pri(" ##### 負荷分散のためにデータ移動フェーズ #####");
			isMoveHappend = moveData(ldb);
			pri(" ##### /負荷分散のためにデータ移動フェーズ #####");
		}

		updateStartTime_msec = getCurrentTime();

		if(counterForLoadCheck < ifOverThisNumberThenMoveDataHappen){
			if(isMoveHappend){
				pri(" ##### インデックスフェーズ #####");
				updateIndex();
				pri(" ##### インデックスフェーズ #####");
			}
		}

		//アクセス負荷とデータ容量をログに出力
		log( AnalyzerManager.getLogLoadTag()
				+" "+checkStartTime_msec
				+" "+ldb.getMyLoad()
				+" "+ldb.getMyDataSize()
				+" "+ldb.getThreshold()
				+" "+ldb.getPrevLoad()
				+" "+ldb.getNextLoad());
		checkEndTime_msec = getCurrentTime();
		//負荷転送フェーズにかかった時間
		log("LOG-LOADBLANCE-CHECKLOAD-TIME"
				+" "+checkStartTime_msec
				+" "+(moveStartTime_msec-checkStartTime_msec)
				+" "+(updateStartTime_msec-moveStartTime_msec)
				+" "+(checkEndTime_msec-updateStartTime_msec));
	}

	protected abstract String fromStatusToString();
	protected abstract LoadDataBox checkLoad(LoadInfoTable loadInfoTable, MessageSender sender);
	protected abstract boolean sendLoadInfo(LoadDataBox ldb, MessageSender sender);
	protected abstract boolean moveData(LoadDataBox ldb);
	protected abstract boolean updateIndex();
	protected abstract String recieveAndUpdateDataForLoadMove(DataNode[] dataNodes,
			InetSocketAddress senderAddress);


	public void resetLoadCounter(){
		synchronized (this) {
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				dataNode.resetLoadCounter();
				dataNode = dataNode.getNext();
			}
		}
	}


	protected int getTotalDataSizeByB_link(){
		synchronized (this) {
			int dataSize =0;
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				dataSize += dataNode.size();
				dataNode = dataNode.getNext();
			}
			return dataSize ;
		}
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
