package distributedIndex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import loadBalance.LoadInfoTable;
import message.LoadMessage;
import store.TreeLocalStore;
import store.TreeNode;
import util.ID;
import util.LatchUtil;
import util.MessageSender;

import node.AddressNode;
import node.DataNode;
import node.Node;

public abstract class AbstractDoubleLinkDistributedIndex extends AbstractSingleLinkDistributedIndex{

	protected InetSocketAddress prevMachine;

	protected InetSocketAddress getPrevMachine() {
		return this.prevMachine;
	}

	/**
	 * example:
	 * this.nextMachine.getAddress().toString -> "edn2/192.168.0.102" (end2 is computer name)
	 * then
	 * this method returns "192.168.0.102".
	 *
	 * i don't need "edn2".
	 * because i can't get address of "edn2/192.168.0.102",
	 * but can get "/192.168.0.102"
	 * so for key of loadInfoTable , i should trim the string of next machine address.
	 */
	protected String getPrevMachineIPString(){
		if(this.getPrevMachine() == null){return "";}
		String address = getPrevMachine().getAddress().toString();
		return address.substring(address.indexOf('/'));
	}


	protected String fromStatusToString(){
		StringBuilder sb = new StringBuilder();
		sb.append("getMyAddressIPString:"+getMyAddressIPString()+"\n");
		sb.append("getPrevMachineIPString:"+getPrevMachineIPString()+"\n");
		sb.append("getNextMachineIP:"+getNextMachineIPString()+"\n");
		return sb.toString();
	}




	public boolean sendLoadInfo(LoadDataBox ldb, LoadInfoTable lit, MessageSender sender){
		if(this.getPrevMachine() != null && lit != null && lit.getLoadList() != null
				&& lit.getLoadList().get(this.getPrevMachineIPString()) != null){
			ldb.setPrevLoad(lit.getLoadList().get(this.getPrevMachineIPString()));
			ldb.setPrevDataSize(lit.getDataSizeList().get(this.getPrevMachineIPString()));
		}
		try {
			sender.setHeader("LOAD_INFO");
			if( this.getPrevMachine() != null){
				pri("====== 左隣へ負荷情報を回します =====");
				sender.send((new LoadMessage(this.getMyAddressIPString(), ldb.getLoadInfoTable())).toJson(), this.getPrevMachine());
			}
			if( this.getNextMachine() != null){
				pri("====== 右隣へ負荷情報を回します =====");
				sender.send((new LoadMessage(this.getMyAddressIPString(), ldb.getLoadInfoTable())).toJson(), this.getNextMachine());
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}





	public LoadDataBox moveData(LoadDataBox ldb){
		ArrayList<DataNode> dataNodeToBeMoved = new ArrayList<DataNode>();
		InetSocketAddress target = null;
		int tempLoadCount = 0;

		/*　
		 * 計算機の位置により３パターンが考えられる
		 * 1.左端
		 * 2.右端
		 * 3.左端、右端のどちらでもない
		 */
		/*
		 * 左が自分より負荷小さい
		 * かつ
		 * 左隣を知っている
		 */
		if(ldb.getPrevLoad()!=-1
				&&(ldb.getMyLoad()> ldb.getPrevLoad())
				){
			priJap("前の計算機へデータを送ります");
			priJap("左端のデータノードを手に入れました。");
			/*
			 * 次の場合は移動するデータノードの探索を終了し移動に移ります。
			 * １．データノード移動あとの負荷が閾値より小さい
			 * ２．移動可能なデータ数（データ数＝データノードに格納されているID数）を超えた
			 */
			DataNode dataNode = this.getFirstDataNode();
			while( true  ){
				if(	dataNode==null
						|| (dataNode!=null&& dataNode.getNext()==null)
						|| (ldb.getMyLoad() - (tempLoadCount + dataNode.getLoad())) <= ldb.getThreshold()
						|| (ldb.getMyLoad() - (tempLoadCount + dataNode.getLoad())) <= ldb.getPrevLoad() ){
					break;
				}
				tempLoadCount += dataNode.getLoad();
				dataNodeToBeMoved.add(dataNode);
				dataNode = dataNode.getNext();
			}
			priJap("移動するデータノードを決定しました。");
			if(dataNodeToBeMoved.size() > 0){
				target = this.getPrevMachine();
			}
		}
		/*
		 * 右隣を知っている
		 * 右が自分より負荷小さい
		 * （かつ　PRingだと自分が右端ならデータ移動できない）
		 */
		else if((ldb.getNextLoad()!=-1)
				&&(ldb.getMyLoad()> ldb.getNextLoad())
				){
			priJap("次の計算機にデータを送ります");
			DataNode dataNode = getRightMostDataNode();
			priJap("右端のデータノードを手に入れました。");
			while( true  ){
				if( dataNode==null
						|| (dataNode!=null&& dataNode.getPrev()==null)
						|| (ldb.getMyLoad()- (tempLoadCount + dataNode.getLoad())) <= ldb.getThreshold()
						|| (ldb.getMyLoad()- (tempLoadCount + dataNode.getLoad())) <= ldb.getNextLoad() ){
					break;
				}
				tempLoadCount += dataNode.getLoad();
				dataNodeToBeMoved.add(dataNode);
				dataNode = dataNode.getPrev();
			}
			priJap("移動するデータノードを決定しました。");
			if(dataNodeToBeMoved.size()> 0){
				target = this.getNextMachine();
			}
		}

		priJap("データノードに蓄積したアクセスをリセットします。");
		//転送するデータノードが決まったら、データノードに蓄積したアクセス負荷の情報をリセットします。
		//でないと、アクセス数がのこったまま転送されます。
		resetLoadCounter();
		priJap("リセット終了");

		if(target!=null){
			priJap("データノード移動開始します。");

			//実際にデータ転送が行われるのはここ
			MessageSender sender = this.getSender();
			priJap("移動する相手のアドレスは");
			pri(target.getAddress().toString());
			priJap("移動するデータノードの数は");
			pri(dataNodeToBeMoved.size());
			priJap("移動するデータノードそれぞれに含むキーの数は");
			for(DataNode d: dataNodeToBeMoved){
				pri(d.size());
			}
			ldb.setTarget(target);
			ldb.setDataNodesToBeMoved((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]));
			try {
				sender.setHeader("LOAD_MOVE_QUESTION");
				String canMove = sender.customSendAndReceive("",target);
				if(canMove==null)canMove = ""; //null対策
				if(canMove.equals("OK")){
					priJap("相手はデータ移動受け入れ状態です。");
					//いまはデータ移動とインデックス更新が終わると「OK」を返すようにしています。
					sender.setHeader("LOAD_MOVE_DATA_NODES");
					String responseMessage = sender.sendDataNodeAndReceive((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]), this.getMyAddress(), target);
					priJap("次のような返事を受け取りました");
					pri(responseMessage);
					if(responseMessage==null)responseMessage="";//null対策
					if(responseMessage.equals("OK")){
						ldb.setIsMoved(true);
					}
				}else{
					priJap("相手はデータ移動中だったのでデータ転送を中止します");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return ldb;
	}









}
