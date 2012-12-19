package distributedIndex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import loadBalance.LoadInfoTable;
import message.LoadMessage;
import node.DataNode;
import util.MessageSender;

public abstract class AbstractSingleLinkDistributedIndex extends AbstractDistributedIndex {

	public abstract InetSocketAddress getNextMachine();



	protected String fromStatusToString(){
		StringBuilder sb = new StringBuilder();
		sb.append("getMyAddressIPString:"+this.getMyAddressIPString());
		sb.append("getNextMachineIP:"+getNextMachineIPString());
		return sb.toString();
	}




	public boolean sendLoadInfo(LoadDataBox ldb, MessageSender sender){
		try {
			sender.setHeader("LOAD_INFO");
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

		synchronized (this) {

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
			if((ldb.getNextLoad()!=-1)
					&&(ldb.getMyLoad()> ldb.getNextLoad())
					){
				priJap("次の計算機にデータを送ります");
				DataNode dataNode = getFirstDataNode();
				synchronized (this) {
					DataNode dn = getFirstDataNode();
					while(dn != null){
						dataNode = dn;
					}
				}
				while( true  ){
					if( ( ldb.getMyLoad() - (tempLoadCount + dataNode.getLoad()) ) <= ldb.getThreshold()
							|| (ldb.getMyLoad() -(tempLoadCount + dataNode.getLoad()) <= ldb.getNextLoad()) ){
						break;
					}
					tempLoadCount += dataNode.getLoad();
					dataNodeToBeMoved.add(dataNode);
					dataNode = dataNode.getPrev();
				}
				if(dataNodeToBeMoved.size() > 0){
					target = this.getNextMachine();
				}
			}
		}

		//転送するデータノードが決まったら、データノードに蓄積したアクセス負荷の情報をリセットします。
		//でないと、アクセス数がのこったまま転送されます。
		resetLoadCounter();


		if(target!=null){
			//実際にデータ転送が行われるのはここ
			MessageSender sender = this.getSender();

			priJap("データノード移動開始します。");
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
			synchronized (this) {
				try {
					//いまはデータ移動とインデックス更新が終わると「OK」を返すようにしています。
					sender.setHeader("LOAD_MOVE_DATA_NODES");
					String responseMessage = sender.sendDataNodeAndReceive((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]), this.getMyAddress(), target);
					priJap("次のような返事を受け取りました");
					pri(responseMessage);
					if(responseMessage.equals("OK")){
						ldb.setIsMoved(true);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return ldb;
	}



}
