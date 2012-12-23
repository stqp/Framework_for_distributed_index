package distributedIndex;
// PRing.java


import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.omg.CosNaming.NamingContextExtPackage.AddressHelper;

import loadBalance.LoadInfoTable;
import log_analyze.MyStringBuilder;
import message.DataMessage;
import message.LoadMessage;
import message.Message;
import message.UpdateInfoMessage;

import store.TreeLocalStore;
import store.TreeNode;
import util.AlphanumericID;
import util.ID;
import util.LatchUtil;
import store.LocalStore;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;

import node.AddressNode;
import node.DataNode;
import node.Node;

public class PRing extends AbstractDistributedIndex implements DistributedIndex{


	private final static String NAME = "PRing";



	public String getName() {return NAME;}



	private ID id;

	private PRingNode rootNode;
	private ArrayList<PRingNode> interNodes;

	private LocalStore store;

	public PRing() {}


	public String handleMessge(InetAddress host, ID id, String[] text) {

		/*
		 * 負荷分散のためのデータノードの移動に伴ってインデックスの更新情報が転送されてくる
		 * 自分がデータ移動にかかわっている場合と
		 * そうでない場合で処理が異なる
		 */
		if (text[0].equals(tagForLoadBalanceUpdateInfo)) {

			priJap("負荷分散のためのデータノードの移動に伴ってインデックスの更新情報が転送されてきました");
			UpdateInfoMessage upInfoMes = UpdateInfoMessage.fromJson(removeTag(text[0], tagForLoadBalanceUpdateInfo) );

			//level方向（HRの縦）のループ
			for(PRingNode pringNode: this.interNodes){
				//order方向(HRの横)のループ
				for(AddressNode addrNode : pringNode.successors){
					//PRing用のアドレスノードクラスに変換して使いやすくします。
					PRingAddressNode paddrNode = (PRingAddressNode)addrNode;

					//HRからデータノード受け手の計算機をみつけたときは担当を更新する
					if(equalsAddress(paddrNode.getAddress(), upInfoMes.getReceiverMachine()) == true){
						paddrNode.setIDByString(upInfoMes.getReceverMachineIDString());
					}
				}
			}

			/*
			 * もし自分が送り主の時にはそこで更新情報の転送をやめる=何もしない
			 *
			 * 自分がデータノードの送り主でないとき
			 * 1.更新情報を元にHRを更新（送り主の担当IDは変わらないので、受けて側の担当IDを更新する）
			 * 2.右隣の計算機へ更新情報をさらに渡す
			 */
			if(!equalsAddress(upInfoMes.getSenderMachine(), this.getMyAddress())){
				priJap("さらに隣へ転送します");
				try {
					getSender().send(
							"message " +text[0],
							getAddressNodeFromHR(getMiddleLevel(), 1).getAddress());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		return "";
	}

	public void initialize(ID id) {
		synchronized (this) {
			this.id = id;
			PRingNode prn = new PRingNode(0);
			this.rootNode = prn;
			this.interNodes = new ArrayList<PRingNode>();
			this.interNodes.add(prn);
			this.store = new TreeLocalStore();
		}
	}



	public void initialize(DistributedIndex _distIndex, InetSocketAddress addr, ID id) {

		synchronized (this) {
			PRing distIndex = (PRing)_distIndex;
			this.id = distIndex.id;
			this.rootNode = distIndex.rootNode;
			this.interNodes = distIndex.interNodes;
			AddressNode[] addrNodes = this.interNodes.get(0).successors;
			for (int i = 0; i < addrNodes.length; i++) {
				if (addrNodes[i].getAddress() == null) {
					addrNodes[i] = new AddressNode(addr, id.toMessage());
					break;
				}
			}


			this.store = distIndex.store;
		}
	}



	public PRing toInstance(String[] text, ID id) {
		return PRing._toInstance(text, id);
	}

	public static PRing _toInstance(String[] text, ID id) {
		int i = 0;
		ID prid = id.toInstance(text[i]); i++;
		PRingNode rootNode = null;
		ArrayList<PRingNode> interNodes = new ArrayList<PRingNode>();
		PRingNode dummyPRingNode = new PRingNode(0);
		for (; i < text.length;) {
			String name = text[i]; i++;
			if (name.compareTo(dummyPRingNode.getName()) != 0) {
				i--;
				break;
			}
			int n = Integer.parseInt(text[i]); i++;
			String[] temp = new String[n];
			System.arraycopy(text, i, temp, 0, n);
			PRingNode node = PRingNode.toInstance(temp, id); i += n;
			node.childNode = rootNode;
			interNodes.add(node);
			rootNode = node;
		}

		PRing pr = new PRing();
		// pr.initialize(prid);
		pr.id = prid;
		pr.rootNode = rootNode;
		pr.interNodes = interNodes;

		String name = text[i]; i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		pr.store = TreeLocalStore._toInstance(temp, id); i += n;

		return pr;
	}

	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.id.toMessage() + " ");
		for (PRingNode prn: this.interNodes) {
			sb.append(prn.toMessage() + " ");
		}
		sb.append(this.store.toMessage());
		String msg = sb.toString();
		String[] temp = msg.split(" ");
		return NAME + " " + temp.length + " " + msg;
	}

	public String toAdjustInfo() {
		PRingNode prn = this.interNodes.get(0);
		AddressNode addrNode = prn.successors[prn.successors.length - 1];
		if (addrNode.getAddress() != null) {
			InetSocketAddress addr = addrNode.getAddress();
			return addr.getAddress().getHostAddress() + ":" + addr.getPort() + ":" + addrNode.getText();
		}
		else {
			return "_null_";
		}
	}

	public boolean adjustCmd(MessageSender sender) throws IOException {
		ArrayList<PRingNode> old_interNodes = this.interNodes;
		ArrayList<PRingNode> new_interNodes = new ArrayList<PRingNode>();
		PRingNode temp_parent = null;
		String msg = "status adjustinfo";
		AddressNode addrNode = old_interNodes.get(0).successors[0];
		for (int i = 0; ; i++) {
			InetSocketAddress addr = addrNode.getAddress();
			if (addr == null) break;
			ID addr_id = this.id.toInstance(addrNode.getText());
			String res = sender.sendAndReceive(msg, addr);
			String[] text = res.split(" ");
			int t = 0;
			String name = text[t]; t++;
			int length = Integer.parseInt(text[t]); t++;
			String[] temp = new String[length];
			System.arraycopy(text, t, temp, 0, length);
			PRing temp_pr = _toInstance(temp, this.id); t += length;
			PRingNode temp_prn = (temp_pr.interNodes.size() > i) ? temp_pr.interNodes.get(i) : new PRingNode(i);
			PRingNode new_prn = new PRingNode(i);
			new_prn.childNode = temp_parent;
			temp_parent = new_prn;
			new_prn.successors[0] = addrNode;
			for (int j = 1; j < new_prn.successors.length; j++) {
				AddressNode newNode = temp_prn.successors[j - 1];
				if (newNode.getAddress() == null) break;
				ID new_id = this.id.toInstance(newNode.getText());
				if (this.id.compareTo(addr_id) < 0) {
					if (new_id.compareTo(addr_id) > 0 || new_id.compareTo(this.id) < 0) {
					}
					else break;
				}
				else if (this.id.compareTo(addr_id) > 0) {
					if (new_id.compareTo(addr_id) > 0 && new_id.compareTo(this.id) < 0) {
					}
					else break;
				}
				else {
					System.err.println("ERROR PRing#adjustCmd");
					return false;
				}
				new_prn.successors[j] = newNode;
			}
			new_interNodes.add(new_prn);
			addrNode = new_prn.successors[new_prn.successors.length - 1];
		}
		synchronized (this.interNodes) {
			this.rootNode = temp_parent;
			this.interNodes = new_interNodes;
		}
		return true;
	}

	public String getAdjustCmdInfo() throws IOException {
		synchronized (this.interNodes) {
			return toMessage();
		}
	}

	public AddressNode adjust(String text, ID id, InetSocketAddress addr, String info) {
		// test=0, id=new id, addr=new addr, info=_null_ or last addrNode of new level0
		if (info == null) {
			return null;
		}

		AddressNode[] addrNodes = this.interNodes.get(0).successors;
		ID temp = this.id;
		int index;
		for (index = 0; index < addrNodes.length; index++) { // CAUTION HR_ORDER>=2
			if (addrNodes[index].getAddress() == null) break;
			ID temp1 = temp.toInstance(addrNodes[index].getText());
			if (temp.compareTo(temp1) > 0) {
				if (id.compareTo(temp) >= 0 || id.compareTo(temp1) < 0) break;
			}
			else if (temp.compareTo(temp1) < 0) {
				if (id.compareTo(temp) >= 0 && id.compareTo(temp1) < 0) break;
			}
			else {
				System.err.println("WARNING PRing#adjust same id");
			}
			temp = temp1;
		}
		if (index == addrNodes.length) return new AddressNode(addrNodes[0].getAddress(), "0");

		for (int i = addrNodes.length - 1; i > index; i--) {
			addrNodes[i] = addrNodes[i - 1];
		}
		addrNodes[index] = new AddressNode(addr, id.toMessage());

		PRingNode level0 = this.interNodes.get(0);
		this.interNodes = new ArrayList<PRingNode>();
		this.interNodes.add(level0);
		if (addrNodes[addrNodes.length - 1].getAddress() != null) {
			for (int l = 1; ; l++) {
				AddressNode[] childAddrNodes = this.interNodes.get(l - 1).successors;
				if (this.interNodes.size() <= l) {
					PRingNode newNode = new PRingNode(l);
					newNode.childNode = this.interNodes.get(l - 1);
					newNode.successors[0] = childAddrNodes[childAddrNodes.length - 1];
					this.interNodes.add(newNode);
					this.rootNode = newNode;
					break;
				}
				else {
					AddressNode[] curAddrNodes = this.interNodes.get(l).successors;
					for (int i = curAddrNodes.length - 1; i >= 1; i--) {
						curAddrNodes[i] = curAddrNodes[i - 1];
					}
					curAddrNodes[0] = childAddrNodes[childAddrNodes.length - 1];
					if (curAddrNodes[curAddrNodes.length - 1].getAddress() == null) break;
				}
			}
		}
		if (index == this.interNodes.get(0).successors.length - 1) {
			ID a = this.id.toInstance(this.interNodes.get(1).successors[0].getText());
			String[] items = info.split(":", 3);
			ID b = this.id.toInstance(items[2]);
			if (this.id.compareTo(a) < 0) {
				if (b.compareTo(this.id) >= 0 && b.compareTo(a) < 0) return new AddressNode(null, "_null_");
			}
			else {
				if (b.compareTo(this.id) >= 0 || b.compareTo(a) < 0) return new AddressNode(null, "_null_");
			}
			InetSocketAddress ar = null;
			try {
				InetAddress host = InetAddress.getByName(items[0]);
				ar = new InetSocketAddress(host, Integer.parseInt(items[1]));
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			AddressNode an = new AddressNode(ar, items[2]);
			this.interNodes.get(1).successors[1] = an;
		}

		return new AddressNode(null, "_null_");
	}

	public InetSocketAddress[] getAckMachine() {
		return new InetSocketAddress[0];
		// AddressNode addrNode = this.interNodes.get(0).successors[1]; // CAUTION magic number 1
		// if (addrNode.getAddress() != null) {
		//     InetSocketAddress[] res = new InetSocketAddress[1];
		//     res[0] = this.interNodes.get(0).successors[0].getAddress();
		//     return res;
		// }
		// return new InetSocketAddress[0];
	}

	public ID getID() {return this.id;}

	public ID[] getResponsibleRange(MessageSender sender) throws IOException {
		ID[] range = new ID[2];
		AddressNode addrNode = this.interNodes.get(0).successors[0];
		if (addrNode.getAddress() != null) {
			range[0] = this.id;
			range[1] = this.id.toInstance(addrNode.getText());
		}
		return range;
	}

	public DataNode getFirstDataNode() {
		return this.store.getFirstDataNode();
	}

	public DataNode getNextDataNode(DataNode dataNode) {
		return this.store.getNextDataNode(dataNode);
	}

	public ID[] getDataNodeRange(DataNode dataNode) {
		return this.store.getRange(dataNode);
	}

	public InetSocketAddress getNextMachine() {
		return this.interNodes.get(0).successors[0].getAddress();
	}

	// public InetSocketAddress getPrevMachine() {
	// 	return null;
	// }

	public Node searchKey(MessageSender sender, ID key) throws IOException {
		return searchKey(sender, key, this.rootNode);
	}

	public Node searchKey(MessageSender sender, ID key, String text) throws IOException {
		if (text != null && text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}
		return searchKey(sender, key, this.rootNode);
	}

	// public Node searchKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return searchKey(sender, key, start);
	// }

	private Node searchKey(MessageSender sender, ID key, PRingNode start) throws IOException {
		PRingNode current = start;
		while (current != null) {
			AddressNode res = null;
			for (AddressNode addrNode: current.successors) {
				if (addrNode.getAddress() == null) break;
				ID addrID = this.id.toInstance(addrNode.getText());
				if (this.id.compareTo(addrID) > 0) {
					if (key.compareTo(this.id) >= 0 || key.compareTo(addrID) < 0) break;
				}
				else if (this.id.compareTo(addrID) < 0) {
					if (key.compareTo(this.id) >= 0 && key.compareTo(addrID) < 0) break;
				}
				else {
					System.err.println("ERROR PRing#searchKey(MessageSender, ID, PRingNode)");
				}
				res = addrNode;
			}
			if (res != null) return res;
			current = current.childNode;
		}
		return this.store.searchKey(key);
	}


	public NodeStatus[] searchData(MessageSender sender, ID[] range) {
		// System.err.println("DEPLICATE PRing#searchData(ID[])");
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		DataNode node = null;
		if (range[0] == null) {
			node = this.store.getFirstDataNode();
		}
		else {
			node = this.store.searchKey(range[0]);
		}
		while (node != null) {
			ID[] r = this.store.getRange(node);
			if (range[1] != null && r[0] != null && r[0].compareTo(range[1]) >= 0) {
				break;
			}
			NodeStatus s = node.searchData();
			status.add(s);
			node = this.store.getNextDataNode(node);
		}
		return status.toArray(new NodeStatus[0]);
	}

	public NodeStatus searchData(MessageSender sender, DataNode dataNode) {
		// System.err.println("DEPLICATE PRing#searchData(DataNode)");
		return this.store.searchData(dataNode);
	}

	public NodeStatus[] searchData(MessageSender sender, DataNode[] dataNodes) {
		return this.store.searchData(dataNodes);
	}

	public void endSearchData(MessageSender sender, NodeStatus[] status) {
		for (NodeStatus s: status) {
			this.endSearchData(sender, s);
		}
	}

	public void endSearchData(MessageSender sender, NodeStatus status) {
		this.store.endSearchData(status);
	}

	public Node updateKey(MessageSender sender, ID key) throws IOException {
		return updateKey(sender, key, this.rootNode);
	}

	public Node updateKey(MessageSender sender, ID key, String text) throws IOException {
		if (text != null && text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}
		return updateKey(sender, key, this.rootNode);
	}

	// public Node updateKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return updateKey(sender, key, start);
	// }

	public Node updateKey(MessageSender sender, ID key, PRingNode start) throws IOException {
		PRingNode current = start;
		while (current != null) {
			AddressNode res = null;
			for (AddressNode addrNode: current.successors) {
				if (addrNode.getAddress() == null) break;
				ID addrID = this.id.toInstance(addrNode.getText());
				if (this.id.compareTo(addrID) > 0) {
					if (key.compareTo(this.id) >= 0 || key.compareTo(addrID) < 0) break;
				}
				else if (this.id.compareTo(addrID) < 0) {
					if (key.compareTo(this.id) >= 0 && key.compareTo(addrID) < 0) break;
				}
				else {
					System.err.println("ERROR PRing#updateKey(MessageSender, ID, PRingNode)");
				}
				res = addrNode;
			}
			if (res != null) return res;
			current = current.childNode;
		}
		return this.store.updateKey(key);
	}

	public NodeStatus[] updateData(MessageSender sender, ID[] range) {
		// System.err.println("DEPLICATE PRing#updateData(ID[])");
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		DataNode node = null;
		if (range[0] == null) {
			node = this.store.getFirstDataNode();
		}
		else {
			node = this.store.updateKey(range[0]);
		}
		while (node != null) {
			ID[] r = this.store.getRange(node);
			if (range[1] != null && r[0] != null && r[0].compareTo(range[1]) >= 0) {
				break;
			}
			NodeStatus s = node.updateData();
			status.add(s);
			node = this.store.getNextDataNode(node);
		}
		return status.toArray(new NodeStatus[0]);
	}

	public NodeStatus updateData(MessageSender sender, DataNode dataNode) {
		// System.err.println("DEPLICATE PRing#updateData(DataNode)");
		return this.store.updateData(dataNode);
	}

	public NodeStatus[] updateData(MessageSender sender, DataNode[] dataNodes) {
		return this.store.updateData(dataNodes);
	}

	public void endUpdateData(MessageSender sender, NodeStatus[] status) {
		for (NodeStatus s: status) {
			this.endUpdateData(sender, s);
		}
	}

	public void endUpdateData(MessageSender sender, NodeStatus status) {
		this.store.endUpdateData(status);
	}


	public ID[] getRangeForNew(ID id) {
		ID[] range = new ID[2];
		range[0] = id;
		return range;
	}



	public PRing splitResponsibleRange(MessageSender sender, ID[] range, ID id, NodeStatus[] status, InetSocketAddress addr) {
		PRing splitedIndex = new PRing();
		if (range[0] != null && range[1] == null) {
			splitedIndex.initialize(range[0]);
			PRingNode rootNode = null;
			ArrayList<PRingNode> interNodes = new ArrayList<PRingNode>();
			for (PRingNode prn: this.interNodes) {
				PRingNode temp = new PRingNode(prn.level);
				for (int i = 0; i < prn.successors.length; i++) {
					temp.successors[i] = prn.successors[i];
				}
				temp.childNode = rootNode;
				interNodes.add(temp);
				rootNode = temp;
			}
			splitedIndex.rootNode = rootNode;
			splitedIndex.interNodes = interNodes;
			// TODO: split fetch, easy
			splitedIndex.store = this.store.splitResponsibleRange(range, status);


			AddressNode[] addrNodes = this.interNodes.get(0).successors;
			for (int i = addrNodes.length - 1; i >= 1; i--) {
				addrNodes[i] = addrNodes[i - 1];
			}
			addrNodes[0] = new AddressNode(addr, range[0].toMessage());
			// PRingNode level0 = this.interNodes.get(0);
			// this.interNodes = new ArrayList<PRingNode>();
			// this.interNodes.add(level0);
			// if (addrNodes[addrNodes.length - 1].getAddress() != null) {
			// 	for (int l = 1; ; l++) {
			// 	    AddressNode[] childAddrNodes = this.interNodes.get(l - 1).successors;
			// 	    if (this.interNodes.size() <= l) {
			// 		PRingNode newNode = new PRingNode(l);
			// 		newNode.childNode = this.interNodes.get(l - 1);
			// 		newNode.successors[0] = childAddrNodes[childAddrNodes.length - 1];
			// 		this.interNodes.add(newNode);
			// 		this.rootNode = newNode;
			// 		break;
			// 	    }
			// 	    // if (this.interNodes.size() == 1) {
			// 	    //     this.interNodes.add(new PRingNode(1));
			// 	    //     this.interNodes.get(1).successors[0] = addrNodes[addrNodes.length - 1];
			// 	    // }
			// 	    else {
			// 		AddressNode[] curAddrNodes = this.interNodes.get(l).successors;
			// 		for (int i = curAddrNodes.length - 1; i >= 1; i--) {
			// 		    curAddrNodes[i] = curAddrNodes[i - 1];
			// 		}
			// 		curAddrNodes[0] = childAddrNodes[childAddrNodes.length - 1];
			// 		if (curAddrNodes[curAddrNodes.length - 1].getAddress() == null) break;
			// 	    }
			// 	}
			// }
		}
		else {
			System.err.println("ERROR PRing#splitResponsibleRange");
		}

		return splitedIndex;
	}



	public String toString() {
		// synchronized (this) {
		StringBuilder sb = new StringBuilder();
		sb.append("PRing: " + Shell.CRLF);
		sb.append("  ID: " + this.id + Shell.CRLF);
		sb.append("  LIST:" + Shell.CRLF);
		for (int i = this.interNodes.size() - 1; i >= 0; i--) {
			sb.append("  " + this.interNodes.get(i) + Shell.CRLF);
		}
		return sb.toString();
		// }
	}





	/*@Override
	public void checkLoad(LoadInfoTable loadInfoTable, MessageSender sender) {
		// TODO 自動生成されたメソッド・スタブ

		pri("My address:" + this.getMyAddressIPString());

		pri("##### 負荷集計フェーズ #####");


		int nextLoad = 0;
		//もしまだ1つ前の計算機の負荷が登録されていなければそっちにデータ移動はできないよね
		//1つ後の計算機に関しても同様。
		//そのためのチェックをしているだけだが、長くなってしまった。
		//nullかどうか調べておかないとすぐエラーになってしまうので。
		if(this.getNextMachine() != null && loadInfoTable.getLoadList() != null
				&& loadInfoTable.getLoadList().get(this.getNextMachineIPString()) != null){
			priJap("後の計算機の負荷を取得できました");
			nextLoad = loadInfoTable.getLoadList().get(this.getNextMachineIPString());
		}

		DataNode rightMostDataNode = this.getFirstDataNode();
		int myLoad = 0;



		pri("====== 負荷を集計します ======");
		synchronized (this) {
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				myLoad += dataNode.getLoad();
				rightMostDataNode = dataNode;
				dataNode = dataNode.getNext();
			}
		}

		pri("====== 自分の負荷を更新 ======");
		loadInfoTable.setLoad(this.getMyAddressIPString(), myLoad);

		pri("====== 負荷の平均値を再計算 =====");
		loadInfoTable.reCalcAverage();


		pri("LOAD_INFO_TABLE_TOJSON :"+loadInfoTable.toJson());

		//データ更新された後に平均値を取得するという順番に注意！
		int average = loadInfoTable.getAverage();
		int threshold = (int) (average * errorRangeRate);

		pri("getNextMachineIP : "+ this.getNextMachineIPString());


		pri("myLoad : "+ myLoad);
		pri("nextLoad : " + nextLoad);
		pri("threshold : "+ threshold);



		// ##### 負荷情報を隣へ転送するフェーズ #####

		try {
			pri("getMyAddressIPString : " +this.getMyAddressIPString());
			pri("loadInfoTable : "+ loadInfoTable.toJson());


			sender.setHeader("LOAD_INFO");
			if( this.getNextMachine() != null){
				pri("====== 右隣へ負荷情報を回します =====");
				sender.send(
						(new LoadMessage(this.getMyAddressIPString(), loadInfoTable)).toJson(),
						this.getNextMachine());
				priJap("一番遠そうな計算機へ負荷情報を回します");
				sender.send(
						(new LoadMessage(this.getMyAddressIPString(), loadInfoTable)).toJson(),
						this.getAddressNodeFromHR(this.getLevel()/2, 0).getAddress());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}




		//##### 負荷移動フェーズ #####


		 * この場合は負荷分散が必要ない
		 * １．自分の負荷がある閾値より小さい
		 * ２．自分の負荷が両隣の負荷のどちらよりも小さい

		if(myLoad < threshold || myLoad < nextLoad ){
			//負荷集計が終わったらデータノードに蓄積したアクセス負荷の情報をリセットします。
			resetLoadCounter();
			return ;
		}

		priJap("負荷分散のためにデータノードを移動します。");
		//前の計算機へデータを送る場合
		if(myLoad > nextLoad && nextLoad != 0){

			priJap("次の計算機にデータを送ります");

			ArrayList<DataNode> dataNodeToBeMoved = new ArrayList<DataNode>();
			DataNode dataNode = rightMostDataNode;
			int tempLoadCount = 0;
			int tempDataCount = 0;

			while( true  ){
				if( ( myLoad - (tempLoadCount + dataNode.getLoad()) ) < threshold
						|| maxDataSizeCanBeMoved < (tempDataCount + dataNode.size())){
					break;
				}
				tempDataCount += dataNode.size();
				tempLoadCount += dataNode.getLoad();
				dataNodeToBeMoved.add(dataNode);
				dataNode = dataNode.getPrev();
			}


			if(dataNodeToBeMoved.size() > 0){
				moveData((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]),this.getNextMachine(), sender);
			}
		}

		//負荷集計が終わったらデータノードに蓄積したアクセス負荷の情報をリセットします。
		resetLoadCounter();
	}*/


	/*
	 * orderは行で見たときの番号を指定してください。
	 * level,orderは１以上の数字を渡してください。
	 * indexっぽく０以上の値ではなく、PRingの理論とおりにしてください。
	 */
	public AddressNode getAddressNodeFromHR(int level, int order){
		try{
			return this.interNodes.get(level-1).successors[order-1];
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}


	/*
	 * HRから指定した場所の計算機の担当IDを返します。
	 */
	public ID getIDFromHR(int level, int order){
		return AlphanumericID._toInstance(this.getAddressNodeFromHR(level, order).getText());
	}


	/*
	 * HRのレベルはたぶん計算機が追加されるごとに大きくなります。
	 * そこで現在のHRのレベルを返すメソッドを用意しておきます。
	 */
	public int getLevel(){
		return this.interNodes.size();
	}

	/*
	 * PRIngでは一番遠い計算機へ負荷情報や、更新情報を
	 * 送りたいのでこれを使います。
	 */
	public int getMiddleLevel(){
		return (getLevel()%2==0)? (getLevel()/2) : (getLevel()+1)/2;
	}

	public void sendUpdateInfoForDataNodeMove(){

	}


	private String tagForLoadBalanceUpdateInfo = "update";



	/*private void updateIndex(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target) {
		priJap("インデックス更新フェーズ");

		for(DataNode dn : dataNodesToBeRemoved){

			 * 目的のデータノードに対して
			 * 1.左右のデータノードからの参照と
			 * 2.親から参照を取り除く

			//if(dn.getPrev() != null){ dn.getPrev().setNext(null);}//左からの参照削除
			//if(dn.getNext() != null){ dn.getNext().setPrev(null);}//右からの参照削除
			//親からの参照削除します
			TreeNode parent = (TreeNode) dn.getParent();

			parent.removeDataNode(dn);



			priJap("START REMOVE CHILD FROM PARENT");
			priJap("INFOMATION OF PARENT NODE OF TARGET DATANODE");
			pri("CHILDREN SIZE IS");
			pri(parent.getChildrenSize());

			priJap("DATA_NODE_INFO: START");
			pri("DATA_NODE_INFO: TO_MESSAGE");
			pri(dn.toMessage());
			pri("DATA-NODE-MIN-ID:");
			pri(dn.getMinID().toString());
			priJap("DATA_NODE_INFO: END");




			priJap("END REMOVE CHILD FROM PARENT");


		}

	}
*/



	@Override
	public String fromStatusToString() {
		// TODO 自動生成されたメソッド・スタブ
		pri("fromStatusToString");
		MyStringBuilder mst = new MyStringBuilder("");
		mst.appendWithReturn("level:"+ getLevel());
		mst.appendWithReturn("middle level:"+getMiddleLevel());
		PRingNode prn = this.rootNode;
		for(int level=getLevel()-1; level>=0&& prn!=null;level--){
			mst.appendWithReturn("current level:"+ level);
			for(int i_order=0;i_order<prn.successors.length;i_order++){
				mst.appendWithReturn("getAddressNodeFromHR:"+ getAddressNodeFromHR(level, i_order));
				try{
					mst.appendWithReturn("address : "+ prn.successors[i_order].getAddress().toString());
				}catch(Exception e){
					mst.appendWithReturn("fail to get address from HR. level:"+level+" i_order:"+i_order);
				}

			}
			prn= prn.childNode;
		}

		return null;
	}


	protected LoadDataBox checkLoad(LoadInfoTable loadInfoTable,
			MessageSender sender) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}


	protected boolean sendLoadInfo(LoadDataBox ldb, MessageSender sender) {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}


	@Override
	protected LoadDataBox moveData(LoadDataBox ldb) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}


	@Override
	protected Message updateIndex(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target) {
				return null;
		// TODO 自動生成されたメソッド・スタブ

	}


	/*@Override
	public String receiveUpdateInfoForLoadMove(UpdateInfoMessage uim,
			InetSocketAddress senderAddress) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}*/


	@Override
	protected boolean sendLoadInfo(LoadDataBox loadDataBox,
			LoadInfoTable loadInfoTable, MessageSender sender) {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}


	@Override
	protected String updateIndexWhenReceivingData(DataMessage dataMessage) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}


	@Override
	protected String updateIndexWhenReceivingUpdateInfo(
			UpdateInfoMessage updateInfoMessage) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}


	@Override
	protected String sendUpdateInfo(Message message) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

}



class PRingAddressNode extends AddressNode{

	public PRingAddressNode(InetAddress host, int port, String text) {
		super(host, port, text);
	}

	public ID getID(){
		return AlphanumericID._toInstance(this.getText());
	}

	/*
	 * AddressNodeはIDを直接保持していません
	 * 手法ごとにこのtextを使い分けているだけです
	 * そこでこのメソッドを用意しました。
	 */
	public void setIDByString(String str){
		this.text = str;
	}

}



final class PRingNode implements Node {
	private static final String NAME = "PRingNode";
	public String getName() {return NAME;}

	private static final int HR_ORDER = 2;
	// private static final int HR_ORDER = 4;

	public int level;
	public AddressNode[] successors;
	public PRingNode childNode;

	private int[] status;

	public PRingNode(int level) {
		this.level = level;
		this.successors = new AddressNode[HR_ORDER];
		for (int i = 0; i < HR_ORDER; i++) {
			this.successors[i] = new AddressNode(null, "_null_"); // empty text occurs error
		}

		this.status = LatchUtil.newLatch();
	}

	public static PRingNode toInstance(String[] text, ID id) {
		int i = 0;
		int level = Integer.parseInt(text[i]); i++;

		AddressNode[] successors = new AddressNode[HR_ORDER];
		for (int c = 0; c < HR_ORDER; c++) {
			String name = text[i]; i++;
			int n = Integer.parseInt(text[i]); i++;
			String[] temp = new String[n];
			System.arraycopy(text, i, temp, 0, n);
			AddressNode successor = AddressNode._toInstance(temp, id); i += n;
			successors[c] = successor;
		}

		PRingNode node = new PRingNode(level);
		node.successors = successors;

		return node;
	}

	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.level + " ");
		for (int i = 0; i < HR_ORDER; i++) {
			sb.append(this.successors[i].toMessage() + " ");
		}
		sb.delete(sb.length() - 1, sb.length());
		String msg = sb.toString();
		String[] temp = msg.split(" ");
		return NAME + " " + temp.length + " " + msg;
	}


	public void ackUpdate(MessageSender sender, Node node) {}

	public NodeStatus searchData() {
		while (true) {
			synchronized (this.status) {
				if (this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.S]++;
					break;
				}
			}
		}
		return new NodeStatus(this, LatchUtil.S);
	}

	public void endSearchData(NodeStatus status) {
		synchronized (this.status) {
			this.status[status.getType()]--;
		}
	}


	public NodeStatus updateData() {
		while (true) {
			synchronized (this.status) {
				if (this.status[LatchUtil.S] == 0 && this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.X]++;
					break;
				}
			}
		}
		return new NodeStatus(this, LatchUtil.X);
	}

	public void endUpdateData(NodeStatus status) {
		synchronized (this.status) {
			this.status[status.getType()]--;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("L" + level + " ");
		for (AddressNode addrNode: this.successors) {
			sb.append(addrNode + " ");
		}
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}
}
