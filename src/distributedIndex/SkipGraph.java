package distributedIndex;
// SkipGraph.java


import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;


import store.TreeLocalStore;
import store.TreeNode;
import util.DBConnector;
import util.ID;
import util.LatchUtil;
import store.LocalStore;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;

import loadBalance.LoadInfoTable;
import log_analyze.AnalyzerManager;
import main.Main;
import message.DataMessage;
import message.LoadMessage;
import message.UpdateInfoMessage;
import node.AddressNode;
import node.DataNode;
import node.Node;

public class SkipGraph extends AbstractDoubleLinkDistributedIndex  {

	private static final String NAME = "SkipGraph";
	public String getName() {return NAME;}
	private final static int MAX_LEVEL = 5;
	private final static int MV_LENGTH = 8;
	private ID id;
	private SkipGraphNode rootNode;
	private SkipGraphNode[] interNodes;
	private int[] m;
	private LocalStore store;


	public SkipGraph() {}
	public boolean adjustCmd(MessageSender sender) {return true;}
	public String getAdjustCmdInfo() {return "";}
	public String handleMessge(InetAddress host, ID id, String[] text) {
		return "";
	}

	public void initialize(ID id) {
		synchronized (this) {
			this.id = id;
			this.interNodes = new SkipGraphNode[MAX_LEVEL];
			SkipGraphNode childNode = null;
			for (int i = 0; i < MAX_LEVEL; i++) {
				SkipGraphNode node = new SkipGraphNode(i, null, null, childNode);
				this.interNodes[i] = node;
				childNode = node;
			}
			this.rootNode = childNode;
			this.m = new int[MV_LENGTH];
			for (int i = 0; i < MV_LENGTH; i++) {
				int r = Main.random.nextInt(2);
				this.m[i] = r;
			}
			this.store = new TreeLocalStore();
		}
	}



	public void initialize(DistributedIndex _distIndex, InetSocketAddress addr, ID id) {
		synchronized (this) {
			SkipGraph distIndex = (SkipGraph)_distIndex;
			this.id = distIndex.id;
			this.rootNode = distIndex.rootNode;
			this.interNodes = distIndex.interNodes;
			if (id.compareTo(this.id) < 0) {
				this.interNodes[0].neighborL = new AddressNode(addr, "0");
			}
			else {
				this.interNodes[0].neighborR = new AddressNode(addr, "0");
			}
			this.m = distIndex.m;
			this.store = distIndex.store;
		}
	}

	public SkipGraph toInstance(String[] text, ID id) {
		return SkipGraph._toInstance(text, id);
	}

	public static SkipGraph _toInstance(String[] text, ID id) {
		int i = 0;
		ID sgid = id.toInstance(text[i]);
		i++;
		SkipGraphNode[] interNodes = new SkipGraphNode[MAX_LEVEL];
		SkipGraphNode childNode = null;


		for (int j = 0; j < MAX_LEVEL; j++) {
			int n = Integer.parseInt(text[i]); i++;
			String[] temp = new String[n];
			System.arraycopy(text, i, temp, 0, n);
			SkipGraphNode node = SkipGraphNode._toInstance(temp, id); i += n;
			node.childNode = childNode;
			interNodes[j] = node;
			childNode = node;
		}
		SkipGraphNode rootNode = childNode;
		int[] m = new int[MV_LENGTH];
		for (int j = 0; j < MV_LENGTH; j++) {
			int n = Character.digit(text[i].charAt(j), 10);
			m[j] = n;
		} i++;
		// boolean deleteFlag;

		SkipGraph sg = new SkipGraph();
		sg.id = sgid;
		sg.rootNode = rootNode;
		sg.interNodes = interNodes;
		sg.m = m;
		// sg.deleteFlag = deleteFlag;

		String name = text[i]; i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		sg.store = TreeLocalStore._toInstance(temp, id); i += n;

		return sg;
	}

	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.id.toMessage() + " ");
		for (int i = 0; i < this.interNodes.length; i++) {
			sb.append(this.interNodes[i].toMessage() + " ");
		}
		for (int i = 0; i < this.m.length; i++) {
			sb.append(this.m[i]);
		}
		sb.append(" ");
		sb.append(this.store.toMessage());
		String msg = sb.toString();
		String[] temp = msg.split(" ");
		return NAME + " " + temp.length + " " + msg;
	}

	public String toAdjustInfo() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.m.length; i++) {
			sb.append(this.m[i]);
		}
		return sb.toString();
	}







	public AddressNode adjust(String text, ID id, InetSocketAddress addr, String info) {
		if (info == null) {
			String[] items = text.split(":");
			int s = Integer.parseInt(items[0]);
			int e = Integer.parseInt(items[1]);
			if (id.compareTo(this.id) < 0) {
				while (s <= e) {
					this.interNodes[s].neighborL = new AddressNode(addr, Integer.toString(s));
					s++;
				}
			}
			else {
				while (s <= e) {
					this.interNodes[s].neighborR = new AddressNode(addr, Integer.toString(s));
					s++;
				}
			}
			return null;
		}

		String[] items = text.split(":");
		int start = (items.length > 1) ? Integer.parseInt(items[1]) + 1 : Integer.parseInt(items[0]);
		int[] mv = new int[MV_LENGTH];
		for (int i = 0; i < MV_LENGTH; i++) {
			mv[i] = Character.digit(info.charAt(i), 10);
		}

		int level = start;
		if (id.compareTo(this.id) < 0) {
			for (; level < MAX_LEVEL; level++) {
				for (int i = 0; i < level; i++) {
					if (mv[i] != this.m[i]) {
						AddressNode temp = this.interNodes[level - 1].neighborR;
						return new AddressNode(temp.getAddress(), start + ":" + temp.getText());
					}
				}
				this.interNodes[level].neighborL = new AddressNode(addr, Integer.toString(level));
			}
		}
		else {
			for (; level < MAX_LEVEL; level++) {
				for (int i = 0; i < level; i++) {
					if (mv[i] != this.m[i]) {
						AddressNode temp = this.interNodes[level - 1].neighborL;
						return new AddressNode(temp.getAddress(), start + ":" + temp.getText());
					}
				}
				this.interNodes[level].neighborR = new AddressNode(addr, Integer.toString(level));
			}
		}

		return new AddressNode(null, start + ":" + (level - 1));
	}



	public InetSocketAddress[] getAckMachine() {
		ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		InetSocketAddress addr = this.interNodes[0].neighborL.getAddress();
		if (addr != null) addrs.add(addr);
		addr = this.interNodes[0].neighborR.getAddress();
		if (addr != null) addrs.add(addr);
		return addrs.toArray(new InetSocketAddress[0]);
	}


	public ID getID() {return this.id;}


	public ID[] getResponsibleRange(MessageSender sender) throws IOException {
		ID[] range = new ID[2];
		if (this.interNodes[0].neighborL.getAddress() != null) range[0] = this.id;
		InetSocketAddress rAddr = this.interNodes[0].neighborR.getAddress();
		if (rAddr != null) {
			String msg = "status id";
			String res = sender.sendAndReceive(msg, rAddr);
			range[1] = this.id.getID(res);
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
		return this.interNodes[0].neighborR.getAddress();
	}

	public InetSocketAddress getPrevMachine() {
		return this.interNodes[0].neighborL.getAddress();
	}

	public Node searchKey(MessageSender sender, ID key) throws IOException {
		return searchKey(sender, key, rootNode);
	}

	public Node searchKey(MessageSender sender, ID key, String text) throws IOException {
		if (text == null) {
			return searchKey(sender, key, rootNode);
		}
		else if (text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}
		else {
			int level = Integer.parseInt(text);
			return searchKey(sender, key, this.interNodes[level]);
		}
	}

	// public Node searchKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return searchKey(sender, key, start);
	// }

	private Node searchKey(MessageSender sender, ID key, SkipGraphNode start) throws IOException {
		String msg = "status id";
		if (key.compareTo(this.id) < 0) {
			SkipGraphNode current = start;
			InetSocketAddress lastAddr = null;
			ID lastId = null;

			while (current != null) {
				InetSocketAddress addr = current.neighborL.getAddress();
				if (addr == null) {
					current = current.childNode;
					continue;
				}
				ID resId;
				if (addr.equals(lastAddr)) {
					resId = lastId;
				}
				else {
					String res = sender.sendAndReceive(msg, addr);
					//System.err.println("MESSAGE status id");
					/*
					 * resは問い合わせ先のIDのString。
					 * getID(String)はファクトリメソッドで、
					 * AlphanumericIDのインスタンスを返す。
					 */
					resId = this.id.getID(res);
					/*
					 * よくわからないが、lastAddrが一番検索キーに近いところを担当している。
					 */
					lastAddr = addr;
					lastId = resId;
				}
				if (key.compareTo(resId) >= 0) {
					current = current.childNode;
				}
				else {
					int i = (current.level + 1 > start.level) ? start.level : current.level + 1;
					AddressNode neighbor = this.interNodes[i].neighborL;
					return (neighbor.getAddress() != null) ? neighbor : this.interNodes[i - 1].neighborL;
				}
			}



			if (interNodes[0].neighborL.getAddress() == null) {
				return this.store.searchKey(key);
			}
			else {
				return interNodes[0].neighborL;
			}
		}
		else {
			SkipGraphNode current = start;
			InetSocketAddress lastAddr = null;
			ID lastId = null;
			while (current != null) {
				InetSocketAddress addr = current.neighborR.getAddress();
				if (addr == null) {
					current = current.childNode;
					continue;
				}
				ID resId;
				if (addr.equals(lastAddr)) {
					resId = lastId;
				}
				else {
					String res = sender.sendAndReceive(msg, addr);
					//System.err.println("MESSAGE status id");
					resId = this.id.getID(res);
					lastAddr = addr;
					lastId = resId;
				}
				if (key.compareTo(resId) < 0) {
					current = current.childNode;
				}
				else {
					return current.neighborR;
				}
			}
			// if (interNodes[0].neighborR.getAddress() == null) {
			return this.store.searchKey(key);
			// }
			// else {
			// 	return interNodes[0].neighborR;
			// }
		}
	}

	public NodeStatus[] searchData(MessageSender sender, ID[] range) {
		// System.err.println("DEPLICATE SkipGraph#searchData(ID[])");
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
			if (range[1] != null && r[0].compareTo(range[1]) >= 0) {
				break;
			}
			NodeStatus s = node.searchData();
			status.add(s);
			node = this.store.getNextDataNode(node);
		}
		return status.toArray(new NodeStatus[0]);
	}

	public NodeStatus searchData(MessageSender sender, DataNode dataNode) {
		// System.err.println("DEPLICATE SkipGraph#searchData(DataNode)");
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
		return updateKey(sender, key, rootNode);
	}

	public Node updateKey(MessageSender sender, ID key, String text) throws IOException {
		if (text == null) {
			return updateKey(sender, key, rootNode);
		}
		else if (text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}
		else {
			int level = Integer.parseInt(text);
			return updateKey(sender, key, this.interNodes[level]);
		}
	}

	// public Node updateKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return updateKey(sender, key, start);
	// }

	public Node updateKey(MessageSender sender, ID key, SkipGraphNode start) throws IOException {
		String msg = "status id";
		if (key.compareTo(this.id) < 0) {
			SkipGraphNode current = start;
			InetSocketAddress lastAddr = null;
			ID lastId = null;
			while (current != null) {
				InetSocketAddress addr = current.neighborL.getAddress();
				if (addr == null) {
					current = current.childNode;
					continue;
				}
				ID resId;
				if (addr.equals(lastAddr)) {
					resId = lastId;
				}
				else {
					String res = sender.sendAndReceive(msg, addr);
					//System.err.println("MESSAGE status id");
					resId = this.id.getID(res);
					lastAddr = addr;
					lastId = resId;
				}
				if (key.compareTo(resId) >= 0) {
					current = current.childNode;
				}
				else {
					int i = (current.level + 1 > start.level) ? start.level : current.level + 1;
					AddressNode neighbor = this.interNodes[i].neighborL;
					return (neighbor.getAddress() != null) ? neighbor : this.interNodes[i - 1].neighborL;
				}
			}
			if (interNodes[0].neighborL.getAddress() == null) {
				return this.store.updateKey(key);
			}
			else {
				return interNodes[0].neighborL;
			}
		}
		else {
			SkipGraphNode current = start;
			InetSocketAddress lastAddr = null;
			ID lastId = null;
			while (current != null) {
				InetSocketAddress addr = current.neighborR.getAddress();
				if (addr == null) {
					current = current.childNode;
					continue;
				}
				ID resId;
				if (addr.equals(lastAddr)) {
					resId = lastId;
				}
				else {
					String res = sender.sendAndReceive(msg, addr);
					//System.err.println("MESSAGE status id");
					resId = this.id.getID(res);
					lastAddr = addr;
					lastId = resId;
				}
				if (key.compareTo(resId) < 0) {
					current = current.childNode;
				}
				else {
					return current.neighborR;
				}
			}
			// if (interNodes[0].neighborR.getAddress() == null) {
			return this.store.updateKey(key);
			// }
			// else {
			// 	return interNodes[0].neighborR;
			// }
		}
	}

	public NodeStatus[] updateData(MessageSender sender, ID[] range) {
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
		// System.err.println("DEPLICATE SkipGraph#updateData(DataNode)");
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
		if (id.compareTo(this.id) < 0) {
			range[1] = this.id;
		}
		else {
			range[0] = id;
		}
		return range;
	}

	public SkipGraph splitResponsibleRange(MessageSender sender, ID[] range, ID id, NodeStatus[] status, InetSocketAddress addr) {
		SkipGraph splitedIndex = new SkipGraph();

		if (range[0] == null && range[1] != null && range[1].compareTo(this.id) <= 0) {
			splitedIndex.initialize(id);
			splitedIndex.interNodes[0].neighborL = this.interNodes[0].neighborL;
			splitedIndex.store = this.store.splitResponsibleRange(range, status);

			// this.id = range[1];
			this.interNodes[0].neighborL = new AddressNode(addr, "0");
		}
		else if (range[0] == null && range[1] != null && range[1].compareTo(this.id) > 0) {
			splitedIndex.initialize(this.id);
			splitedIndex.interNodes[0].neighborL = this.interNodes[0].neighborL;
			splitedIndex.store = this.store.splitResponsibleRange(range, status);

			this.id = range[1];
			this.interNodes[0].neighborL = new AddressNode(addr, "0");
		}
		else if (range[0] != null && range[1] == null && range[0].compareTo(this.id) > 0) {
			splitedIndex.initialize(range[0]);
			splitedIndex.interNodes[0].neighborR = this.interNodes[0].neighborR;
			splitedIndex.store = this.store.splitResponsibleRange(range, status);

			// this.id = this.id;
			this.interNodes[0].neighborR = new AddressNode(addr, "0");
		}
		// else if (range[0] != null && range[1] != null && range[1].compareTo(this.id) <= 0) {
		// }
		else {
			System.err.println("ERROR SkipGraph#splitResponsibleRange");
		}

		return splitedIndex;
	}

	public String toString() {
		// synchronized (this) {
		StringBuilder sb = new StringBuilder();
		sb.append("SkipGraph: " + Shell.CRLF);
		sb.append("  ID: " + this.id + Shell.CRLF);
		sb.append("  LIST:" + Shell.CRLF);
		for (int i = MAX_LEVEL - 1; i >= 0; i--) {
			sb.append("  " + this.interNodes[i] + Shell.CRLF);
		}
		sb.append("  vector: ");
		for (int i = 0; i < MV_LENGTH; i++) {
			sb.append(this.m[i]);
		}
		// sb.append(Shell.CRLF + "  " + this.deleteFlag + Shell.CRLF);
		return sb.toString();
		// }
	}




	/*
	 * TODO
	 * データベースのデータを削除していないので、
	 * 時間が余ったらそこまでやります。
	 *
	 * DBConnector を分散手法側から使うようにすればよい思います。
	 *
	 */
	synchronized protected void updateIndex(
			DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target) {
		/*
		 * 送り手が左側に送る時は自分の担当IDを更新する
		 * 右側に送る時は何もしない。
		 */
		if(getPrevMachine()!=null&& getPrevMachine().equals(target)){
			pri("this.getPrevMachine().equals(target) is true");
		}
		if(dataNodesToBeRemoved[0].equals(getFirstDataNode())){
			pri("dataNodesToBeRemoved[0].equals(this.getFirstDataNode())");
		}
		if(equalsAddress(target, getPrevMachine())){
			pri("equalsAddress(target, this.getPrevMachine())");
		}
		if((getPrevMachine()!=null&& getPrevMachine().equals(target))
				|| dataNodesToBeRemoved[0].equals(getFirstDataNode()) //たぶん3つのうち２番目、３番目のチェックは必要ない
				|| equalsAddress(target, getPrevMachine())){
			pri("before responsible range minid:");
			pri(this.id.toString());
			((TreeLocalStore)this.store).setLeftmost(dataNodesToBeRemoved[dataNodesToBeRemoved.length-1].getNext());
			this.id = this.getFirstDataNode().getMinID();
			pri("before responsible range minid:");
			pri(this.id.toString());
		}

		priJap("データ削除前の自分の担当範囲:"+ this.id.toString());
		priJap("左端のデータノードのレンジ:"+getFirstDataNode().getMinID().toString()+" - "+getFirstDataNode().getMaxID().toString());
		if(getFirstDataNode().getNext()==null){
			priJap("左端から２番目のデータノードはありません。");
		}

		priJap("いまからループでデータを消していきます。");
		for(DataNode dn : dataNodesToBeRemoved){
			priJap("データ削除前のデータ数:"+getTotalDataSizeByB_link());
			priJap("削除するデータ数:"+dn.size());
			TreeNode parent = (TreeNode) dn.getParent();
			priJap("データ削除前親の持つデータ数:"+parent.getDataSize());
			try{
				priJap("親の持つdataプロパティ");
				for(ID id:parent.data){
					if(id!=null)
						pri(id.toString());
				}
				priJap("親の持つchildrenの種類");
				for(Node n: parent.children){
					if(n!=null)
						pri(n.getName());
				}
				priJap("親の持つ子供の数:"+parent.getChildrenSize());
				if(parent.equals(rootNode)){
					priJap("親はルートノードです");
				}
			}catch(Exception e){
				e.printStackTrace();
			}

			/*
			 * datanode がidを消すと
			 * その親にそれが伝達される
			 * 親は子の状態をみて削除するかどうかを判断する
			 * 同様に一番親まで処理が続く。
			 */
			for(ID id : dn.getAll()){
				pri("dn.remove");
				dn.remove(this.getSender(),id);
			}

			//parent.removeDataNode(dn);


			priJap("データ削除後のデータ数:"+getTotalDataSizeByB_link());
			priJap("データ削除後の親の持つデータ数:"+parent.getDataSize());
			priJap("対象とするデータノードのレンジ:"+dn.getMinID().toString()+" - "+dn.getMaxID().toString());
		}

		((TreeLocalStore)this.store).setFirstDataNode(((TreeLocalStore)this.store).searchFirstDataNode());
		this.id = getFirstDataNode().getMinID();
		priJap("データ削除前の自分の担当範囲:"+ this.id.toString());
		priJap("ループ終わり");
		priJap("左端のデータノードのレンジ:"+getFirstDataNode().getMinID().toString()+" - "+getFirstDataNode().getMaxID().toString());
		if(getFirstDataNode().getNext()==null){
			priJap("左端から２番目のデータノードはありません。");
		}
	}


	/*
	 * TODO
	 *
	 *
	 * 負荷分散のためのデータノード移動が起きた時に呼ばれます。
	 * この関数はデータノード受け取り用です。
	 *
	 * ここでデータノードを自分のインデックスの管理に入れます。
	 * 自分自身のインデックスを更新して何らかのメッセージを返します。
	 */
	/*@Override
	synchronized public String recieveAndUpdateDataForLoadMove(DataNode[] dataNodes,
			InetSocketAddress senderAddress) {

		priJap("METHOD : recieveAndUpdateDataForLoadMove関数が呼ばれました");
		priJap("受け取ったデータノードの数は");
		pri(dataNodes.length);
		priJap("受け取ったデータノードに含まれるキーの数はそれぞれ");
		int total=0;
		for(DataNode d : dataNodes){
			total+=d.size();
			pri(d.size());
		}


		priJap("受け取ったデータ総計:"+total);
		priJap("データの送り主のアドレスは");
		pri(senderAddress.getAddress().toString());
		priJap("データ追加前の総合データ数は");
		pri(getTotalDataSizeByB_link());

		boolean isAdded = false;

		TreeNode current = ((TreeLocalStore)this.store).getRoot();
		while(current instanceof TreeNode){

		}

		priJap("インデックス更新開始");
		for(DataNode dnToAdd: dataNodes){
			for(ID id: dnToAdd.getAll()){
				DataNode dn = store.updateKey(id);
				NodeStatus status = updateData(getSender(), dn);
				dn.add(getSender(), id);
				endUpdateData(getSender(), status);
			}
		}


		if(this.getPrevMachine() !=null && this.getPrevMachine().equals(senderAddress) ){
			priJap("左からデータノードを受け取りました");

			TreeLocalStore store = ((TreeLocalStore)this.store);
			TreeNode parent = (TreeNode)store.getFirstDataNode().getParent();
			isAdded = parent.addDataNodes(dataNodes);

			//インデックス手法からの参照を作る
			store.setFirstDataNode(dataNodes[0]);


		 * 受け手が左側からデータを受けとった時は
		 * 自分の担当範囲を更新する

			this.id = this.getFirstDataNode().getMinID();

		}else if(this.getNextMachine() != null && this.getNextMachine().equals(senderAddress)){
			priJap("右からデータノードを受け取りました");

			DataNode child = getRightMostDataNode();
			TreeNode parent = (TreeNode)child.getParent();
			isAdded = parent.addDataNodes(dataNodes);
		}

		priJap("データ追加後の総合データ数は");
		pri(getTotalDataSizeByB_link());

		//if(isAdded == false){
		//return "NO";
		//}

		priJap("テスト用に「OK」を返します");
		return "OK";
	}
*/



	@Override
	protected String updateIndexWhenReceivingData(DataMessage dataMessage) {
		DataNode[] dataNodes = dataMessage.getDataNodeMessage().getDataNodes();
		InetSocketAddress senderAddress = dataMessage.getDataNodeMessage().getSenderAddress();
		priJap("METHOD : recieveAndUpdateDataForLoadMove関数が呼ばれました");
		priJap("受け取ったデータノードの数:"+dataNodes.length);
		priJap("受け取ったデータノードに含まれるキーの数はそれぞれ");
		int total=0;
		for(DataNode d : dataNodes){
			total+=d.size();
			pri(d.size());
		}
		priJap("受け取ったデータ総計:"+total);
		priJap("データの送り主のアドレス:"+senderAddress.getAddress().toString());
		priJap("データ追加前の総合データ数:"+getTotalDataSizeByB_link());
		priJap("インデックス更新開始");

		for(DataNode dnToAdd: dataNodes){
			for(ID id: dnToAdd.getAll()){
				DataNode dn = store.updateKey(id);
				NodeStatus status = updateData(getSender(), dn);
				dn.add(getSender(), id);
				endUpdateData(getSender(), status);
			}
		}
		priJap("データ追加後の総合データ数:"+getTotalDataSizeByB_link());
		priJap("テスト用に「OK」を返します");
		return "OK";
	}



	@Override
	protected String updateIndexWhenReceivingUpdateInfo(
			UpdateInfoMessage updateInfoMessage) {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}



}







final class SkipGraphNode implements Node {
	private static final String NAME = "SkipGraphNode";
	public String getName() {return NAME;}

	public int level;
	public AddressNode neighborL;
	public AddressNode neighborR;
	public SkipGraphNode childNode;

	private int[] status;

	private SkipGraphNode(int level, AddressNode neighborL, AddressNode neighborR, SkipGraphNode childNode) {
		this.level = level;
		this.neighborL = neighborL;
		this.neighborR = neighborR;
		this.childNode = childNode;

		this.status = LatchUtil.newLatch();
	}

	public SkipGraphNode(int level, InetSocketAddress lAddr, InetSocketAddress rAddr, SkipGraphNode childNode) {
		this.level = level;
		this.neighborL = new AddressNode(lAddr, Integer.toString(this.level));
		this.neighborR = new AddressNode(rAddr, Integer.toString(this.level));
		this.childNode = childNode;

		this.status = LatchUtil.newLatch();
	}

	public static SkipGraphNode _toInstance(String[] text, ID id) {
		int i = 0;
		int level = Integer.parseInt(text[i]); i++;

		String name = text[i]; i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		AddressNode neighborL = AddressNode._toInstance(temp, id); i += n;

		name = text[i]; i++; n = Integer.parseInt(text[i]); i++;
		temp = new String[n]; System.arraycopy(text, i, temp, 0, n);
		AddressNode neighborR = AddressNode._toInstance(temp, id); i += n;

		SkipGraphNode node = new SkipGraphNode(level, neighborL, neighborR, null);

		return node;
	}

	public String toMessage() {
		String msg = level + " " + neighborL.toMessage() + " " + neighborR.toMessage();
		String[] temp = msg.split(" ");
		return temp.length + " " + msg; // NAME
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
		InetSocketAddress addr = neighborL.getAddress();
		if (addr != null) {
			sb.append(addr.getAddress().getHostAddress() + ":" + addr.getPort());
		}
		else {
			sb.append("null");
		}
		sb.append(" <-> ");
		addr = neighborR.getAddress();
		if (addr != null) {
			sb.append(addr.getAddress().getHostAddress() + ":" + addr.getPort());
		}
		else {
			sb.append("null");
		}
		return sb.toString();
	}
}
