package store;

import node.AddressNode;
import node.DataNode;
import node.Node;
import util.ID;
import util.LatchUtil;
import util.MessageSender;
import util.MyUtil;
import util.NodeStatus;
import util.Shell;


public final class TreeNode extends MyUtil implements Node {
	private static final String NAME = "TreeNode";
	public String getName() {return NAME;}

	private static final int MAX_CHILDREN_NODES = 64;
	private static final int MAX_ID_PER_DATANODE = 32;

	private TreeLocalStore store;
	public TreeNode parent;
	public ID[] data;		// last member is always null
	public Node[] children;
	// public TreeNode prev;
	// public TreeNode next;

	private int[] status;

	public TreeNode(TreeLocalStore store) {
		this.store = store;
		this.parent = null;
		this.data = new ID[MAX_CHILDREN_NODES];
		this.children = new Node[MAX_CHILDREN_NODES + 1];
		// this.prev = null;
		// this.next = null;

		this.status = LatchUtil.newLatch();
	}

	// public TreeNode(TreeNode parent, TreeNode prev, TreeNode next) {
	// 	this.parent = parent;
	// 	this.data = new ID[MAX_CHILDREN_NODES];
	// 	this.children = new Node[MAX_CHILDREN_NODES];
	// 	this.prev = prev;
	// 	this.next = next;

	// 	this.status = LatchUtil.newLatch();
	// }

	
	
	
	
	
	

	/*
	 * @author tokuda
	 * add new code.
	 * start.
	 */
	
	public DataNode getFirstDataNode(){
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode){
				return (DataNode) this.children[i];
			}
		}
		return null;
	}
	
	public DataNode getLastDataNode(){
		DataNode lastDn = null;
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode){
				lastDn = (DataNode)children[i];
			}
		}
		return lastDn;
	}
	
	/*
	 * B-linkの構造を使ってデータノードをループしてデータサイズを取得します
	 */
	public int getDataSizeByB_link(){
		int count = 0;
		DataNode dn = getFirstDataNode();
		if(dn == null)return 0;
		
		while(!dn.equals(getLastDataNode())){
			count+= dn.size();
			dn = dn.getNext();
		}
		count+= dn.size();
		return count;
	}
	
	public int getDataSize(){
		int count=0;
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode){
				count+= ((DataNode)this.children[i]).size();
			}
		}
		return count;
	}
	
	
	public int getNumberOfLeafNode(){
		int count=0;
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode){
				count++;
			}
		}
		return count;
	}
	
	public int getNumberOfInterNodes(){
		int count = 0;
		for(int i=0; i< this.getChildrenSize();i++){
			if(this.children[i] instanceof TreeNode){
				count++;
			}
		}
		return count;
	}
	
	
	
	/*
	 * childrenに渡された配列の中身をコピーしていきます。
	 * 単純にポインタを入れ替えるだけではありません。
	 */
	public boolean replaceChildren(Node[] childreToReplace){
		if(childreToReplace.length > MAX_CHILDREN_NODES) return false;
		for(int i=0; i< childreToReplace.length;i++){
			children[i] = childreToReplace[i];
		}
		for(int i=childreToReplace.length; i< MAX_CHILDREN_NODES;i++){
			children[i] = null;
		}
		return true;
	}
	
	
	
	public void removeDataNode(DataNode dn){
		Node[] newChildren = new Node[getChildrenSize()-1];//親の子供を新しい子供に置き換えます
		for(int i=0,j=0;i < getChildrenSize();i++){
			/*
			 * 目的のデータノードはコピーしないでとばす。
			 * ポインタで判別しようとしたらうまくいってなかったらしい。(parent.chidlren[i] == dn　のような感じ)
			 * そこで担当IDが同じということで判別する。
			 */
			if(dn.equals(children[i])== true){
				//削除するデータノードの前後のB-linkを更新します。
				if(dn.getPrev()!=null && dn.getNext()!=null){
					dn.getPrev().setNext(dn.getNext());
					dn.getNext().setPrev(dn.getPrev());
				}else if(dn.getPrev()==null && dn.getNext()!=null){
					dn.getNext().setPrev(null);
				}else if(dn.getPrev()!=null && dn.getNext()==null){
					dn.getPrev().setNext(null);
				}else if(dn.getPrev()==null && dn.getNext()==null){
					//do nothing.
				}
				
			}else{
				//データノードが見つからなかった場合
				if(j == getChildrenSize()-1){
					return ;
				}
				newChildren[j] = children[i]; //error : ArrayIndexOutOfBoundsException
				j++;
			}
		}
		//親から子への参照を取り除く
		replaceChildren(newChildren);
	}
	
	
	
	/*
	 * クラス変数のchildrenを、 渡された配列dnsをchildrenに追加して作った新しい配列に置き換えます
	 * ただしdnsには担当範囲が連続し整列された（担当ID的に）配列を渡してください。
	 * 
	 * データノードの追加場所（データノード的に左側OR右側）は勝手に判断します。
	 * またB-link構造も追加にあわせて更新します。
	 * 
	 * このメソッドは負荷分散のためにデータノードを移動するので、そのために追加しました。
	 * 
	 * ##### caution #####
	 * DataNodeを１つも持たない場合は使用しないでください。
	 * DataNodeが１つはないとB-linkに追加したDataNodeを割り込ませることができません
	 * 解決方法としては親に尋ねるという方法があります。
	 */
	public void addDataNodes(DataNode[] dns){
		
		//追加するデータノード間の参照を作る。親ノードへの参照を作る
		for(int i=0;i< dns.length ; i++){
			dns[i].setParent(this);
			
			if(dns.length == 1){break;}
			if(i==0){
				dns[i].setNext(dns[i+1]);
			}else if(i== dns.length-1){
				dns[i].setPrev(dns[i-1]);
			}else{
				dns[i].setNext(dns[i+1]);
				dns[i].setPrev(dns[i-1]);
			}
		}
		
		DataNode rightMostDataNode = null;
		boolean addToRightMost= false;
		boolean isFirst = true;
		
		
		for(int i=0; i< getChildrenSize();i++){
			if(children[i] instanceof DataNode){
				DataNode dnc = (DataNode)children[i];
				/*
				 * 子供の左はしに入れるか右端に入れるか判断します。
				 */
				/*
				 * 左端に入れるとき
				 * B-Link構造を維持するために
				 * もともと一番左にあったデータノードのさらに前へのリンクがあれば
				 * そのリンクを追加するデータノードの一番左へと更新する
				 * また、一番左にあったデータノードの前へのリンクを追加するデータノードの一番右へ更新する。
				 */
				if(isFirst == true){
					if(dns[dns.length-1].getMaxID().compareTo(dnc.getMinID()) < 0){
						if(dnc.getPrev() != null){
							dnc.getPrev().setNext(dns[0]);
							dns[0].setPrev(dnc.getPrev());
						}
						dns[dns.length-1].setNext(dnc);
						dnc.setPrev(dns[dns.length-1]);
						/*
						 * childrenの中の一番右のデータノードを後で使いたいのでループを抜けないように
						 * してください。
						 * don't break here!
						 */
					}
					//右端に入れるとき
					//右端がわからないのでここでは何もしない
					//ループを抜けてから処理します。
					else{
						addToRightMost = true;
					}
					isFirst = false;
				}
				
				rightMostDataNode = dnc;
			}
		}
		//右端に入れるとき
		if(addToRightMost == true){
			if(rightMostDataNode.getNext() != null){
				rightMostDataNode.getNext().setPrev(dns[dns.length-1]);
				dns[dns.length-1].setNext(rightMostDataNode.getNext());
			}
			dns[0].setPrev(rightMostDataNode);
			rightMostDataNode.setNext(dns[0]);
		}
		
		
		
		//childrenをdataNodeを追加した新しいもの(newChildren)に置き換えます。
		Node[] newChildren = new Node[getChildrenSize()+dns.length];
		
		if(addToRightMost == false){
			for(int i=0,j=0;i < newChildren.length;i++){
				if(i < dns.length){
					newChildren[i] = dns[i];
				}else{
					newChildren[i] = children[j];
					j++;
				}
			}
		}else{
			for(int i=0,j=0;i < newChildren.length;i++){
				if(i < getChildrenSize()){
					newChildren[i] = children[i];
				}else{
					newChildren[i] = dns[j];
					j++;
				}
			}
		}
		
		replaceChildren(newChildren);
	}
	
	
	/*
	 * @author tokuda
	 * end
	 * 
	 */
	
	
	
	
	
	
	
	
	public Node toInstance(String[] text, ID id) {return null;}
	public String toMessage() {return null;}


	public ID[] getRange(Node node) {
		for (int i = 0; i < MAX_CHILDREN_NODES; i++) {
			if (node == this.children[i]) {
				ID[] range = new ID[2];
				range[0] = (i > 0) ? this.data[i - 1] : null;
				range[1] = this.data[i];
				return range;
			}
		}
		return null;
	}

	public int getChildrenSize() {
		int i = 0;
		for (i = 0; i < this.MAX_CHILDREN_NODES; i++) {
			if (this.children[i] == null) break;
		}
		return i;
	}

	// public void ackSearch(Node node) {
	// }

	private TreeNode split() {
		TreeNode treeNode = new TreeNode(this.store);
		treeNode.parent = this.parent;
		/*
		 * "/"の演算では切り捨て
		 * 5 / 2 -> 2
		 * 4 / 2 -> 2
		 *
		 * this.dataの後半分を新しいtreeNodeにコピー
		 * dataはidの配列
		 */
		int center = MAX_CHILDREN_NODES / 2;
		for (int i = 0, j = center; j < MAX_CHILDREN_NODES; i++, j++) {
			treeNode.data[i] = this.data[j];
			this.data[j] = null;
		}
		this.data[center - 1] = null;


		/*
		 * childrenはNodeの配列
		 * 半分を新しいtreeNodeにコピー
		 */
		for (int i = 0, j = center; j < MAX_CHILDREN_NODES + 1; i++, j++) {

			treeNode.children[i] = this.children[j];
			this.children[j] = null;

			/*
			 * 子から親へリンクを張る。
			 */
			if (treeNode.children[i] instanceof DataNode) {
				DataNode dataNode = (DataNode)treeNode.children[i];
				dataNode.setParent(treeNode);
			}
			else {
				TreeNode temp = (TreeNode)treeNode.children[i];
				temp.parent = treeNode;
			}
		}

		return treeNode;
	}


	public void ackUpdate(MessageSender sender, Node childrenNode) {
		Node splitedNode = null;

		/*
		 *
		 */
		if (childrenNode instanceof DataNode) {
			DataNode dataNode = (DataNode)childrenNode;
			int size = dataNode.size();
			if (size <= MAX_ID_PER_DATANODE) return;
			splitedNode = dataNode.split();
		}
		else {
			TreeNode treeNode = (TreeNode)childrenNode;
			if (treeNode.data[MAX_CHILDREN_NODES - 1] == null) return;
			splitedNode = treeNode.split();
		}

		int index = 0;
		for (int i = MAX_CHILDREN_NODES - 1; i >= 1; i--) {
			if (this.children[i] == childrenNode) {
				index = i;
				break;
			}
			this.data[i] = this.data[i - 1];
			this.children[i + 1] = this.children[i];
		}

		this.data[index] =
				(splitedNode instanceof DataNode) ?
						((DataNode)splitedNode).getMinID() :
							((TreeNode)splitedNode).data[0];
						this.children[index + 1] = splitedNode;


						if (this.children[MAX_CHILDREN_NODES] != null) {
							if (this.parent != null) {
								this.parent.ackUpdate(sender, this);
							}
							else {
								TreeNode treeNode = this.split();
								TreeNode root = new TreeNode(this.store);
								root.data[0] = treeNode.data[0];
								root.children[0] = this;
								root.children[1] = treeNode;
								this.parent = root;
								treeNode.parent = root;
								this.store.setRoot(root);
							}
						}
	}

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
			if (this.status[status.getType()] < 0) this.status[status.getType()] = 0;
		}
	}


	public NodeStatus updateData() {
		// while (true) {
		for (int i = 0; i < 1000; i++) {
			synchronized (this.status) {
				if (this.status[LatchUtil.S] == 0 && this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.X]++;
					// break;
					return new NodeStatus(this, LatchUtil.X);
				}
			}
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
			}
		}
		synchronized (this.status) {
			this.status[LatchUtil.S] = 0;
			this.status[LatchUtil.X] = 1;
		}
		return new NodeStatus(this, LatchUtil.X);
	}

	public void endUpdateData(NodeStatus status) {
		synchronized (this.status) {
			this.status[status.getType()]--;
			if (this.status[status.getType()] < 0) this.status[status.getType()] = 0;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(NAME + ": " + this.hashCode() + Shell.CRLF);
		sb.append("  parent: " + ((this.parent != null) ? this.parent.hashCode() : null) + Shell.CRLF);
		sb.append("  data: ");
		for (int i = 0; i < this.data.length; i++) {
			sb.append(this.data[i] + ", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append(Shell.CRLF + "  children: ");
		for (int i = 0; i < this.children.length; i++) {
			sb.append(((this.children[i] != null) ? this.children[i].hashCode() : null) + ", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		return sb.toString();
	}
}

