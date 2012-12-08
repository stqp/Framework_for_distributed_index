package node;
// DataNode.java


import java.util.ArrayList;

import javax.xml.crypto.Data;

import util.ID;
import util.LatchUtil;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;

// note: not use synchronized (this) {} in each function
// will use alternate exclusion access control or not lock this at all
// range, prev, next is depend on a employed distributed index method
public class DataNode implements Node {
	public static final String NAME = "DataNode";
	public String getName() {return NAME;}

	private ArrayList<ID> idList;
	private Node parent;
	private ID[] range;
	private DataNode prev;
	private DataNode next;
	
	
	/*
	 * データノードへのアクセス負荷を蓄積する変数
	 */
	private int readCounter = 0;
	private int writeCounter = 0;
	
	/*
	 * 読み込みと書き込みの重みを付ける
	 */
	private static int readWeight = 1;
	private static int writeWeight = 2;
	
	public int getLoad(){
		return readCounter*readWeight + writeCounter*writeWeight;
	}
	
	public void resetLoadCounter(){
		this.writeCounter = 0;
		this.readCounter = 0;
	}
	
	
	/*
	 * ここで返すサイズを切り替えることで
	 * 容量負荷もしくはアクセス負荷を簡単に変えることができる
	 */
	public int size() {
		return this.idList.size();
	}
	
	public void incrementReadCount(){
		this.readCounter++;
	}
	
	public void incrementWriteCount(){
		this.writeCounter++;
	}
	
	


	private int[] status;

	public DataNode() {
		this.idList = new ArrayList<ID>();
		this.parent = null;
		this.range = null;
		this.prev = null;
		this.next = null;

		this.status = LatchUtil.newLatch();
	}

	public DataNode(Node parent, DataNode prev, DataNode next) {
		this.idList = new ArrayList<ID>();
		this.parent = parent;
		this.range = null;
		this.prev = prev;
		this.next = next;

		this.status = LatchUtil.newLatch();
	}

	
	
	
	/*
	 * データノードの等しい条件
	 */
	public boolean equals(Object o){
		if(o == null){return false;}
		
		if(o instanceof DataNode){
			if(((DataNode) o).getMinID().compareTo(this.getMinID())==0
					&& ((DataNode) o).getMaxID().compareTo(this.getMaxID())==0){
				return true;
			}
		}
		return false;
	}
	
	
	/*
	 * text
	 * by tokuda
	 */
	public static DataNode toInstance(String[] text, ID id) {
		DataNode dataNode = new DataNode();
		for (String t: text) {
			dataNode.idList.add(id.toInstance(t));
		}
		return dataNode;
	}

	
	
	
	
	
	
	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		for (ID id: this.idList) {
			sb.append(id.toMessage() + " ");
		}
		if (sb.length() > 0) sb.delete(sb.length() - 1, sb.length());
		return NAME + " " + idList.size() + " " + sb.toString();
	}

	public void ackUpdate(MessageSender sender, Node node) {
		System.err.println("WARNING DataNode#ackUpdate");
	}

	// note: need exclusion access control
	public NodeStatus searchData() {
		//this.readCounter++;
		// while (true) {
		for (int i = 0; i < 50; i++) {
			synchronized (this.status) {
				if (this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.S]++;
					// break;
					return new NodeStatus(this, LatchUtil.S);
				}
			}
			//
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
			//
		}
		synchronized (this.status) {
			this.status[LatchUtil.X] = 0;
			this.status[LatchUtil.S] = 1;
		}
		return new NodeStatus(this, LatchUtil.S);
	}

	
	// note: need exclusion access control
	public void endSearchData(NodeStatus status) {
		synchronized (this.status) {
			this.status[status.getType()]--;
			if (this.status[status.getType()] < 0) this.status[status.getType()] = 0;
		}
	}

	// note: need exclusion access control
	public NodeStatus updateData() {
		//this.writeCounter++;
		// while (true) {
		for (int i = 0; i < 50; i++) {
			synchronized (this.status) {
				if (this.status[LatchUtil.S] == 0 && this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.X]++;
					// break;
					return new NodeStatus(this, LatchUtil.X);
				}
			}
			//
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
			//
		}
		synchronized (this.status) {
			this.status[LatchUtil.S] = 0;
			this.status[LatchUtil.X] = 1;
		}
		return new NodeStatus(this, LatchUtil.X);
	}

	// note: need exclusion access control
	public void endUpdateData(NodeStatus status) {
		synchronized (this.status) {
			this.status[status.getType()]--;
			if (this.status[status.getType()] < 0) this.status[status.getType()] = 0;
		}
	}

	//
	// note: need exclusion access control
	//

	public boolean add(MessageSender sender, ID id) {
		//this.incrementWriteCount();
		boolean res = this.idList.add(id);
		if (res) {
			this.parent.ackUpdate(sender, this);
		}
		return res;
	}
	
	public boolean add(ID id) {
		//this.incrementWriteCount();
		boolean res = this.idList.add(id);
		return res;
	}

	public boolean addAll(MessageSender sender, ArrayList<ID> idList) {
		//this.writeCounter += idList.size();
		boolean res = this.idList.addAll(idList);
		if (res) {
			this.parent.ackUpdate(sender, this);
		}
		return res;
	}

	public boolean contains(ID id) {
		return this.idList.contains(id);
	}

	public boolean remove(MessageSender sender, ID id) {
		boolean res = this.idList.remove(id);
		if (res) {
			this.parent.ackUpdate(sender, this);
		}
		return res;
	}

	
	
	
	
	
	
	
	public DataNode split() {
		ID[] idArray = this.getSortedArray();
		int center = idArray.length / 2;

		ID[] forward = java.util.Arrays.copyOfRange(idArray, 0, center);
		this.idList = new ArrayList<ID>(java.util.Arrays.asList(forward));

		DataNode dataNode = new DataNode(this.parent, this, this.next);
		ID[] last = java.util.Arrays.copyOfRange(idArray, center, idArray.length);
		dataNode.idList = new ArrayList<ID>(java.util.Arrays.asList(last));

		this.next = dataNode;
		
		dataNode.readCounter = this.readCounter/2;
		this.readCounter = this.readCounter/2;
		
		dataNode.writeCounter = this.writeCounter/2;
		this.writeCounter = this.writeCounter/2;

		return dataNode;
	}

	public ID[] getAll() {
		return this.idList.toArray(new ID[0]);
	}

	public Node getParent() {return this.parent;}
	public Node setParent(Node parent) {
		Node old = this.parent;
		this.parent = parent;
		return old;
	}

	public ID[] getRange() {return this.range;}
	public ID[] setRange(ID[] range) {
		ID[] old = this.range;
		this.range = new ID[2];
		this.range[0] = range[0];
		this.range[1] = range[1];
		return old;
	}

	public DataNode getPrev() {return this.prev;}
	public DataNode setPrev(DataNode prev) {
		DataNode old = this.prev;
		this.prev = prev;
		return old;
	}

	public DataNode getNext() {return this.next;}
	public DataNode setNext(DataNode next) {
		DataNode old = this.next;
		this.next = next;
		return old;
	}

	private ID[] getSortedArray() {
		ID[] idArray = this.idList.toArray(new ID[0]);
		java.util.Arrays.sort(idArray);
		return idArray;
	}

	public ID getMinID() {
		ID[] idArray = this.getSortedArray();
		return (idArray.length != 0) ? idArray[0] : null;
	}

	public ID getMaxID() {
		ID[] idArray = this.getSortedArray();
		return (idArray.length != 0) ? idArray[idArray.length - 1] : null;
	}

	public String toLabel() {
		return NAME +
				((this.range[0] != null) ? this.range[0].toMessage() : "") + "," +
						((this.range[1] != null) ? this.range[1].toMessage() : "");
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("DataNode:");
		if (this.range != null) {
			sb.append(" ");
			if (this.range[0] != null) sb.append(this.range[0] + " ");
			sb.append("<->");
			if (this.range[1] != null) sb.append(" " + this.range[1]);
		}
		sb.append(Shell.CRLF);
		sb.append("  size: " + this.idList.size() + Shell.CRLF);
		sb.append("  status: ");
		for (int i = 0; i < status.length; i++) {
			sb.append(status[i] + ", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		return sb.toString();
	}
}
