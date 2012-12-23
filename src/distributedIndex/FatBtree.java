package distributedIndex;
// FatBtree.java


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;


import com.google.gson.Gson;

import store.TreeNode;
import util.ID;
import util.LatchUtil;
import util.MessageReceiver;
import util.MessageSender;
import util.MyUtil;
import util.NodeStatus;
import util.Shell;


import loadBalance.LoadInfoTable;
import log_analyze.AnalyzerManager;
import main.Main;
import message.DataMessage;
import message.LoadMessage;
import message.Message;
import message.UpdateInfoMessage;
import node.AddressNode;
import node.DataNode;
import node.Node;

public class FatBtree extends AbstractDoubleLinkDistributedIndex {


	private static final String NAME = "FatBtree";
	public String getName() {return NAME;}

	private ID[] range;
	private FBTNode root;
	protected DataNode leftmost;
	protected HashMap<String,Node> fbtNodes; // Node.toLabel(), Node instance (FBTNode, AddressNode, DataNode)
	protected InetSocketAddress nextMachine;

	//protected InetSocketAddress myAddress;

	private FBTNode lock = new FBTNode(null);

	public FatBtree() {}

	public boolean adjustCmd(MessageSender sender) {return true;}
	public String getAdjustCmdInfo() {return "";}



	public String handleMessge(InetAddress host, ID id, String[] text) {
		if (text[0].compareTo("replace") == 0) {
			// replace port self label message
			InetSocketAddress remote = new InetSocketAddress(host, Integer.parseInt(text[1]));
			String[] items = text[2].split(":");
			InetAddress selfHost = null;
			try {
				selfHost = InetAddress.getByName(items[0]);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			InetSocketAddress selfAddr = new InetSocketAddress(selfHost, Integer.parseInt(items[1]));
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[3]);
			}
			if (fbtNode == null) {
				System.err.println("WARNING FatBtree#handleMessage replace");
				return "delete share _self_" + Shell.CRLF;
			}
			HashMap<String,Node> deleteNode = new HashMap<String,Node>();
			deleteFBTNodeChildren(this, fbtNode, null, deleteNode);
			deleteNode.put(fbtNode.toLabel(), fbtNode);
			fbtNode.deleteFlag = true;
			//String name = text[4];
			int n = Integer.parseInt(text[5]);
			String[] temp = new String[n];
			System.arraycopy(text, 6, temp, 0, n);
			FatBtree fbt = new FatBtree();
			ID[] fbtRange = new ID[2];
			fbtRange[0] = this.range[0];
			fbtRange[1] = this.range[1];
			fbt.range = fbtRange;
			fbt.fbtNodes = new HashMap<String,Node>();
			FBTNode recvFBTNode = FBTNode._toInstance(temp, id, fbt, this);
			fbt.root = recvFBTNode;
			fbt.leftmost = null;
			fbt.fbtNodes = new HashMap<String,Node>();
			fbt.nextMachine = null;
			fbt.prevMachine = null;
			HashMap<String,Node> deleteNodeAdd = new HashMap<String,Node>();
			adjustIndex(fbt, fbt.root, null, remote, selfAddr, deleteNodeAdd);
			fbt.root.deleteFlag = false; // DEBUG
			if (fbtNode.parent == null) {
				this.root = fbt.root;
			}
			else {
				FBTNode fn = fbtNode.parent;
				for (int i = 0; i < fn.children.length - 1; i++) {
					if (fn.children[i] == fbtNode) {
						fn.children[i] = fbt.root;
						fbt.root.parent = fn;
						break;
					}
				}
				fn.deleteFlag = false;
			}
			synchronized (this.fbtNodes) {
				for (String label: deleteNode.keySet()) {
					this.fbtNodes.remove(label);
				}
				// this.fbtNodes.putAll(fbt.fbtNodes);
				//
				for (String label: fbt.fbtNodes.keySet()) {
					Node nd = fbt.fbtNodes.get(label);
					if (nd instanceof FBTNode) {
						FBTNode fnd = (FBTNode)nd;
						fnd.deleteFlag = false;
					}
				}
				this.fbtNodes.putAll(fbt.fbtNodes);
				//
			}
			//
			NodeStatus s = new NodeStatus(this.lock, LatchUtil.X);
			endUpdateData(null, s);
			//
			StringBuilder sb = new StringBuilder();
			sb.append("delete share ");
			for (String label: deleteNodeAdd.keySet()) {
				sb.append(label + " ");
			}
			sb.delete(sb.length() - 1, sb.length());
			sb.append(Shell.CRLF);
			return sb.toString();
		}
		else if (text[0].compareTo("latchXTree") == 0) {
			// latchXTree port label
			System.err.println("MESSAGE latchXTree");
			if (Thread.activeCount() > 16) return "false" + Shell.CRLF;
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[2]);
			}
			if (fbtNode == null) {
				System.err.println("WARNING FatBtree#handleMessage latchXTree " + text[2]);
				return "true0" + Shell.CRLF;
				// return "true" + Shell.CRLF;
			}
			NodeStatus s = this.lock.latchXNode();
			if (s == null) return "false" + Shell.CRLF;
			NodeStatus status = fbtNode.latchXNode();
			if (status == null) {
				endUpdateData(null, s);
				return "false" + Shell.CRLF;
			}
			boolean flag = checkXTree(fbtNode, true);
			for (int i = 0; flag == false && i < 50; i++) {
				try {Thread.sleep(100);}
				catch (InterruptedException e) {}
				flag = checkXTree(fbtNode, true);
			}
			if (flag) {
				return "true" + Shell.CRLF;
			}
			else {
				endUpdateData(null, status);
				endUpdateData(null, s);
				return "false" + Shell.CRLF;
			}
		}
		else if (text[0].compareTo("unlatchXTree") == 0) {
			System.err.println("MESSAGE unlatchXTree");
			// latchXTree port label [lock]
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[2]);
			}
			// if (fbtNode == null) {
			// 	System.err.println("WARNING FatBtree#handleMessage unlatchXTree" + text[2]);
			// 	return "true" + Shell.CRLF;
			// }
			NodeStatus s = new NodeStatus(this.lock, LatchUtil.X);
			endUpdateData(null, s);
			endUpdateData(null, s); // DEBUG
			if (text.length == 3 && fbtNode != null) {
				// if (text.length == 3) {
				NodeStatus status = new NodeStatus(fbtNode, LatchUtil.X);
				endUpdateData(null, status);
				endUpdateData(null, status); // DEBUG
			}
			return "true" + Shell.CRLF;
		}
		// else if (text[0].compareTo("add") == 0) {
		//     // add port self label index range0 range1 node
		//     InetSocketAddress remote = new InetSocketAddress(host, Integer.parseInt(text[1]));
		// String[] items = text[2].split(":");
		// InetAddress selfHost = null;
		// try {
		// 	selfHost = InetAddress.getByName(items[0]);
		// }
		// catch (IOException e) {
		// 	e.printStackTrace();
		// }
		// InetSocketAddress selfAddr = new InetSocketAddress(selfHost, Integer.parseInt(items[1]));
		//     FBTNode fbtNode = (FBTNode)this.fbtNodes.get(text[3]);
		//     if (fbtNode == null) {
		//     	System.err.println("WARNING FatBtree#handleMessage add");
		//     	return "";
		//     }
		//     return "";
		// }
		else if (text[0].compareTo("move") == 0) {
			// move port label index addrNode
			// move FBTNode or DataNode to an other machine
			InetSocketAddress remote = new InetSocketAddress(host, Integer.parseInt(text[1]));
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[2]);
			}
			if (fbtNode == null) {
				System.err.println("WARNING FatBtree#handleMessage move");
				return "delete share _self_" + Shell.CRLF;
			}
			int index = Integer.parseInt(text[3]);
			String name = text[4];
			int n = Integer.parseInt(text[5]);
			String[] temp = new String[n];
			System.arraycopy(text, 6, temp, 0, n);
			if (name.compareTo("AddressNode") == 0) {
				AddressNode addrNode = AddressNode._toInstance(temp, null);
				if (fbtNode.children[index] instanceof FBTNode) {
					((FBTNode)fbtNode.children[index]).shareAddress.remove(remote);
				}
				else if (fbtNode.children[index] instanceof AddressNode) {
					if (((AddressNode)fbtNode.children[index]).getAddress().equals(remote)) {
						synchronized (this.fbtNodes) {
							this.fbtNodes.remove(((AddressNode)fbtNode.children[index]).toLabel());
							this.fbtNodes.put(addrNode.toLabel(), addrNode);
						}
						fbtNode.children[index] = addrNode;
					}
				}
				return Shell.CRLF;
			}
			System.err.println("ERROR FatBtree#handleMessage move");
			return Shell.CRLF;
		}
		else if (text[0].compareTo("share") == 0) {
			// share port label address
			InetSocketAddress remote = new InetSocketAddress(host, Integer.parseInt(text[1]));
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[2]);
			}
			if (fbtNode == null) {
				System.err.println("WARNING FatBtree#handleMessage share");
				return "delete share _self_" + Shell.CRLF;
			}
			String[] items = text[3].split(":");
			InetAddress h = null;
			try {
				h = InetAddress.getByName(items[0]);
			}
			catch (IOException e) {
				e.printStackTrace();
				return Shell.CRLF;
			}
			InetSocketAddress addr = new InetSocketAddress(h, Integer.parseInt(items[1]));
			fbtNode.shareAddress.add(addr);
			return Shell.CRLF;
		}
		else if (text[0].compareTo("unshare") == 0) {
			// unshare port label [addr]
			InetSocketAddress remote = new InetSocketAddress(host, Integer.parseInt(text[1]));
			FBTNode fbtNode;
			synchronized (this.fbtNodes) {
				fbtNode = (FBTNode)this.fbtNodes.get(text[2]);
			}
			if (fbtNode == null) {
				System.err.println("WARNING FatBtree#handleMessage unshare");
				return "delete share _self_" + Shell.CRLF;
				// return Shell.CRLF;
			}
			if (text.length == 4) {
				String[] items = text[3].split(":");
				try {
					InetAddress host0 = InetAddress.getByName(items[0]);
					InetSocketAddress addr = new InetSocketAddress(host0, Integer.parseInt(items[1]));
					fbtNode.shareAddress.remove(addr);
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				fbtNode.shareAddress.remove(remote);
			}
			return Shell.CRLF;
		}
		System.err.println("WARNING FatBtree#handleMessage not message");
		return Shell.CRLF;
	}

	public void initialize(ID id) {
		synchronized (this) {
			this.range = new ID[2];
			this.root = new FBTNode(this);
			DataNode dataNode = new DataNode(this.root, null, null);
			dataNode.setRange(this.range);
			this.leftmost = dataNode;
			this.root.children[0] = dataNode;
			this.fbtNodes = new HashMap<String,Node>();
			this.fbtNodes.put(this.root.toLabel(), this.root);
			this.fbtNodes.put(dataNode.toLabel(), dataNode);
			this.nextMachine = null;
			this.prevMachine = null;

		}
	}

	public void initialize(DistributedIndex _distIndex, InetSocketAddress addr, ID id) {
		System.out.println("HOST NAME" + addr.getHostName());
		synchronized (this) {
			FatBtree distIndex = (FatBtree)_distIndex;
			this.range = distIndex.range;
			this.root = distIndex.root;
			this.leftmost = null;
			this.fbtNodes = new HashMap<String,Node>();
			this.nextMachine = null;
			this.prevMachine = null;
			HashMap<String,Node> deleteNode = new HashMap<String,Node>();
			adjustIndex(this, this.root, null, addr, null, deleteNode);
		}
	}

	private boolean checkXTree(FBTNode fbtNode, boolean isRoot) {
		if (fbtNode.deleteFlag == true) {
			return true;
		}
		if (isRoot == false) {
			if (fbtNode.isFree() == false) {
				return false;
			}
		}
		for (int i = 0; i < fbtNode.children.length - 1; i++) {
			Node node = fbtNode.children[i];
			if (node == null) {
				break;
			}
			if (node instanceof FBTNode) {
				FBTNode fnode = (FBTNode)node;
				return checkXTree(fnode, false);
			}
			else if (node instanceof AddressNode) {
			}
			else if (node instanceof DataNode) {
			}
			else {
				System.err.println("ERROR FatBtree#checkTree");
			}
		}
		return true;
	}

	/*
	 * remove all items in FBTNode's children.
	 * TODO delete
	 */
	private AddressNode deleteFBTNodeChildren(FatBtree fbt, FBTNode fbtNode, DataNode current, HashMap<String,Node> deleteNode) {
		// if (fbtNode.deleteFlag == true) return null;
		AddressNode toAddr = null;

		for (int i = 0; i < fbtNode.children.length - 1; i++) {
			Node node = fbtNode.children[i];
			if (node == null) {
				break;
			}
			if (node instanceof FBTNode) {
				FBTNode fnode = (FBTNode)node;
				deleteNode.put(fnode.toLabel(), fnode);
				fnode.deleteFlag = true;
				AddressNode anode = deleteFBTNodeChildren(fbt, fnode, current, deleteNode);//FBTNodeの時はその子供に対して再帰的に削除処理を行います。
				if (current == null) toAddr = anode;
				else if (/* current != null && */toAddr == null) toAddr = anode;
			}
			else if (node instanceof AddressNode) {
				AddressNode anode = (AddressNode)node;
				deleteNode.put(anode.toLabel(), anode);
				if (current == null) toAddr = anode;
				else if (/* current != null && */toAddr == null) toAddr = anode;
			}
			else if (node instanceof DataNode) {
				DataNode dnode = (DataNode)node;
				deleteNode.put(dnode.toLabel(), dnode);
			}
			else {
				System.err.println("ERROR FatBtree#deleteFBTNodeChildren");
			}
		}
		return toAddr;
	}

	private DataNode adjustIndex(FatBtree fbt, FBTNode fbtNode, DataNode current, InetSocketAddress addr, InetSocketAddress selfAddr, HashMap<String,Node> deleteNode) {

		fbt.fbtNodes.put(fbtNode.toLabel(), fbtNode);
		fbtNode.deleteFlag = false;
		for (int i = 0; i < fbtNode.children.length - 1; i++) {
			Node node = fbtNode.children[i];
			if (node == null) {
				break;
			}
			if ((fbtNode.data[i] != null && fbt.range[1] != null && fbtNode.data[i].compareTo(fbt.range[1]) >= 0) ||
					(fbtNode.data[i + 1] != null && fbt.range[0] != null && fbtNode.data[i + 1].compareTo(fbt.range[0]) <= 0)) {
				if (node instanceof FBTNode) {
					FBTNode fnode = (FBTNode)node;
					AddressNode toAddr = deleteFBTNodeChildren(fbt, fnode, current, deleteNode);
					deleteNode.put(fnode.toLabel(), fnode);
					fnode.deleteFlag = true;
					fnode.parent = null;
					AddressNode addNode = null;
					if (addr == null) {
						addNode = new AddressNode(toAddr.getAddress(), fnode.toLabel());
					}
					else {
						if (toAddr != null) {
							addNode = new AddressNode(toAddr.getAddress(), fnode.toLabel());
						}
						else {
							addNode = new AddressNode(addr, fnode.toLabel());
						}
					}
					fbtNode.children[i] = addNode;
					fbt.fbtNodes.put(addNode.toLabel(), addNode);
					if (current == null) fbt.prevMachine = addNode.getAddress();
					else if (/* current != null && */fbt.nextMachine == null) fbt.nextMachine = addNode.getAddress();
					continue;
				}
				// else if (node instanceof AddressNode) {
				//     AddressNode anode = (AddressNode)node;
				//     fbt.fbtNodes.put(anode.toLabel(), anode);
				//     if (current == null) fbt.prevMachine = anode.getAddress();
				//     else if (/* current != null && */fbt.nextMachine == null) fbt.nextMachine = anode.getAddress();
				//     continue;
				// }
				else if (node instanceof DataNode) {
					DataNode dnode = (DataNode)node;
					AddressNode addNode = new AddressNode(addr, dnode.toLabel());
					fbtNode.children[i] = addNode;
					fbt.fbtNodes.put(addNode.toLabel(), addNode);
					if (current == null) fbt.prevMachine = addr;
					else if (fbt.nextMachine == null) fbt.nextMachine = addr;
					continue;
				}
			}
			if (node instanceof FBTNode) {
				FBTNode fnode = (FBTNode)node;
				current = adjustIndex(fbt, fnode, current, addr, selfAddr, deleteNode);
			}
			else if (node instanceof AddressNode) {
				AddressNode anode = (AddressNode)node;
				Node temp;
				synchronized (this.fbtNodes) {
					temp = this.fbtNodes.get(anode.getText());
				}
				if (temp != null) {
					// if (this.fbtNodes.containsKey(anode.getText())) {
					fbtNode.children[i] = temp;
					// fbtNode.children[i] = this.fbtNodes.get(anode.getText());
					fbt.fbtNodes.put(anode.getText(), fbtNode.children[i]);
					if (fbtNode.children[i] instanceof FBTNode) {
						((FBTNode)fbtNode.children[i]).deleteFlag = false;
						((FBTNode)fbtNode.children[i]).parent = fbtNode;
						current = adjustIndex(fbt, (FBTNode)fbtNode.children[i], current, addr, selfAddr, deleteNode);
					}
					else if (fbtNode.children[i] instanceof DataNode) {
						((DataNode)fbtNode.children[i]).setParent(fbtNode);
						if (fbt.leftmost == null) fbt.leftmost = (DataNode)fbtNode.children[i];
						// if (current != null) {
						//     current.setNext((DataNode)fbtNode.children[i]);
						//     ((DataNode)fbtNode.children[i]).setPrev(current);
						// }
						current = (DataNode)fbtNode.children[i];
					}
				}
				else {
					if (anode.getAddress() == null) anode.setAddress(addr);
					fbt.fbtNodes.put(anode.toLabel(), anode);
					if (current == null) fbt.prevMachine = anode.getAddress();
					if (current != null && fbt.nextMachine == null) fbt.nextMachine = anode.getAddress();
				}
			}
			else if (node instanceof DataNode) {
				DataNode dnode = (DataNode)node;
				fbt.fbtNodes.put(dnode.toLabel(), dnode);
				if (fbt.leftmost == null) fbt.leftmost = dnode;
				if (current != null) {
					current.setNext(dnode);
					dnode.setPrev(current);
				}
				current = dnode;
			}
			else {
				System.err.println("ERROR FatBtree#adjustIndex");
			}
		}
		boolean f = fbtNode.shareAddress.remove(null);
		if (f) {
			fbtNode.shareAddress.add(addr);
		}
		f = fbtNode.shareAddress.remove(selfAddr);
		if (f) {
			fbtNode.shareAddress.add(addr);
		}
		return current;
	}

	public FatBtree toInstance(String[] text, ID id) {
		return FatBtree._toInstance(text, id, this);
	}

	public static FatBtree _toInstance(String[] text, ID id, FatBtree mainTree) {
		FatBtree fbt = new FatBtree();

		int i = 0;
		ID[] range = new ID[2];
		range[0] = (text[i].compareTo("") != 0) ? id.toInstance(text[i]) : null; i++;
		range[1] = (text[i].compareTo("") != 0) ? id.toInstance(text[i]) : null; i++;
		fbt.range = range;

		String name = text[i]; i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		fbt.fbtNodes = new HashMap<String,Node>();
		FBTNode root = FBTNode._toInstance(temp, id, fbt, mainTree); i += n;
		fbt.root = root;

		return fbt;
	}

	public void toLog(){
		this.root.toLog();
	}

	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		sb.append(((this.range[0] != null) ? this.range[0].toMessage() : "") + " ");
		sb.append(((this.range[1] != null) ? this.range[1].toMessage() : "") + " ");
		sb.append(this.root.toMessage() + " ");
		sb.append("_end_");

		String msg = sb.toString();
		return NAME + " " + msg.split(" ").length + " " + msg;
	}

	public String toAdjustInfo() {
		return "_null_";
	}

	public AddressNode adjust(String text, ID id, InetSocketAddress addr, String info) {
		if (info == null) {
			return null;
		}
		return new AddressNode(null, "_null_");
	}

	public InetSocketAddress[] getAckMachine() {
		return new InetSocketAddress[0];
	}


	public ID getID() {return this.range[0];}

	public ID[] getResponsibleRange(MessageSender sender) throws IOException {
		return this.range;
	}

	public DataNode getFirstDataNode() {
		return this.leftmost;
	}

	public DataNode getNextDataNode(DataNode dataNode) {
		return dataNode.getNext();
	}

	public ID[] getDataNodeRange(DataNode dataNode) {
		return dataNode.getRange();
	}

	public InetSocketAddress getNextMachine() {
		return this.nextMachine;
	}


	public Node searchKey(MessageSender sender, ID key) throws IOException {
		return searchKey(sender, key, this.root, false);
	}
	public Node searchKey(MessageSender sender, ID key, String text) throws IOException {

		if (text == null) {
			return searchKey(sender, key, this.root, false);
		}
		else if (text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}

		Node node;
		synchronized (this.fbtNodes) {
			node = this.fbtNodes.get(text);
		}
		if (node == null) {
			System.err.println("WARNING FatBtree#searchKey(MessageSender, ID, String)");
			// return searchKey(sender, key, this.root);
			DataNode dataNode = this.leftmost;
			while (dataNode != null) {
				ID[] r = dataNode.getRange();
				if ((r[0] == null || key.compareTo(r[0]) >= 0) &&
						(r[1] == null || key.compareTo(r[1]) < 0)) {
					return dataNode;
				}
				dataNode = dataNode.getNext();
			}
			// if (this.range[0] != null && key.compareTo(this.range[0]) < 0) {
			// 	if (this.prevMachine != null) {
			// 	    return new AddressNode(this.prevMachine, "_root_");
			// 	}
			// }
			// else if (this.range[1] != null && key.compareTo(this.range[1]) >= 0) {
			// 	if (this.nextMachine != null) {
			// 	    return new AddressNode(this.nextMachine, "_root_");
			// 	}
			// }
			return null;
		}
		else if (node instanceof FBTNode) {
			FBTNode fbtNode = (FBTNode)node;
			return searchKey(sender, key, fbtNode, false);
		}
		else if (node instanceof DataNode) {
			return node;
		}
		System.err.println("ERROR FatBtree#searchKey(MessageSender, ID, String)");
		return null;
	}

	// public Node searchKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return searchKey(sender, key, start);
	// }

	private Node searchKey(MessageSender sender, ID key, FBTNode start, boolean f) throws IOException {
		Node current = start;
		int count = 0;
		while (current instanceof FBTNode) {
			if (count > 5) return null; count++;
			FBTNode fbtNode = (FBTNode)current;
			NodeStatus status = fbtNode.latchISNode();
			if (status == null) {
				System.err.println("MESSAGE access path error 0");
				if (f == true || Thread.activeCount() > 16) return null;
				// while (this.root.deleteFlag) {}
				int i;
				int bb = 50;
				for (i = 0; i < bb; i++) {
					if (this.root.deleteFlag == false) break;
					try {Thread.sleep(100);}
					catch (InterruptedException e) {}
				}
				if (i == bb) {
					this.root.deleteFlag = false;
				}
				return searchKey(sender, key, this.root, true);
			}
			if ( ! ((fbtNode.range[0] == null || key.compareTo(fbtNode.range[0]) >= 0) &&
					(fbtNode.range[1] == null || key.compareTo(fbtNode.range[1]) < 0)) ) {
				System.err.println("MESSAGE access path error");
				endSearchData(sender, status);
				if (f == true || Thread.activeCount() > 16) return null;
				int i;
				int bb = 50;
				for (i = 0; i < bb; i++) {
					if (this.root.deleteFlag == false) break;
					try {Thread.sleep(100);}
					catch (InterruptedException e) {}
				}
				if (i == bb) {
					this.root.deleteFlag = false;
				}
				// while (this.root.deleteFlag) {}
				return searchKey(sender, key, this.root, true);
			}
			int i;
			for (i = 0; i < fbtNode.children.length - 1; i++) {
				if ((fbtNode.data[i] == null || key.compareTo(fbtNode.data[i]) >= 0) &&
						(fbtNode.data[i + 1] == null || key.compareTo(fbtNode.data[i + 1]) < 0)) {
					current = fbtNode.children[i];
					break;
				}
			}
			endSearchData(sender, status);
			if (i == fbtNode.children.length - 1) {
				return null;
				// 	int bb = 50;
				// 	for (i = 0; i < bb; i++) {
				// 	    if (this.root.deleteFlag == false) break;
				// 	    try {Thread.sleep(100);}
				// 	    catch (InterruptedException e) {}
				// 	}
				// 	if (i == bb) {
				// 	    this.root.deleteFlag = false;
				// 	}
				// 	return searchKey(sender, key, this.root);
			}
		}
		return current;
	}



	/*
	 * もしrangeの左端がnullのときには
	 * 最も左端のデータノードから検索を始める。
	 *
	 * rangeで指定した範囲のキーをもつデータノードのラッチ状態を配列にして返す。
	 */
	public NodeStatus[] searchData(MessageSender sender, ID[] range) throws IOException {
		// System.err.println("DEPLICATE FatBtree#searchData(ID[])");
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		DataNode node = null;
		if (range[0] == null) {
			node = getFirstDataNode();
		}
		else {
			/*
			 * たぶんDataNodeが必ず返るところなのだが、
			 * たまに別のノードが返るのでエラーチェックを入れるようにしたらしい。
			 */
			Node temp = searchKey(sender, range[0]);
			if (temp instanceof DataNode) {
				node = (DataNode)temp;
			}
			else {
				System.err.println("WARNING FatBtree#searchData");
			}
		}


		while (node != null) {
			NodeStatus s = node.searchData();
			status.add(s);
			ID[] r = node.getRange();
			if (r[1] != null && range[1] != null && r[1].compareTo(range[1]) >= 0) {
				break;
			}
			node = getNextDataNode(node);
		}
		return status.toArray(new NodeStatus[0]);
	}



	public NodeStatus searchData(MessageSender sender, DataNode dataNode) throws IOException {
		// System.err.println("DEPLICATE FatBtree#searchData(DataNode)");
		return dataNode.searchData();
	}

	public NodeStatus[] searchData(MessageSender sender, DataNode[] dataNodes) throws IOException {
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		for (DataNode dataNode: dataNodes) {
			NodeStatus s = dataNode.searchData();
			status.add(s);
		}
		return status.toArray(new NodeStatus[0]);
	}

	public void endSearchData(MessageSender sender, NodeStatus[] status) {
		for (NodeStatus s: status) {
			endSearchData(sender, s);
		}
	}

	public void endSearchData(MessageSender sender, NodeStatus status) {
		status.getNode().endSearchData(status);
	}

	public Node updateKey(MessageSender sender, ID key) throws IOException {
		return updateKey(sender, key, this.root, false);
	}

	public Node updateKey(MessageSender sender, ID key, String text) throws IOException {
		// if (text == null || text.compareTo("_root_") == 0) {
		if (text == null) {
			return updateKey(sender, key, this.root, false);
		}
		else if (text.compareTo("_first_") == 0) {
			return getFirstDataNode();
		}

		Node node;
		synchronized (this.fbtNodes) {
			node = this.fbtNodes.get(text);
		}
		if (node == null) {
			System.err.println("WARNING FatBtree#updateKey(MessageSender, ID, String)");
			// return updateKey(sender, key, this.root, false);
			//

			DataNode dataNode = this.leftmost;
			while (dataNode != null) {
				ID[] r = dataNode.getRange();
				if ((r[0] == null || key.compareTo(r[0]) >= 0) &&
						(r[1] == null || key.compareTo(r[1]) < 0)) {
					return dataNode;
				}
				dataNode = dataNode.getNext();
			}
			//
			// if (this.range[0] != null && key.compareTo(this.range[0]) < 0) {
			// 	if (this.prevMachine != null) {
			// 	    return new AddressNode(this.prevMachine, "_root_");
			// 	}
			// }
			// else if (this.range[1] != null && key.compareTo(this.range[1]) >= 0) {
			// 	if (this.nextMachine != null) {
			// 	    return new AddressNode(this.nextMachine, "_root_");
			// 	}
			// }
			//
			return null;
		}
		else if (node instanceof FBTNode) {
			FBTNode fbtNode = (FBTNode)node;
			return updateKey(sender, key, fbtNode, false);
		}
		else if (node instanceof DataNode) {
			return node;
		}

		System.err.println("ERROR FatBtree#updateKey(MessageSender, ID, String)");
		return null;
	}

	// public Node updateKey(MessageSender sender, ID key, Node start) throws IOException {
	// 	return updateKey(sender, key, start);
	// }

	private Node updateKey(MessageSender sender, ID key, FBTNode start, boolean f) throws IOException {

		Node current = start;
		int count = 0;


		while (current instanceof FBTNode) {

			if (count > 5) return null;
			count++;

			FBTNode fbtNode = (FBTNode)current;
			NodeStatus status = fbtNode.latchIXNode();


			if (status == null) {

				System.err.println("MESSAGE access path error 0");
				if (f == true || Thread.activeCount() > 16) return null;
				// while (this.root.deleteFlag) {}
				int i;
				int bb = 50;
				for (i = 0; i < bb; i++) {
					if (this.root.deleteFlag == false) break;
					try {Thread.sleep(100);}
					catch (InterruptedException e) {}
				}
				if (i == bb) {
					this.root.deleteFlag = false;
				}
				return updateKey(sender, key, this.root, true);
			}


			if ( ! ((fbtNode.range[0] == null || key.compareTo(fbtNode.range[0]) >= 0) &&
					(fbtNode.range[1] == null || key.compareTo(fbtNode.range[1]) < 0)) ) {


				System.err.println("MESSAGE access path error");
				endUpdateData(sender, status);
				if (f == true || Thread.activeCount() > 16) return null;
				int i;
				int bb = 50;
				for (i = 0; i < bb; i++) {
					if (this.root.deleteFlag == false) break;
					try {Thread.sleep(100);}
					catch (InterruptedException e) {}
				}
				if (i == bb) {
					this.root.deleteFlag = false;
				}
				// while (this.root.deleteFlag) {}
				return updateKey(sender, key, this.root, true);
			}


			int i;
			for (i = 0; i < fbtNode.children.length - 1; i++) {
				if ((fbtNode.data[i] == null || key.compareTo(fbtNode.data[i]) >= 0) &&
						(fbtNode.data[i + 1] == null || key.compareTo(fbtNode.data[i + 1]) < 0)) {
					current = fbtNode.children[i];
					break;
				}
			}
			endUpdateData(sender, status);
			if (i == fbtNode.children.length - 1) {
				return null;
				// int bb = 50;
				// for (i = 0; i < bb; i++) {
				//     if (this.root.deleteFlag == false) break;
				//     try {Thread.sleep(100);}
				//     catch (InterruptedException e) {}
				// }
				// if (i == bb) {
				//     this.root.deleteFlag = false;
				// }
				// return updateKey(sender, key, this.root);
			}
		}
		return current;
	}

	public NodeStatus[] updateData(MessageSender sender, ID[] range) throws IOException {
		// System.err.println("DEPLICATE FatBtree#updateData(ID[])");
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		DataNode node = null;
		if (range[0] == null) {
			node = getFirstDataNode();
		}
		else {
			// node = (DataNode)updateKey(sender, range[0]);
			Node temp = updateKey(sender, range[0]);
			if (temp instanceof DataNode) {
				node = (DataNode)temp;
			}
			else {
				System.err.println("WARNING FatBtree#updateData");
			}
		}
		while (node != null) {
			NodeStatus s = node.updateData();
			status.add(s);
			ID[] r = node.getRange();
			if (r[1] != null && range[1] != null && r[1].compareTo(range[1]) >= 0) {
				break;
			}
			node = getNextDataNode(node);
		}
		return status.toArray(new NodeStatus[0]);
	}




	public NodeStatus updateData(MessageSender sender, DataNode dataNode) throws IOException {
		// System.err.println("DEPLICATE FatBtree#updateData(DataNode)");
		return dataNode.updateData();
	}




	public NodeStatus[] updateData(MessageSender sender, DataNode[] dataNodes) throws IOException {
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		for (DataNode dataNode: dataNodes) {
			NodeStatus s = dataNode.updateData();
			status.add(s);
		}
		return status.toArray(new NodeStatus[0]);
	}



	/*
	 * usage:
	 * 1.Status status = updateData();
	 * 2.****** here you can change the node state. *****
	 * 3.endUpdateData(status)
	 */
	public void endUpdateData(MessageSender sender, NodeStatus[] status) {
		for (NodeStatus s: status) {
			endUpdateData(sender, s);
		}
	}



	public void endUpdateData(MessageSender sender, NodeStatus status) {
		status.getNode().endUpdateData(status);
	}




	public ID[] getRangeForNew(ID id) {
		ID[] range = new ID[2];
		range[0] = id;
		range[1] = this.range[1];
		return range;
	}


	private Node splitIndex(MessageSender sender, ID[] range, ID id, InetSocketAddress addr, Node node) {
		if (node instanceof FBTNode) {
			return splitIndex(sender, range, id, addr, (FBTNode)node);
		}
		else if (node instanceof AddressNode) {
			return splitIndex(sender, range, id, addr, (AddressNode)node);
		}
		else if (node instanceof DataNode) {
			return splitIndex(sender, range, id, addr, (DataNode)node);
		}
		System.err.println("ERROR FatBtree#splitIndex(MessageSender, ID[], ID, InetSocketAddress, Node)");
		return null;
	}




	/*
	 *
	 * ##### caution #####
	 * this implementation is not follow the FatBtree theory.
	 *
	 * ロックしたものは
	 * 親、左、右のノードだが、とりあえず親だけをロックしてみよう。
	 * そもそもロックを使用してはいけないかもしれない。FatBtreeならラッチを使わないと、、、
	 */
	public DataNode[] moveRightMostDataNodes(DataNode[] dataNodesToBeRemoved, InetSocketAddress address, MessageSender sender){


		System.out.println("DEBUG_moveRightMostDataNodes");




		DataNode dataNode = this.leftmost;
		while( dataNode.getNext() != null){
			dataNode = dataNode.getNext();
		}

		System.out.println("DEBUG_real right most dataNode:"+dataNode.toString());
		System.out.println("DEBUG_target right most dataNode:"+dataNodesToBeRemoved[dataNodesToBeRemoved.length-1].toString());

		//System.out.println("DEBUG_passed dataNodes:"+dataNodesToBeRemoved.toString());
		//System.out.println("DEBUG_real right most dataNode's parent:" + ((FBTNode)dataNode.getParent()).children[((FBTNode)dataNode.getParent()).children.length-1].toString()  );


		System.out.println("BEFORE_DEBUG_SEND_START");
		try {
			/*
			 * protocol
			 * id1.toString id2.toString ... :(<- this means separation of dataNodes) idx.toString id(x+1).toString .....
			 */
			System.out.println("DEBUG_SEND_START");
			List<String> strs = new ArrayList<String>();
			Gson gson = new Gson();
			for(DataNode dt:dataNodesToBeRemoved){
				for(ID id: dt.getAll()){
					strs.add(id.toString());
				}
				strs.add(":");
			}
			sender.send(gson.toJson(strs), address);
			System.out.println("MOVED this:"+ gson.toJson(strs));
			System.out.println("DEBUG_SEND_END");
		} catch (IOException e) {
			e.printStackTrace();
		}


		/*
		 * 取り除くには1つずつ行う
		 * 1つ取り除き他に子供がいなければその親も再帰的に取り除く
		 * 1つ以上あるときは親の担当範囲を変更して終わり。
		 */
		for(int i=0; i< dataNodesToBeRemoved.length;i++){



			//i think dataNode's parent must be FBTNode in FatBtree!!
			if(dataNode.getParent() instanceof FBTNode){



				FBTNode parent = (FBTNode)dataNode.getParent();


				Node[] temp = parent.children.clone();

				// parent get child in which dataNode removed.
				System.arraycopy(temp, 0, parent.children, 0, parent.children.length-1);

				// delete all reference to the dataNode i want to delete.
				DataNode prevDataNode = dataNode.getPrev();
				prevDataNode.setNext(null);

				if(parent.getChildrenSize() == 0){
					//  delete parent node.
				}
				// update parent range.
				else{
					parent.range[1] = prevDataNode.getMaxID();
				}

			}
		}

		/*while(dataNode != null){


			synchronized (dataNode.getParent()) {
				for(int i=0;i<dataNodes.length;i++){

					DataNode targetNode = dataNodes[i];
					if(dataNode.toLabel().equals(targetNode.toLabel())){

						counterOfFoundDataNode++;



		 * update dataNode's parent index.

						if(dataNode.getParent() instanceof FBTNode){
							FBTNode parent = (FBTNode) dataNode.getParent();


		 * when parent has one child, you should remove this parent too.
		 * and you should update this parent's parent index...

							if(parent.getChildrenSize() == 1){

							}


							// when dataNode is at the most left in dataNode's parent's child.
							// you should update range which the parent has.
							if(parent.range[0].toString().equals(dataNode.getMinID().toString())){
								parent.range[0] = dataNode.getMinID();
							}
							//when dataNode is at the most right in dataNode's parent's child.
							else if(parent.range[1].toString().equals(dataNode.getMaxID().toString())){
								parent.range[1] = dataNode.getMaxID();
							}
							else{

							}
						}





					}
				}
			}
			if(counterOfFoundDataNode == dataNodes.length) break;
			dataNode = dataNode.getNext();
		}*/
		return null;
	}


	/*
	 * movement data nodes for load balance
	 *
	 *  必ずsenderを渡す構造はよくない。
	 *
	 */
	public void addPassedDataNodes(boolean toLeftMost, List<DataNode> dataNodes){

		System.out.println("DEBUG_IN_ADD_DATANODE");
		/*
		 * here is not important!!
		 * sender object is mess!
		 */
		MessageSender sender = null;
		try {
			sender = (new MessageReceiver(0, null)).getMessageSender();
		} catch (IOException e) {
			e.printStackTrace();
		}


		if(toLeftMost == true){

			System.out.println("DEBUG_TO_LEFTMOST");

			DataNode dataNode = this.leftmost;

			System.out.println("DEBUG:numberOfDataNodes:"+ dataNodes.size());
			System.out.println("DEBUG:"+dataNodes.get(0));
			System.out.println("DEBUG:"+dataNodes.get(1));

			if(dataNode.getParent() instanceof FBTNode){

				FBTNode parent = (FBTNode) dataNode.getParent();
				Node[] childrenToBeReplace = new Node[parent.children.length+dataNodes.size()];

				for(int i=0; i<dataNodes.size(); i++){
					if(i==0){

					}else if(i > 0){
						dataNodes.get(i-1).setNext(dataNodes.get(i));
						dataNodes.get(i).setPrev(dataNodes.get(i-1));
					}
					dataNodes.get(i).setParent(parent);
				}
				dataNodes.get(dataNodes.size()-1).setNext(this.leftmost);
				// i want do like this : childrenToBeReplace = dataNodes + parent.children !!
				for(int i=0,j=0; i<childrenToBeReplace.length; i++){
					if(i < dataNodes.size()){
						childrenToBeReplace[i] = dataNodes.get(i);
					}else if( j < parent.children.length){
						childrenToBeReplace[i] = parent.children[j];
						j++;
					}
				}
				parent.children = childrenToBeReplace;

				parent.range[0] = dataNodes.get(0).getMinID();
				parent.tree.range[0] = dataNodes.get(0).getMinID();

				this.leftmost = dataNodes.get(0);

				System.out.println("DEBUG_ADD_DATANODE_END");
				System.out.println(this.leftmost.toString());
				System.out.println("DEBUG:PASSED_DATA_LEFTMOST:"+ dataNodes.get(0));
				//System.out.println("DEBUG:parent" + parent.toString());
				System.out.println("DEBUG_END");

			}


		}else{

		}
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
/*
	private String getPrevMachineIPString(){
		if(this.getPrevMachine() == null){
			return "";
		}
		String address = this.getPrevMachine().getAddress().toString();
		return address.substring(address.indexOf('/'));
	}*/



	/*
	 * TODO
	 */
	/*public void checkLoad(LoadInfoTable loadInfoTable, MessageSender sender){

		// ##### 時間測定用変数 #####
		long checkStartTime_msec = getCurrentTime();
		Long moveStartTime_msec;
		Long updateStartTime_msec;
		Long checkEndTime_msec;
		// ##### /時間測定用変数 #####



		try{
			//アドレスノードの位置を調べる
			for(Node node : this.root.children){
				//priJap("ルートの子供です。");
				//pri("root child : "+node.toMessage());
				if(node instanceof FBTNode){
					for(Node cnode: ((FBTNode) node).children){
						pri(cnode.toMessage());
					}
				}
			}
		}catch( Exception e){
			e.printStackTrace();
		}
		//pri(this.root.range[0].toMessage());
		//pri(this.root.range[1].toMessage());
		try{
			for(ID id : this.root.data){
				pri(id.toMessage());
			}
		}catch(Exception e){

		}



		pri("##### 負荷集計フェーズ #####");
		//もしまだ1つ前の計算機の負荷が登録されていなければそっちにデータ移動はできない
		//1つ後の計算機に関しても同様。
		//そのためのチェックをしているだけだが、長くなってしまった。
		//nullかどうか調べておかないとすぐエラーになってしまうので。
		int prevLoad = 0;
		int nextLoad = 0;
		if(this.getPrevMachine() != null && loadInfoTable != null && loadInfoTable.getLoadList() != null
				&& loadInfoTable.getLoadList().get(this.getPrevMachineIPString()) != null){
			//priJap("前の計算機の負荷を取得できました");
			prevLoad = loadInfoTable.getLoadList().get(this.getPrevMachineIPString());
		}
		if(this.getNextMachine() != null && loadInfoTable.getLoadList() != null
				&& loadInfoTable.getLoadList().get(this.getNextMachineIPString()) != null){
			//priJap("後の計算機の負荷を取得できました");
			nextLoad = loadInfoTable.getLoadList().get(this.getNextMachineIPString());
		}


		//負荷を集計するときに左端と右端のデータノードを格納しておきます。後で使うので便利です。
		//DataNode leftMostDataNode = this.getFirstDataNode();
		DataNode rightMostDataNode = this.getFirstDataNode();
		int myLoad = 0;
		int myDataSize = 0;



		pri("====== 負荷を集計 ======");
		synchronized (this) {
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				myLoad += dataNode.getLoad();
				myDataSize += dataNode.size();
				rightMostDataNode = dataNode;
				dataNode = dataNode.getNext();
			}
		}
		pri("====== /負荷を集計 ======");


		pri("====== 自分の負荷更新と負荷平均値を再計算 ======");
		loadInfoTable.setLoad(this.getMyAddressIPString(), myLoad);
		loadInfoTable.setDataSize(this.getMyAddressIPString(), myDataSize);
		loadInfoTable.reCalcAverage();
		pri("====== /自分の負荷更新と負荷平均値を再計算 ======");


		//データ更新された後に平均値を取得するという順番に注意！
		int average = loadInfoTable.getAverage();
		int threshold = (int) (average * errorRangeRate);



		// ##### 計算機の状態をログに出力 #####
		pri("LOAD_INFO_TABLE_TOJSON :"+loadInfoTable.toJson());
		pri("getMyAddressIPString : " +this.getMyAddressIPString());
		pri("getPrevMachineIP : " + this.getPrevMachineIPString());
		pri("getNextMachineIP : "+ this.getNextMachineIPString());
		pri("myLoad : "+ myLoad);
		pri("prevLoad : " + prevLoad);
		pri("nextLoad : " + nextLoad);
		pri("average : " + loadInfoTable.getAverage());
		pri("threshold : "+ threshold);
		this.toLog();
		int prevDataSize=0;
		int nextDataSize=0;
		try{
			prevDataSize = loadInfoTable.getDataSizeList().get(this.getPrevMachineIPString());
		}catch(Exception e){}
		try{
			nextDataSize = loadInfoTable.getDataSizeList().get(this.getNextMachineIPString());
		}catch(Exception e){}
		pri("myDataSize : "+ myDataSize);
		pri("prevDataSize : " + prevDataSize);
		pri("nextDataSize : " + nextDataSize);
		// ##### /計算機の状態をログに出力 #####



		//アクセス負荷とデータ容量をログに出力
		log( AnalyzerManager.getLogLoadTag()
				+" "+checkStartTime_msec
				+" "+myLoad
				+" "+myDataSize
				+" "+threshold);




		// ##### 負荷転送フェーズ  #####
		try {
			sender.setHeader("LOAD_INFO");
			if( this.getPrevMachine() != null){
				pri("====== 左隣へ負荷情報を回します =====");
				sender.send((new LoadMessage(this.getMyAddressIPString(), loadInfoTable)).toJson(), this.getPrevMachine());
			}
			if( this.getNextMachine() != null){
				pri("====== 右隣へ負荷情報を回します =====");
				sender.send((new LoadMessage(this.getMyAddressIPString(), loadInfoTable)).toJson(), this.getNextMachine());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// ##### /負荷転送フェーズ  #####




		//負荷集計かつ負荷転送フェーズ終わりと負荷移動フェーズの始まり
		moveStartTime_msec = getCurrentTime();




		//##### 負荷移動フェーズ #####

		 * この場合は負荷分散が必要ない
		 * １．自分の負荷がある閾値より小さい
		 * ２．自分の負荷が両隣の負荷のどちらよりも小さい

		if(myLoad <= threshold || (myLoad <= prevLoad && myLoad <= nextLoad) ){
			//負荷集計が終わったらデータノードに蓄積したアクセス負荷の情報をリセットします。
			resetLoadCounter();
			moveStartTime_msec = getCurrentTime();
			updateStartTime_msec = getCurrentTime();
			checkEndTime_msec = getCurrentTime();
			//負荷転送フェーズにかかった時間
			log("LOG-LOADBLANCE-CHECKLOAD-TIME"
					+" "+checkStartTime_msec
					+" "+(moveStartTime_msec-checkStartTime_msec)
					+" "+(updateStartTime_msec-moveStartTime_msec)
					+" "+(checkEndTime_msec-updateStartTime_msec));
			return ;
		}


		priJap("負荷分散のためにデータノードを移動します。");
		ArrayList<DataNode> dataNodeToBeMoved = new ArrayList<DataNode>();
		InetSocketAddress target = null;
		int tempLoadCount = 0;
		int tempDataCount = 0;

		//前と後のどちらにデータを移動するか決定する
		if(myLoad > prevLoad && prevLoad != 0){
			priJap("前の計算機へデータノードを転送します。");

			 * 次の場合は移動するデータノードの探索を終了し移動に移ります。
			 * １．データノード移動あとの負荷が閾値より小さい
			 * ２．移動可能なデータ数（データノードに格納されているID数）を超えた

			DataNode dataNode = this.getFirstDataNode();
			while( true  ){
				if( 	( myLoad - (tempLoadCount + dataNode.getLoad()) ) < threshold
						|| maxDataSizeCanBeMoved < (tempDataCount + dataNode.size())){
					break;
				}
				tempDataCount += dataNode.size();
				tempLoadCount += dataNode.getLoad();
				dataNodeToBeMoved.add(dataNode);
				dataNode = dataNode.getNext();
			}
			if(dataNodeToBeMoved.size() > 0){
				target = this.getPrevMachine();
			}
		}
		else if(myLoad > nextLoad && nextLoad != 0){
			priJap("次の計算機へデータノードを転送します。");
			DataNode dataNode = rightMostDataNode;
			while( true  ){
				if( 	( myLoad - (tempLoadCount + dataNode.getLoad()) ) < threshold
						|| maxDataSizeCanBeMoved < (tempDataCount + dataNode.size())){
					break;
				}
				tempDataCount += dataNode.size();
				tempLoadCount += dataNode.getLoad();
				dataNodeToBeMoved.add(dataNode);
				dataNode = dataNode.getPrev();
			}
			if(dataNodeToBeMoved.size() > 0){
				target = this.getNextMachine();
			}
		}

		//転送するデータノードが決まったら、データノードに蓄積したアクセス負荷の情報をリセットします。
		resetLoadCounter();



		//実際にデータ転送が行われるのはここ
		moveData((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]),target , sender);
		// ##### /負荷移動フェーズ #####



		//負荷移動フェーズの終わりとインデックス更新フェーズの始まり
		updateStartTime_msec = getCurrentTime();



		// ##### インデックス更新フェーズ #####
		updateIndex((DataNode[])dataNodeToBeMoved.toArray(new DataNode[0]), target);
		// ##### インデックス更新フェーズ #####



		checkEndTime_msec = getCurrentTime();
		//負荷転送フェーズにかかった時間
		log("LOG-LOADBLANCE-CHECKLOAD-TIME"
				+" "+checkStartTime_msec
				+" "+(moveStartTime_msec-checkStartTime_msec)
				+" "+(updateStartTime_msec-moveStartTime_msec)
				+" "+(checkEndTime_msec-updateStartTime_msec));

	}
	*/

	private void sendUpdateInfo(InetSocketAddress target, DataNode[] dataNodesToBeRemoved){


	}


	/*private void updateIndex(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target) {
		priJap("updateIndex関数が呼ばれました");


		for(DataNode dn : dataNodesToBeRemoved){
			FBTNode parent = (FBTNode) dn.getParent();
			parent.replaceDataNodeToAddressNode(dn, new AddressNode(target, dn.toLabel()));

			//データノードの場合
			ArrayList<InetSocketAddress> listToSend = new ArrayList<InetSocketAddress>();
			for(InetSocketAddress isa : parent.shareAddress){
				listToSend.add(isa);
			}
			//データノードの場合終わり


			//もし親がルートノードなら終了
			if(parent.getNumberOfLeafNodes() > 0 || parent.equals(this.root)){
				for(InetSocketAddress isa : listToSend){
					sendUpdateInfo(isa,dataNodesToBeRemoved);
				}
				return;
			}


			parent = parent.parent;
			FBTNode child = parent;

			//インターノードとインターノード
			//再帰的な処理に移ります
			while(parent != null
					&& parent.getNumberOfInterOrLeafNodes()== 0
					&& !parent.equals(this.root) ){
				for(InetSocketAddress isa : parent.shareAddress){
					if(!listToSend.contains(isa)){
						listToSend.add(isa);
					}
				}
				parent.replaceFbtNodeToLink(child, target);
				parent = parent.parent;
				child = parent;
			}

			//TODO

			 * 送るもの
			 * ・minId
			 * ・自分のアドレス　<-- 送った先でアドレスノードを突き止めるのに使えそう

			for(InetSocketAddress isa : listToSend){
				//sendUpdateInfo();
			}




			 * 以降は削除かなー
			 * here,
			 * if parent has no leaf node, replace FBTnode parent to address node.
			 * and if the grand parent is not root node, calcurate recursively.
			 *
			 * in short, check the grand parent has no inter-node(FBTNode) or not ,
			 * and if then check the grand grand parent,,,,
			 *
			 * ここでは
			 * リーフノードとインターノード間での更新を行います。

			FBTNode grandParent = parent.parent;
			FBTNode targetChild = parent;
			ArrayList<InetSocketAddress> listToSendUpdateInfo = new ArrayList<InetSocketAddress>();
			if(parent.getNumberOfLeafNodes() == 0){

			 *　祖父の子からparentの位置を見つけて入れ替える
			 *　ついでに更新共有している計算機を見つけておく
				for(int i=0; i< grandParent.getChildrenSize(); i++){
					Node child = grandParent.children[i];
					if(grandParent.children[i] == parent){
						grandParent.children[i] = new AddressNode(target, parent.toLabel());;
					}
					//更新情報を送る計算機を集めています。
					if(child instanceof AddressNode){
						AddressNode achild = (AddressNode) child;// <-- address node child
						//まだ追加していないアドレスだけ追加
						if(!listToSendUpdateInfo.contains(achild.getAddress())){
							listToSendUpdateInfo.add(achild.getAddress());
						}
					}
				}
			}else {
			}

			 * データノードとインターノード間の更新情報を送る
			 *
			 * ここで階層ごとに更新情報を送るようにします。
			 * なぜならまとめて更新情報を送ると面倒くさいからです

			//sendUpdateInfo(listToSendUpdateInfo);
			listToSendUpdateInfo.clear();
			parent = parent.parent;
			targetChild = parent;
			 * ここでは
			 * インターノードとルート間の再帰的な更新を行います。
			while(parent != this.root){
			}


			 * 目的のデータノードに対して
			 * 1.左右のデータノードからの参照と
			 * 2.親から参照を取り除く

			if(dn.getPrev() != null){ dn.getPrev().setNext(null);}//左からの参照削除
			if(dn.getNext() != null){ dn.getNext().setPrev(null);}//右からの参照削除
			//親からの参照削除します
			TreeNode parent = (TreeNode) dn.getParent();
			Node[] newChildren = new Node[parent.children.length-1];//親の子供を新しい子供に置き換えます
			for(int i=0,j=0;i < parent.children.length;i++){
				//目的のデータノードはコピーしないでとばす。
				if(parent.children[i] == dn){
					//do nothing
				}else{
					newChildren[j] = parent.children[i];
					j++;
				}
			}
			//親から子への参照を取り除く
			parent.children = newChildren;
		}
	}




	@Override
	public boolean moveData(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target, MessageSender sender) {
		priJap("moveData関数が呼ばれました");
		priJap("移動するデータノードの数は");
		pri(dataNodesToBeRemoved.length );
		priJap("移動するデータノードそれぞれに含むキーの数は");
		for(DataNode d: dataNodesToBeRemoved){
			pri(d.size());
		}
		priJap("移動する相手のアドレスは");
		pri(target.getAddress().toString());

		synchronized (this) {
			// ##### データノード移動フェーズ ######
			priJap("データノード移動フェーズ");
			try {
				sender.setHeader("LOAD_MOVE_DATA_NODES");
				String responseMessage = sender.sendDataNodeAndReceive(dataNodesToBeRemoved, this.getMyAddress(), target);
				priJap("データ移動終わりました。");
				priJap("次のような返事を受け取りました");
				pri(responseMessage);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}

*/

	//TODO
	private String parseTextAndGetMinIdString(String text){
		return null;
	}

	//TOOD
	private String parseTextAndGetMaxIdString(String text){
		return null;
	}


	//TODO
	//データノードの挿入と送信者へ返信
	public String recieveAndUpdateDataForLoadMove(DataNode[] dataNodes, InetSocketAddress senderAddress){
		priJap("データノード受け取りました");
		priJap("送り主のアドレスは");
		pri(senderAddress.getAddress().toString());

		synchronized (this) {
			//from left machine
			if(this.getPrevMachine().toString().equals(senderAddress.toString()) ){
				priJap("左隣から送られてきました");


			}
			//from right machine
			else if(this.getNextMachine().toString().equals(senderAddress.toString()) ){
				priJap("右隣から送られてきました");
				//this.root.children;
				//子ノードをなめてデータノードと同じレンジを持つアドレスノードを探します。
				//見つけたらそのノードをデータノードに置き換えて
				//親ノードの状態と左右のデータノードのリンクを更新します。

			}
			//??? where this data node come from?????
			else{
				pri("this data come from  wrong place..");
				return "知らないアドレスから送られてきました。";
			}
		}

		return "OK";

	}




	/*
	 * ##### dont use this method ######
	 * １．データノードを転送
	 * ２．返事をもらう
	 * ３．自分のインデックスを更新する
	 */
	public void moveData(DataNode[] dataNodesToBeRemoved,
			boolean doMoveDataToLeftNextToMe, MessageSender sender) {

		System.out.println("CALLED_MOVEDATA");
		System.out.println("dataNodesToBeRemoved : "+dataNodesToBeRemoved);
		System.out.println("toLeft : "+doMoveDataToLeftNextToMe);

		/*for(DataNode d: dataNodesToBeRemoved){
			pri("dataNode.toLabel : " + d.toLabel());
		}
		 */


		this.toLog();

		InetSocketAddress targetAddress = null;

		synchronized (this) {

			// ##### データノード移動フェーズ ######
			priJap("データノード移動フェーズ");



			if(doMoveDataToLeftNextToMe==true){
				priJap("データノードを左に転送します");

				targetAddress = this.getPrevMachine();

				try {
					priJap("転送先は");			pri(targetAddress.toString());
					priJap("データーノードの数は");	pri(dataNodesToBeRemoved.length+"");
					priJap("データノードに含まれるアイテムの数はそれぞれ");
					for(DataNode d : dataNodesToBeRemoved){
						pri(d.size()+"");
					}
					priJap("相手から返事が来るのを待ちます");


					//いまはデータ移動とインデックス更新が終わると「OK」を返すようにしています。
					sender.setHeader("LOAD_MOVE_DATA_NODES");
					String responseMessage = sender.sendDataNodeAndReceive(dataNodesToBeRemoved, this.myAddress, targetAddress);


					if(responseMessage.equals("OK") ){
						priJap("OKきました！");
						System.out.println("DATA_NODE_MOVE_SUCCESS");
					}

				} catch (IOException e) {
					e.printStackTrace();
				}

			}else{

				priJap("データノードを右に転送します");
				targetAddress = this.getNextMachine();

				try {
					priJap("転送先は");			pri(targetAddress.toString());
					priJap("データーノードの数は");	pri(dataNodesToBeRemoved.length+"");
					priJap("データノードに含まれるアイテムの数はそれぞれ");
					for(DataNode d : dataNodesToBeRemoved){
						pri(d.size()+"");
					}


					sender.setHeader("LOAD_MOVE_DATA_NODES");
					String responseMessage = sender.sendDataNodeAndReceive(dataNodesToBeRemoved, this.myAddress, targetAddress);


					if(responseMessage.equals("OK") ){
						System.out.println("DATA_NODE_MOVE_SUCCESS");
					}


				} catch (IOException e) {
					e.printStackTrace();
				}

			}




			// ##### インデックス更新フェーズ #####


			priJap("インデックス削除更新フェーズ");

			//TODO
			/*
			 * アイデア1
			 * 取り除くには1つずつ行う
			 * 1つ取り除き他に子供がいなければその親も再帰的に取り除く
			 * 1つ以上あるときは親の担当範囲を変更して終わり。
			 *
			 * データノードのことをc(child)とする
			 * cの親をp(parent)とする
			 *
			 * 1.pのcへのリンクを削除する
			 * 2.pの範囲を更新する
			 * 3.pの子が０の時には
			 *   4.pの親からpへのリンクを削除する
			 *   5.pの親の範囲を更新する
			 * 6.cを次のデータノードに置き換えて1.に戻る
			 */

			DataNode dataNode = null;


			//アイデア２
			if(doMoveDataToLeftNextToMe == true){

				dataNode = this.leftmost;

				InetSocketAddress targetComputer = this.getPrevMachine();

				for(int i=0; i< dataNodesToBeRemoved.length;i++){


					//アイデア２
					DataNode nextDataNode = dataNode.getNext();
					if(nextDataNode != null){
						nextDataNode.setPrev(null);
						this.leftmost = nextDataNode;
					}
					FBTNode parent = (FBTNode)dataNode.getParent();


					//ターゲットの親からターゲットへのリンクをアドレスノードに置き換える
					for(int j=0;j<parent.getChildrenSize();j++){
						if(parent.children[j] == dataNode){
							pri("FIND_CHILD");
							parent.children[j] = new AddressNode(targetComputer, dataNode.toLabel());
						}
					}
					parent.range[0] = dataNode.getPrev() != null? dataNode.getPrev().getMinID():null;


					//再帰的に親の範囲を更新する。
					while(parent.parent != this.root || parent.parent.parent != null){
						FBTNode child = parent;
						parent = parent.parent;
						parent.range[0] = child.range[0];
					}


					//ここからルートのアドレスノードを更新する
					for(int j_rootChildren=0;j_rootChildren<this.root.getChildrenSize();j_rootChildren++){
						Node child = this.root.children[i];

						if(child instanceof AddressNode){
							if(j_rootChildren+1 < this.root.getChildrenSize()-1 // to avoid out of index
									&& this.root.children[j_rootChildren+1] instanceof FBTNode){

								Node[] childrenForReplace = new Node[this.root.getChildrenSize()+1];
								for(int k=0; k<childrenForReplace.length; k++){

									if(k == j_rootChildren){
										childrenForReplace[k] = new AddressNode(targetComputer, dataNode.toLabel());
										k--;// for adjust index. childForReplace is longer than root.childre by 1.
									}else{
										childrenForReplace[k] = this.root.children[k];
									}

								}

								this.root.children = childrenForReplace;
								break;
							}
						}

					}

				}
			}
			else if(doMoveDataToLeftNextToMe == false){
				dataNode = this.leftmost;
				while(dataNode.getNext() != null){
					dataNode = dataNode.getNext();
				}

				InetSocketAddress targetComputer = this.getNextMachine();



				for(int i=0; i< dataNodesToBeRemoved.length;i++){

					//アイデア２
					DataNode prevDataNode = dataNode.getPrev();
					if(prevDataNode != null){
						prevDataNode.setPrev(null);
						this.leftmost = prevDataNode;
					}
					FBTNode parent = (FBTNode)dataNode.getParent();


					//ターゲットの親からターゲットへのリンクをアドレスノードに置き換える
					for(int j=parent.getChildrenSize()-1; j>0;j--){
						if(parent.children[j] == dataNode){
							pri("FIND_CHILD");
							parent.children[j] = new AddressNode(targetComputer, dataNode.toLabel());
						}
					}
					parent.range[0] = dataNode.getNext() != null? dataNode.getNext().getMaxID():null;


					//再帰的に親の範囲を更新する。
					while(parent.parent != this.root || parent.parent.parent != null){
						FBTNode child = parent;
						parent = parent.parent;
						parent.range[1] = child.range[1];
					}


					//ここからルートのアドレスノードを更新する
					for(int j_rootChildren=this.root.getChildrenSize()-1 ;j_rootChildren>0 ; j_rootChildren--){
						Node child = this.root.children[i];

						if(child instanceof AddressNode){
							if(j_rootChildren-1 > 0 // to avoid out of index
									&& this.root.children[j_rootChildren-1] instanceof FBTNode){

								Node[] childrenForReplace = new Node[this.root.getChildrenSize()+1];
								for(int k=childrenForReplace.length-1; k >0 ; k--){

									if(k == j_rootChildren){
										childrenForReplace[k] = new AddressNode(targetComputer, dataNode.toLabel());
										k++;// for adjust index. childForReplace is longer than root.childre by 1.
									}else{
										childrenForReplace[k] = this.root.children[k];
									}

								}

								this.root.children = childrenForReplace;
								break;
							}
						}

					}

				}

			}

			//アイデア１
			/*					//隣のデータノードから削除したいデータノードへのリンクを削除
					DataNode nextDataNode = dataNode.getNext();
					if(nextDataNode != null){
						nextDataNode.setPrev(null);
					}
					//1.pのcへのリンクを削除する
					//データノードの親はたぶんFBTNode
					FBTNode parent = (FBTNode)dataNode.getParent();

					//親から削除したいデータノードへのリンクを削除
					Node[] childrenOfParentToReplace = new Node[parent.children.length-1];//parent.children.clone();
					System.arraycopy( parent.children, 1,childrenOfParentToReplace, 0, parent.children.length-1);

			 * TODO
			 * まずアドレスノードの位置を調べてから
			 * もしFBTノードに入っていたら子供の一番端はデータノードではなくアドレスノードのはずなので対処する
			 * if(parent.children[parent.children.length-1] instanceof AddressNode){

					}
					parent.children = childrenOfParentToReplace;
					FBTNode grandParent = parent.parent;
					parent.range[1] = nextDataNode.getMaxID();
					//親を再帰的にインデックス更新します。
					while(true){
						//祖父がいるときは繰り返し更新していかなくてはいけない
						if(grandParent != null){
							//親に子供がいるとき
							if(parent.getChildrenSize() != 0){
								if(parent.children[parent.children.length-1] instanceof DataNode){
									DataNode child = (DataNode) parent.children[parent.children.length-1];
									parent.range[0] = child.getMaxID();
								}
								if(parent.children[parent.children.length-1] instanceof FBTNode){
									FBTNode child = (FBTNode) parent.children[parent.children.length-1];
									parent.range[1] = child.range[1];
								}
							}else{
								Node[] childrenOfGrandParentToReplace =  new Node[grandParent.children.length-1];
								System.arraycopy(grandParent.children, 1,childrenOfGrandParentToReplace, 0, grandParent.children.length-1);
							}
							parent = grandParent;
							grandParent = grandParent.parent;
						}
						//祖父がいないのでここで再帰的な更新作業は終わり
						else{
							if(parent.getChildrenSize() != 0){
								if(parent.children[parent.children.length-1] instanceof DataNode){

								}
								if(parent.children[parent.children.length-1] instanceof FBTNode){

								}
								break;
							}else{
								//たぶんここには到達しないはず
								priJap("ここに到達するのはおかしいと思う");
							}
						}
					}

					if(parent.getChildrenSize() == 0){
						Node[] childrenOfGrandParentToReplace =  new Node[grandParent.children.length-1];
						System.arraycopy(grandParent.children, 1,childrenOfGrandParentToReplace, 0, grandParent.children.length-1);
						if(grandParent.children.length > 0){
							//Node child = grandParent.children[grandParent.children.length-1].
						}
					}
					// update parent range.
					else{
						parent.range[1] = nextDataNode.getMaxID();
					}
				}
			 */
			//when toLeft==false
			/*else{
				dataNode = this.leftmost;
				while(dataNode.getNext() != null){
					dataNode = dataNode.getNext();
				}
			}*/
		}
		priJap("データ移動のための分散インデックスのロック解除");
	}

	//public void sendUpdateInfoForDataNodeMove(updateInfoForLoadMoveMessage upInfo);
	public void sendUpdateInfoForDataNodeMove(DataNode[] movedDataNodes, InetSocketAddress dataSender,
			InetSocketAddress dataReceiver,ID[] afterSenderRange, ID[] afterReceiverRange){

	}



	/*
	 *
	 * ##### caution #####
	 * this method is not thread safe!!
	 * you should use this method  locking the "this" object!
	 * and you must be care about dataNodes which you passing as argument,
	 * because this method depends on sequence of dataNodes.
	 *
	 * example:
	 * if now, we assume the array of the dataNodes of this tree is like this
	 *                this.root(=FBTNode)
	 *       FBTNode1               FBTNode2
	 *  dataNode1  dataNode2   dataNode3  dataNode4
	 *
	 *  and if you want to move dataNode1,dataNode2,dataNode3 to previous computer, (if you don't know previous computer, @see getPrevMachine() method )
	 *  you should pass dataNodes arguments like this,
	 *  	[dataNode1, dataNode2, dataNode3] (<- this means array object)
	 *  you "must not" pass like this,
	 *  	[dataNode3, dataNode1, dataNode2]
	 *
	 *  you understand?
	 *
	 *  @address: target computer which you want to pass dataNodes
	 *  @sender: anyway, use MessageReceiver object's method (@see getMessageSender() method)
	 *
	 */
	public DataNode[] moveLeftMostDataNodes(DataNode[] dataNodesToBeRemoved, InetSocketAddress address, MessageSender sender){
		System.out.println("DEBUG_moveLeftMostDataNodes");
		DataNode dataNode = this.leftmost;
		System.out.println(dataNodesToBeRemoved[0].toString());
		System.out.println(  ((FBTNode)dataNode.getParent()).children[0].toString()  );
		try {
			sender.send(dataNodesToBeRemoved.toString(), address);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(int i=0; i< dataNodesToBeRemoved.length;i++){
			//i think dataNode's parent must be FBTNode in FatBtree!!
			if(dataNode.getParent() instanceof FBTNode){
				FBTNode parent = (FBTNode)dataNode.getParent();
				Node[] temp = parent.children.clone();
				// parent get child in which dataNode removed.
				System.arraycopy(temp, 1, parent.children, 0, parent.getChildrenSize()-1);
				dataNode = dataNode.getNext();
			}
		}
		/*while(dataNode != null){
			synchronized (dataNode.getParent()) {
				for(int i=0;i<dataNodes.length;i++){
					DataNode targetNode = dataNodes[i];
					if(dataNode.toLabel().equals(targetNode.toLabel())){
						counterOfFoundDataNode++;
		 * update dataNode's parent index.
						if(dataNode.getParent() instanceof FBTNode){
							FBTNode parent = (FBTNode) dataNode.getParent();
		 * when parent has one child, you should remove this parent too.
		 * and you should update this parent's parent index...
							if(parent.getChildrenSize() == 1){
							}
							// when dataNode is at the most left in dataNode's parent's child.
							// you should update range which the parent has.
							if(parent.range[0].toString().equals(dataNode.getMinID().toString())){
								parent.range[0] = dataNode.getMinID();
							}
							//when dataNode is at the most right in dataNode's parent's child.
							else if(parent.range[1].toString().equals(dataNode.getMaxID().toString())){
								parent.range[1] = dataNode.getMaxID();
							}
							else{
							}
						}
					}
				}
			}
			if(counterOfFoundDataNode == dataNodes.length) break;
			dataNode = dataNode.getNext();
		}
		 */
		return null;
	}






	private Node splitIndex(MessageSender sender, ID[] range, ID id, InetSocketAddress addr, FBTNode fbtNode) {
		boolean s = (range[0] != null
				&& fbtNode.range[1] != null
				&& range[0].compareTo(fbtNode.range[1]) >= 0);
		boolean e = (range[1] != null
				&& fbtNode.range[0] != null
				&& range[1].compareTo(fbtNode.range[0]) <= 0);
		if (s || e) {
			AddressNode addrNode = new AddressNode(null, fbtNode.toLabel());
			return addrNode;
		}

		s = (range[0] == null
				|| (fbtNode.range[0] != null && range[0].compareTo(fbtNode.range[0]) <= 0));
		e = (range[1] == null
				|| (fbtNode.range[1] != null && range[1].compareTo(fbtNode.range[1]) >= 0));
		if (s && e) {
			FBTNode parent = fbtNode.parent;
			for (int i = 0; i < parent.children.length - 1; i++) {
				if (parent.children[i] == fbtNode) {
					AddressNode addrNode = new AddressNode(addr, fbtNode.toLabel());
					synchronized (this.fbtNodes) {
						this.fbtNodes.remove(fbtNode.toLabel());
						this.fbtNodes.put(addrNode.toLabel(), addrNode);
					}
					InetSocketAddress[] temp = parent.shareAddress.toArray(new InetSocketAddress[0]);
					for (InetSocketAddress a: temp) {
						try {
							String res = sender.sendAndReceive("message move " + sender.getResponsePort() + " " +
									parent.toLabel() + " " + i + " " + addrNode.toMessage(), a);
							if (res.compareTo("delete share _self_") == 0) {
								parent.shareAddress.remove(a);
							}
						}
						catch (IOException ioe) {
							ioe.printStackTrace();
						}
					}
					parent.children[i] = addrNode;
					break;
				}
			}
			return fbtNode;
		}

		FBTNode copyNode = new FBTNode(null);
		copyNode.parent = null;
		copyNode.range[0] = fbtNode.range[0];
		copyNode.range[1] = fbtNode.range[1];
		for (int i = 0; i < fbtNode.data.length; i++) {
			copyNode.data[i] = fbtNode.data[i];
		}
		for (int i = 0; i < fbtNode.children.length; i++) {
			if (fbtNode.children[i] != null) {
				Node node = splitIndex(sender, range, id, addr, fbtNode.children[i]);
				copyNode.children[i] = node;
				if (node instanceof FBTNode) ((FBTNode)node).parent = copyNode;
				else if (node instanceof DataNode) ((DataNode)node).setParent(copyNode);
			}
		}
		InetSocketAddress[] temp = fbtNode.shareAddress.toArray(new InetSocketAddress[0]);
		for (InetSocketAddress a: temp) {
			try {
				String res = sender.sendAndReceive("message share " + sender.getResponsePort() + " " + fbtNode.toLabel() +
						" " + addr.getAddress().getHostAddress() + ":" + addr.getPort(), a);
				if (res.compareTo("delete share _self_") == 0) {
					fbtNode.shareAddress.remove(a);
				}
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		copyNode.shareAddress = new ArrayList<InetSocketAddress>(fbtNode.shareAddress);
		copyNode.shareAddress.add(null);
		fbtNode.shareAddress.add(addr);
		return copyNode;
	}

	private Node splitIndex(MessageSender sender, ID[] range, ID id, InetSocketAddress addr, AddressNode addrNode) {
		return addrNode;
	}

	private Node splitIndex(MessageSender sender, ID[] range, ID id, InetSocketAddress addr, DataNode dataNode) {
		ID[] dataRange = dataNode.getRange();

		boolean s = (range[0] != null
				&& dataRange[1] != null
				&& range[0].compareTo(dataRange[1]) >= 0);
		boolean e = (range[1] != null
				&& dataRange[0] != null
				&& range[1].compareTo(dataRange[0]) <= 0);
		if (s || e) {
			AddressNode addrNode = new AddressNode(null, dataNode.toLabel());
			return addrNode;
		}

		s = (range[0] == null
				|| (dataRange[0] != null && range[0].compareTo(dataRange[0]) <= 0));
		e = (range[1] == null
				|| (dataRange[1] != null && range[1].compareTo(dataRange[1]) >= 0));
		if (s && e) {
			FBTNode fbtNode = (FBTNode)dataNode.getParent();
			for (int i = 0; i < fbtNode.children.length - 1; i++) {
				if (fbtNode.children[i] == dataNode) {
					AddressNode addrNode = new AddressNode(addr, dataNode.toLabel());
					synchronized (this.fbtNodes) {
						this.fbtNodes.remove(dataNode.toLabel());
						this.fbtNodes.put(addrNode.toLabel(), addrNode);
					}
					InetSocketAddress[] temp = fbtNode.shareAddress.toArray(new InetSocketAddress[0]);
					for (InetSocketAddress a: temp) {
						try {
							String res = sender.sendAndReceive("message move " + sender.getResponsePort() + " " +
									fbtNode.toLabel() + " " + i + " " + addrNode.toMessage(), a);
							if (res.compareTo("delete share _self_") == 0) {
								fbtNode.shareAddress.remove(a);
							}
						}
						catch (IOException ioe) {
							ioe.printStackTrace();
						}
					}
					fbtNode.children[i] = addrNode;
					break;
				}
			}
			if (dataNode.getPrev() != null) {
				dataNode.getPrev().setNext(null);
			}
			if (dataNode.getNext() != null) {
				dataNode.getNext().setPrev(null);
			}
			if (this.leftmost == dataNode) {
				this.leftmost = dataNode.getNext();
			}
			return dataNode;
		}

		AddressNode addrNode = new AddressNode(null, dataNode.toLabel());
		return addrNode;
	}

	private FBTNode ackUpdate(FBTNode fbtNode) {
		if (fbtNode.children[fbtNode.children.length - 1] != null) {
			synchronized (this.fbtNodes) {
				this.fbtNodes.remove(fbtNode.toLabel());
			}
			FBTNode s = new FBTNode(this);
			for (int i = fbtNode.children.length / 2, j = 0; i < fbtNode.data.length; i++, j++) {
				s.data[j] = fbtNode.data[i];
			}
			for (int i = fbtNode.children.length / 2, j = 0; i < fbtNode.children.length; i++, j++) {
				s.children[j] = fbtNode.children[i];
				if (fbtNode.children[i] instanceof FBTNode) ((FBTNode)fbtNode.children[i]).parent = s;
				else if (fbtNode.children[i] instanceof DataNode) ((DataNode)fbtNode.children[i]).setParent(s);
			}
			for (int i = fbtNode.children.length / 2 + 1; i < fbtNode.data.length; i++) {
				fbtNode.data[i] = null;
			}
			for (int i = fbtNode.children.length / 2; i < fbtNode.children.length; i++) {
				fbtNode.children[i] = null;
			}
			s.shareAddress.addAll(fbtNode.shareAddress);
			FBTNode parent = fbtNode.parent;
			if (parent == null) {
				parent = new FBTNode(this);
				parent.range[0] = fbtNode.range[0];
				parent.range[1] = fbtNode.range[1];
				s.range[0] = s.data[0];
				s.range[1] = s.data[s.children.length / 2 + 1];
				fbtNode.range[1] = s.range[0];
				parent.data[0] = fbtNode.range[0];
				parent.data[1] = s.range[0];
				parent.data[2] = s.range[1];
				parent.children[0] = fbtNode;
				parent.children[1] = s;
				parent.shareAddress.addAll(fbtNode.shareAddress);
				fbtNode.parent = parent;
				s.parent = parent;
				synchronized (this.fbtNodes) {
					this.fbtNodes.put(parent.toLabel(), parent);
					this.fbtNodes.put(fbtNode.toLabel(), fbtNode);
					this.fbtNodes.put(s.toLabel(), s);
				}
				return parent;
			}
			else {
				s.parent = parent;
				s.range[0] = s.data[0];
				if (s.children.length % 2 == 0) { // even number
					s.range[1] = s.data[s.children.length / 2];
				}
				else {		// odd number
					s.range[1] = s.data[s.children.length / 2 + 1];
				}
				fbtNode.range[1] = s.range[0];
				synchronized (this.fbtNodes) {
					this.fbtNodes.put(fbtNode.toLabel(), fbtNode);
					this.fbtNodes.put(s.toLabel(), s);
				}
				for (int i = parent.children.length - 1 - 1; i >= 0; i--) {
					if (parent.children[i] == fbtNode) {
						parent.data[i + 2] = s.range[1];
						parent.data[i + 1] = s.range[0];
						parent.children[i + 1] = s;
						break;
					}
					parent.data[i + 2] = parent.data[i + 1];
					parent.data[i + 1] = parent.data[i];
					parent.children[i + 1] = parent.children[i];
				}
				return ackUpdate(parent);
			}
		}
		return fbtNode;
	}

	private ArrayList<NodeStatus> latchXTree(FBTNode fbtNode) {
		ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
		if (fbtNode.deleteFlag == true) return status;
		for (int i = 0; i < fbtNode.children.length - 1; i++) {
			Node node = fbtNode.children[i];
			if (node == null) {
				break;
			}
			if (node instanceof FBTNode) {
				FBTNode fnode = (FBTNode)node;
				// status.add(fnode.updateData());
				NodeStatus s = fnode.latchXNode();
				if (s == null) {
					endUpdateData(null, status.toArray(new NodeStatus[0]));
					return null;
				}
				status.add(s);
				ArrayList<NodeStatus> temp = latchXTree(fnode);
				if (temp == null) {
					endUpdateData(null, status.toArray(new NodeStatus[0]));
					return null;
				}
				status.addAll(temp);
			}
			else if (node instanceof AddressNode) {
			}
			else if (node instanceof DataNode) {
			}
			else {
				System.err.println("ERROR FatBtree#latchXTree");
			}
		}
		return status;
	}

	public DataNode splitDataNode(MessageSender sender, DataNode dataNode, ID[] range) {
		FBTNode modifyingRoot = null;
		String modifyingLabel = null;
		ArrayList<NodeStatus> status = null;
		ArrayList<InetSocketAddress> latchedRemote = null;
		NodeStatus s = null;
		int loop = 0;
		int aa = 2;
		while (true) {
			if (loop > 0 && Thread.activeCount() > 8) return null;
			s = this.lock.latchXNode();
			if (s == null) {
				loop++;
				if (loop >= aa) return null;
				continue;
			}
			modifyingRoot = (FBTNode)dataNode.getParent();
			while (true) {
				if (modifyingRoot.deleteFlag == true) {
					endUpdateData(sender, s);
					loop++;
					if (loop >= aa) return null;
					continue;
				}
				if (modifyingRoot.isFull() && modifyingRoot.parent != null) {
					modifyingRoot = modifyingRoot.parent;
				}
				else break;
			}


			modifyingLabel = modifyingRoot.toLabel();
			ArrayList<String> remoteIPAddress = new ArrayList<String>();
			for (InetSocketAddress remote: modifyingRoot.shareAddress) {
				String IPAddress = remote.getAddress().getHostAddress() + ":" + remote.getPort();
				remoteIPAddress.add(IPAddress);
			}


			String[] remoteHostAddress = remoteIPAddress.toArray(new String[0]);
			java.util.Arrays.sort(remoteHostAddress);
			String selfBaseHostAddress = (this.prevMachine != null) ?
					this.prevMachine.getAddress().getHostAddress() + ":" + this.prevMachine.getPort() :
						null;
					// ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
					status = null;
					// ArrayList<InetSocketAddress> latchedRemote = new ArrayList<InetSocketAddress>();
					latchedRemote = new ArrayList<InetSocketAddress>();
					boolean selfLatch = false;
					boolean allLatch = true;
					if (remoteHostAddress.length == 0) {
						NodeStatus s0 = modifyingRoot.latchXNode();
						if (s0 == null) {
							endUpdateData(sender, s);
							loop++;
							if (loop >= aa) return null;
							continue;
						}
						FBTNode checkRoot = (FBTNode)dataNode.getParent();
						while (true) {
							if (checkRoot.deleteFlag == true) {
								endUpdateData(sender, s);
								endUpdateData(sender, s0);
								loop++;
								if (loop >= aa) return null;
								continue;
							}
							if (checkRoot.isFull() && checkRoot.parent != null) {
								checkRoot = checkRoot.parent;
							}
							else break;
						}
						if (modifyingRoot != checkRoot) {
							endUpdateData(sender, s);
							endUpdateData(sender, s0);
							loop++;
							if (loop >= aa) return null;
							continue;
						}
						status = latchXTree(modifyingRoot);
						if (status == null) {
							endUpdateData(sender, s);
							endUpdateData(sender, s0);
							selfLatch = false;
							allLatch = false;
							loop++;
							if (loop >= aa) return null;
							continue;
						}
						status.add(s0);
						selfLatch = true;
					}
					for (int i = 0; i < remoteHostAddress.length; i++) {
						if ((selfLatch == false && selfBaseHostAddress == null) ||
								(selfLatch == false && selfBaseHostAddress.compareTo(remoteHostAddress[i]) < 0)) {
							NodeStatus s0 = modifyingRoot.latchXNode();
							if (s0 == null) {
								allLatch = false;
								break;
							}
							FBTNode checkRoot = (FBTNode)dataNode.getParent();
							while (true) {
								if (checkRoot.deleteFlag == true) {
									endUpdateData(sender, s0);
									allLatch = false;
									break;
								}
								if (checkRoot.isFull() && checkRoot.parent != null) {
									checkRoot = checkRoot.parent;
								}
								else break;
							}
							if (allLatch == false) break;
							if (modifyingRoot != checkRoot) {
								endUpdateData(sender, s0);
								allLatch = false;
								break;
							}
							status = latchXTree(modifyingRoot);
							if (status == null) {
								endUpdateData(sender, s0);
								selfLatch = false;
								allLatch = false;
								break;
							}
							status.add(s0);
							selfLatch = true;
						}
						try {
							String items[] = remoteHostAddress[i].split(":");
							InetAddress host = InetAddress.getByName(items[0]);
							InetSocketAddress addr = new InetSocketAddress(host, Integer.parseInt(items[1]));
							String res = sender.sendAndReceive("message latchXTree " + sender.getResponsePort() + " " + modifyingLabel,
									addr);
							if (res.compareTo("false") == 0) {
								System.err.println("MESSAGE remote X latch not success");
								allLatch = false;
								break;
							}
							if (res.compareTo("true0") == 0) {
								modifyingRoot.shareAddress.remove(addr);
								System.err.println("WARNING latchXTree true0");
							}
							else if (res.compareTo("true") == 0) {
								latchedRemote.add(addr);
							}
							else {
								System.err.println("WARNING unknown response latchXTree");
							}
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					}
					if (allLatch == true && selfLatch == false) {
						NodeStatus s0 = modifyingRoot.latchXNode();
						if (s0 == null) {
							allLatch = false;
						}
						else {
							FBTNode checkRoot = (FBTNode)dataNode.getParent();
							while (true) {
								if (checkRoot.deleteFlag == true) {
									endUpdateData(sender, s0);
									allLatch = false;
									break;
								}
								if (checkRoot.isFull() && checkRoot.parent != null) {
									checkRoot = checkRoot.parent;
								}
								else break;
							}
							if (allLatch) {
								if (modifyingRoot != checkRoot) {
									endUpdateData(sender, s0);
									allLatch = false;
								}
								else {
									status = latchXTree(modifyingRoot);
									if (status == null) {
										endUpdateData(sender, s0);
										selfLatch = false;
										allLatch = false;
									}
									else {
										status.add(s0);
										selfLatch = true;
									}
								}
							}
						}
					}
					if (allLatch == false) {
						System.err.println("MESSAGE unlatch remote and local. retry.");
						for (InetSocketAddress addr: latchedRemote) {
							try {
								String res = sender.sendAndReceive("message unlatchXTree " + sender.getResponsePort() + " " + modifyingLabel, addr);
							}
							catch (IOException e) {
								e.printStackTrace();
							}
						}
						endUpdateData(sender, s);
						if (selfLatch) {
							endUpdateData(sender, status.toArray(new NodeStatus[0]));
						}
						loop++;
						if (loop >= aa) return null;
						try {
							Thread.sleep((Main.random.nextInt(8) == 0 ? 1 : 1000));
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
					break;
		}



		synchronized (this.fbtNodes) {
			this.fbtNodes.remove(dataNode.toLabel());
		}
		DataNode splitDataNode = dataNode.split();
		ID minID = splitDataNode.getMinID();
		if (minID != null) {
			ID[] sr = new ID[2];
			sr[0] = minID;
			ID[] r = dataNode.getRange();
			sr[1] = r[1];
			splitDataNode.setRange(sr);
			r[1] = minID;
			dataNode.setRange(r);
		}
		else {
			ID[] sr = new ID[2];
			sr[0] = range[0];
			sr[1] = range[1];
			splitDataNode.setRange(sr);
			ID[] r = dataNode.getRange();
			r[1] = range[0];
			dataNode.setRange(r);
		}
		synchronized (this.fbtNodes) {
			this.fbtNodes.put(dataNode.toLabel(), dataNode);
			this.fbtNodes.put(splitDataNode.toLabel(), splitDataNode);
		}
		FBTNode parent = (FBTNode)dataNode.getParent();
		for (int i = parent.children.length - 1 - 1; i >= 0; i--) {
			if (parent.children[i] == dataNode) {
				ID[] sr = splitDataNode.getRange();
				parent.data[i + 2] = sr[1];
				parent.data[i + 1] = sr[0];
				parent.children[i + 1] = splitDataNode;
				break;
			}
			parent.data[i + 2] = parent.data[i + 1];
			parent.data[i + 1] = parent.data[i];
			parent.children[i + 1] = parent.children[i];
		}
		FBTNode modifiedRoot = ackUpdate(parent);
		// InetSocketAddress[] temp = trueModifyingRoot.shareAddress.toArray(new InetSocketAddress[0]);
		InetSocketAddress[] temp = modifyingRoot.shareAddress.toArray(new InetSocketAddress[0]);
		ArrayList<InetSocketAddress> sended = new ArrayList<InetSocketAddress>();
		for (InetSocketAddress addr: temp) {
			try {
				String res = sender.sendAndReceive("message replace " + sender.getResponsePort() + " " +
						addr.getAddress().getHostAddress() + ":" + addr.getPort() + " " +
						modifyingLabel + " " + modifiedRoot.toMessage(), addr);
				String[] items = res.split(" ");
				for (int i = 2; i < items.length; i++) {
					if (items[i].compareTo("_self_") == 0) {
						modifyingRoot.shareAddress.remove(addr);
						continue;
					}
					Node node;
					synchronized (this.fbtNodes) {
						node = this.fbtNodes.get(items[i]);
					}
					if (node instanceof FBTNode) {
						FBTNode fnode = (FBTNode)node;
						fnode.shareAddress.remove(addr);
						InetSocketAddress[] temp00 = fnode.shareAddress.toArray(new InetSocketAddress[0]);
						for (InetSocketAddress re: temp00) {
							if (!sended.contains(re)) continue;
							String res0 = sender.sendAndReceive("message unshare " + sender.getResponsePort() + " " + fnode.toLabel() + " " + addr.getAddress().getHostAddress() + ":" + addr.getPort(), re);
							if (res0.compareTo("delete share _self_") == 0) {
								fnode.shareAddress.remove(re);
							}
						}
					}
				}
				sended.add(addr);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (modifyingRoot != modifiedRoot) {
			this.root = modifiedRoot;
			modifyingLabel = modifyingRoot.toLabel();
		}
		// if (trueModifyingRoot != modifiedRoot) {
		//     this.root = modifiedRoot;
		//     modifyingLabel = trueModifyingRoot.toLabel();
		// }
		FatBtree fbt = new FatBtree();
		fbt.range = this.range;
		fbt.root = modifiedRoot;
		fbt.fbtNodes = new HashMap<String,Node>();
		HashMap<String,Node> deleteNodeAdd = new HashMap<String,Node>();
		adjustIndex(fbt, fbt.root, null, null, null, deleteNodeAdd);
		synchronized (this.fbtNodes) {
			this.fbtNodes.putAll(fbt.fbtNodes);
		}
		for (String label: deleteNodeAdd.keySet()) {
			Node node;
			synchronized (this.fbtNodes) {
				node = this.fbtNodes.remove(label);
			}
			if (node instanceof FBTNode) {
				FBTNode fbtNode = (FBTNode)node;
				for (InetSocketAddress a: fbtNode.shareAddress) {
					try {
						String res = sender.sendAndReceive("message unshare " + sender.getResponsePort() + " " + fbtNode.toLabel(), a);
					}
					catch (IOException ioe) {
						ioe.printStackTrace();
					}
				}
			}
		}

		// for (InetSocketAddress addr: remote) {
		//     try {
		// 	String res = sender.sendAndReceive("message unlatchXTree " + sender.getResponsePort() + " " +
		// 					   modifyingLabel, addr);
		// 	// if (res.compareTo("false") == 0) {
		// 	// 	System.err.println("MESSAGE remote X latch not success");
		// 	// 	flag = false;
		// 	// 	break;
		// 	// }
		// 	// remoteLatch.add(addr);
		//     }
		//     catch (IOException e) {
		// 	e.printStackTrace();
		//     }
		// }

		// for (InetSocketAddress addr: remote) {
		for (InetSocketAddress addr: latchedRemote) {
			try {
				String res = sender.sendAndReceive("message unlatchXTree " + sender.getResponsePort() + " " +
						modifyingLabel + " lock", addr);
				// if (res.compareTo("false") == 0) {
				// 	System.err.println("MESSAGE remote X latch not success");
				// 	flag = false;
				// 	break;
				// }
				// remoteLatch.add(addr);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		endUpdateData(sender, status.toArray(new NodeStatus[0]));
		endUpdateData(sender, s);
		//
		endUpdateData(sender, s);
		//

		System.err.println("MESSAGE splitDataNode success");

		return splitDataNode;
	}

	public FatBtree splitResponsibleRange(MessageSender sender, ID[] range, ID id, NodeStatus[] status, InetSocketAddress addr) {
		boolean f = false;
		DataNode last = this.leftmost;
		while (true) {
			ID[] r = last.getRange();
			boolean s = (range[0] == null || (r[0] != null && range[0].compareTo(r[0]) <= 0));
			boolean e = (range[1] == null || (r[1] != null && range[1].compareTo(r[1]) >= 0));
			if (s && e) {
				f = true;
				break;
			}
			if (last.getNext() == null) {
				break;
			}
			last = last.getNext();
		}
		if (f == false) {
			// InitCommand only, the others will occur error
			if (range[0] != null && (this.range[0] == null || this.range[0].compareTo(range[0]) < 0)) {
				DataNode splitDataNode = null;
				while (true) {
					splitDataNode = splitDataNode(sender, last, range);
					if (splitDataNode != null) break;
				}
				range = splitDataNode.getRange();
			}
			else {
				System.err.println("WARINING FatBtree#splitResponsibleRange");
			}
		}

		FatBtree splitIndex = new FatBtree();
		splitIndex.range = range;
		splitIndex.root = (FBTNode)splitIndex(sender, range, id, addr, this.root);

		if (this.range[0] == null || this.range[0].compareTo(range[0]) < 0) {
			splitIndex.nextMachine = this.nextMachine;
			this.range[1] = range[0];
			this.nextMachine = addr;
		}
		else {
			splitIndex.prevMachine = this.prevMachine;
			this.range[0] = range[1];
			this.prevMachine = addr;
		}

		return splitIndex;
	}

	public String toString() {
		// synchronized (this) {
		StringBuilder sb = new StringBuilder();
		sb.append("******** " + NAME + " ********" + Shell.CRLF);
		sb.append("  RANGE: " + this.range[0] + " " + this.range[1] + Shell.CRLF);
		sb.append("  TREE:" + Shell.CRLF);
		for (Map.Entry<String,Node> item: this.fbtNodes.entrySet()) {
			sb.append("  ======== " + item.getKey() + " ========" + Shell.CRLF + item.getValue() + Shell.CRLF);
		}
		sb.append("  LEFTMOST: " + ((this.leftmost != null) ? this.leftmost.toLabel() : null) + Shell.CRLF);
		sb.append("  NEIGHBOR: " +
				((this.prevMachine != null) ?
						this.prevMachine.getAddress().getHostAddress() + ":" + this.prevMachine.getPort() :
							null) +
							" <-> " +
							((this.nextMachine != null) ?
									this.nextMachine.getAddress().getHostAddress() + ":" + this.nextMachine.getPort() :
										null));
		return sb.toString();
		// }
	}





	@Override
	protected Message updateIndex(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target) {
				return null;
		// TODO 自動生成されたメソッド・スタブ

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

final class FBTNode extends MyUtil implements Node{
	private static final String NAME = "FBTNode";
	public String getName() {return NAME;}

	private static final int MAX_CHILDREN_NODES = 64;
	// private static final int MAX_CHILDREN_NODES = 8;
	private static final int MAX_ID_PER_DATANODE = 32;

	public FatBtree tree;
	public FBTNode parent;
	public ID[] range;
	public ID[] data;
	public Node[] children;
	public ArrayList<InetSocketAddress> shareAddress;
	public volatile boolean deleteFlag;

	private int[] status;

	public FBTNode(FatBtree tree) {
		this.tree = tree;
		this.parent = null;
		this.range = new ID[2];
		this.data = new ID[MAX_CHILDREN_NODES + 2];
		this.children = new Node[MAX_CHILDREN_NODES + 1];
		this.shareAddress = new ArrayList<InetSocketAddress>();
		this.deleteFlag = false;

		this.status = LatchUtil.newLatch();
	}



	/*
	 * 自分が見やすくなるようにログをはかせます
	 * by tokuda
	 */
	public void toLog(){
		pri("FBTNODE : FOR DEBUG : ");
		pri("CHILDREN : ");
		for(Node n : this.children){
			pri("child:");
			System.out.println(n.toMessage());
		}

		pri("DATA : ");
		for(ID id: this.data){
			pri("id : ");
			pri(id.toMessage());
		}

		pri("SHARE ADDRESS:");
		for(InetSocketAddress addr: this.shareAddress){
			pri("host name : "+addr.getHostName() + "; port : " + addr.getPort() + " ; toString : " +addr.toString());
		}
	}


	public boolean equals(Object o){
		if(o == null){return false;}

		if(o instanceof FBTNode){
			if(((FBTNode) o).toLabel().equals(this.toLabel()) == true){
				return true;
			}
		}

		return false;

	}


	public static FBTNode _toInstance(String[] text, ID id, FatBtree fbt, FatBtree mainTree) {
		int index = 0;
		FBTNode fbtNode = new FBTNode(mainTree);
		for (int i = 0; i < fbtNode.data.length; i++) {
			if (text[index].compareTo("") != 0) {
				fbtNode.data[i] = id.toInstance(text[index]);
				index++;
			}
			else {
				fbtNode.data[i] = null;
				index++;
			}
		}
		fbtNode.range[0] = fbtNode.data[0];
		int num = 0;
		for (int i = 0; i < fbtNode.children.length; i++) {
			if (text[index].compareTo("") != 0) {
				String name = text[index]; index++;
				int n = Integer.parseInt(text[index]); index++;
				String[] temp = new String[n];
				System.arraycopy(text, index, temp, 0, n);
				Node node = null;
				if (name.compareTo("FBTNode") == 0) {
					node = FBTNode._toInstance(temp, id, fbt, mainTree);
					((FBTNode)node).parent = fbtNode;
				}
				else if (name.compareTo("AddressNode") == 0) {
					node = AddressNode._toInstance(temp, id);
					fbt.fbtNodes.put(((AddressNode)node).toLabel(), node);
				}
				else if (name.compareTo("DataNode") == 0) {
					node = DataNode.toInstance(temp, id);
					((DataNode)node).setParent(fbtNode);
					ID[] r = new ID[2];
					r[0] = fbtNode.data[i];
					r[1] = fbtNode.data[i + 1];
					((DataNode)node).setRange(r);
					if (fbt.leftmost == null) {
						fbt.leftmost = (DataNode)node;
					}
					fbt.fbtNodes.put(((DataNode)node).toLabel(), node);
					if (n == 0) index++;
				}
				else {
					System.err.println("ERROR FBTNode#toInstance");
				}
				fbtNode.children[i] = node; index += n;
				num = i;
			}
			else {
				fbtNode.children[i] = null;
				index++;
			}
		}
		fbtNode.range[1] = fbtNode.data[num + 1];
		ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		for (; index < text.length - 1;) {
			if (text[index].compareTo("_null_") == 0) {
				addrs.add(null);
				index++;
				continue;
			}
			String[] temp = text[index].split(":");
			try {
				InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName(temp[0]), Integer.parseInt(temp[1]));
				addrs.add(addr);
				index++;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		fbtNode.shareAddress = addrs;

		fbt.fbtNodes.put(fbtNode.toLabel(), fbtNode);

		return fbtNode;
	}

	public String toMessage() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.data.length; i++) {
			if (this.data[i] != null) {
				sb.append(this.data[i].toMessage());
			}
			sb.append(" ");
		}
		for (int i = 0; i < this.children.length; i++) {
			if (this.children[i] != null) {
				sb.append(this.children[i].toMessage());
			}
			sb.append(" ");
		}
		for (InetSocketAddress addr: shareAddress) {
			if (addr != null) {
				sb.append(addr.getAddress().getHostAddress() + ":" + addr.getPort() + " ");
			}
			else {
				sb.append("_null_ ");
			}
		}
		sb.append("_end_");

		String msg = sb.toString();
		String[] temp = msg.split(" ");
		return NAME + " " + temp.length + " " + msg;
	}

	public boolean isFull() {
		int num = this.getChildrenSize();
		return (num >= MAX_CHILDREN_NODES);
	}

	public int getChildrenSize() {
		int i = 0;
		for (i = 0; i < this.MAX_CHILDREN_NODES; i++) {
			if (this.children[i] == null) break;
		}
		return i;
	}





	public int getNumberOfLeafNodes(){
		int count=0;
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode){
				count++;
			}
		}
		return count;
	}


	public int getNumberOfInterOrLeafNodes(){
		int count=0;
		for(int i=0; i< this.getChildrenSize(); i++){
			if(this.children[i] instanceof DataNode
					|| this.children[i] instanceof FBTNode){
				count++;
			}
		}
		return count;
	}

	/*
	 * 子供のノードのうちで中間ノードの数を返します。
	 * 実装的にはFBTNodeのchildrenに入っているFBTNodeの数を返します。
	 */
	public int getNumberOfInterNodes(){
		int count = 0;
		for(int i=0; i< this.getChildrenSize();i++){
			if(this.children[i] instanceof FBTNode){
				count++;
			}
		}
		return count;
	}



	/*
	 * もしこのFBTノードが渡されたfnと”等しい”ものを所持していたら
	 * 渡されたアドレスノードと置き換えます。
	 */
	public void replaceFbtNodeToLink(FBTNode fn, InetSocketAddress linkTo){
		for(int i=0; i< parent.getChildrenSize();i++ ){
			if(fn.equals(parent.children[i]) == true){
				parent.children[i] = new AddressNode(linkTo, fn.toLabel());
			}
		}
	}


	/*
	 * もしこのFNTNodeが渡されたdnと”等しい”ものを所持していたら
	 * 渡されたアドレスノードと置き換えます。
	 * B-link構造も考慮して置き換えます。
	 */
	public void replaceDataNodeToAddressNode(DataNode dn, AddressNode an){
		for(int i=0; i< getChildrenSize();i++ ){
			if(dn.equals(children[i]) == true){
				children[i] = an;
				/*
				 * B-link構造が壊れないようにリンクを更新する
				 * change the link of both (left or right) of data nodes.
				 */
				if(dn.getNext() == null && dn.getPrev() != null){
					dn.getPrev().setNext(null);
				}
				else if(dn.getNext() != null && dn.getPrev() == null){
					dn.getNext().setPrev(null);
				}
				else if(dn.getNext() != null && dn.getPrev() != null){
					dn.getNext().setPrev(null);
					dn.getPrev().setNext(null);
				}
			}
		}
	}






	public void ackUpdate(MessageSender sender, Node childrenNode) {
		DataNode dataNode = (DataNode)childrenNode;
		int size = dataNode.size();
		if (size <= MAX_ID_PER_DATANODE) return;
		if (Main.random.nextInt(64) == 0 && Thread.activeCount() < 16)
			this.tree.splitDataNode(sender, dataNode, null);
	}

	public boolean isFree() {
		synchronized (this.status) {
			return
					this.status[LatchUtil.IS] == 0 &&
					this.status[LatchUtil.IX] == 0 &&
					this.status[LatchUtil.S] == 0 &&
					this.status[LatchUtil.SIX] == 0 &&
					this.status[LatchUtil.X] == 0;
		}
	}

	public NodeStatus latchXNode() {
		// for (int i = 0; i < 110; i++) {
		for (int i = 0; i < 10; i++) {
			synchronized (this.status) {
				if (this.deleteFlag) return null;
				if (this.status[LatchUtil.IS] == 0 &&
						this.status[LatchUtil.IX] == 0 &&
						this.status[LatchUtil.S] == 0 &&
						this.status[LatchUtil.SIX] == 0 &&
						this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.X]++;
					return new NodeStatus(this, LatchUtil.X);
				}
			}
			// try {Thread.sleep((i < 100) ? 100 : 1000);}
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
		}
		synchronized (this.status) {
			if (this.deleteFlag) return null;
			if (this.status[LatchUtil.IS] == 0 &&
					this.status[LatchUtil.IX] == 0 &&
					this.status[LatchUtil.S] == 0 &&
					this.status[LatchUtil.SIX] == 0 &&
					this.status[LatchUtil.X] == 0) {
				this.status[LatchUtil.X]++;
				return new NodeStatus(this, LatchUtil.X);
			}
		}
		return null;
	}

	public NodeStatus latchISNode() {
		// while (true) {
		//
		for (int i = 0; i < 50; i++) {
			//
			synchronized (this.status) {
				if (this.deleteFlag) return null;
				if (this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.IS]++;
					// break;
					return new NodeStatus(this, LatchUtil.IS);
				}
			}
			//
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
			//
		}
		//
		synchronized (this.status) {
			this.status[LatchUtil.X] = 0;
			this.status[LatchUtil.IS] = 1;
		}
		//
		return new NodeStatus(this, LatchUtil.IS);
	}

	public NodeStatus latchIXNode() {
		// while (true) {
		for (int i = 0; i < 50; i++) {
			//
			synchronized (this.status) {
				if (this.deleteFlag) return null;
				if (this.status[LatchUtil.S] == 0 &&
						this.status[LatchUtil.SIX] == 0 &&
						this.status[LatchUtil.X] == 0) {
					this.status[LatchUtil.IX]++;
					// break;
					return new NodeStatus(this, LatchUtil.IX);
				}
			}
			//
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
			//
		}
		synchronized (this.status) {
			this.status[LatchUtil.S] = 0;
			this.status[LatchUtil.SIX] = 0;
			this.status[LatchUtil.X] = 0;
			this.status[LatchUtil.IX] = 1;
		}
		return new NodeStatus(this, LatchUtil.IX);
	}

	public NodeStatus searchData() {
		// while (true) {
		for (int i = 0; i < 50; i++) {
			synchronized (this.status) {
				if (this.status[LatchUtil.IX] == 0 &&
						this.status[LatchUtil.SIX] == 0 &&
						this.status[LatchUtil.X] == 0) {
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
			this.status[LatchUtil.IX] = 0;
			this.status[LatchUtil.SIX] = 0;
			this.status[LatchUtil.X] = 0;
			this.status[LatchUtil.S] = 1;
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
		for (int i = 0; i < 50; i++) {
			synchronized (this.status) {
				if (this.status[LatchUtil.IS] == 0 &&
						this.status[LatchUtil.IX] == 0 &&
						this.status[LatchUtil.S] == 0 &&
						this.status[LatchUtil.SIX] == 0 &&
						this.status[LatchUtil.X] == 0) {
					// break;
					this.status[LatchUtil.X]++;
					return new NodeStatus(this, LatchUtil.X);
				}
			}
			//
			try {Thread.sleep(100);}
			catch (InterruptedException e) {}
			//
		}
		synchronized (this.status) {
			this.status[LatchUtil.IS] = 0;
			this.status[LatchUtil.IX] = 0;
			this.status[LatchUtil.S] = 0;
			this.status[LatchUtil.SIX] = 0;
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

	public String toLabel() {
		return NAME +
				((this.range[0] != null) ? this.range[0].toMessage() : "") + "," +
				((this.range[1] != null) ? this.range[1].toMessage() : "");
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(NAME + ": " + this.toLabel() + Shell.CRLF);
		sb.append("  parent: " + ((this.parent != null) ? this.parent.toLabel() : null) + Shell.CRLF);
		sb.append("  range: " + this.range[0] + " " + this.range[1] + Shell.CRLF);
		for (int i = 0; i < this.data.length; i++) {
			sb.append(this.data[i] + ", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append(Shell.CRLF);
		sb.append("  children: ");
		for (int i = 0; i < this.children.length; i++) {
			if (this.children[i] != null) {
				if (this.children[i] instanceof FBTNode)
					sb.append(((FBTNode)this.children[i]).toLabel() + ", ");
				else if (this.children[i] instanceof DataNode)
					sb.append(((DataNode)this.children[i]).toLabel() + ", ");
				else
					sb.append(((AddressNode)this.children[i]).toLabel() + ", ");
			}
			else {
				sb.append("null, ");
			}
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append(Shell.CRLF);
		sb.append("  share: ");
		for (InetSocketAddress addr: this.shareAddress) {
			sb.append(addr.getAddress().getHostName() + ":" + addr.getPort() + " ");
		}
		sb.replace(sb.length() - 1, sb.length(), Shell.CRLF);
		sb.append("  status: ");
		for (int i = 0; i < status.length; i++) {
			sb.append(status[i] + ", ");
		}
		sb.delete(sb.length() - 2, sb.length());

		return sb.toString();
	}
}
