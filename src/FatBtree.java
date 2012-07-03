// FatBtree.java

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class FatBtree implements DistributedIndex {
    private static final String NAME = "FatBtree";
    public String getName() {return NAME;}

    private ID[] range;
    private FBTNode root;
    protected DataNode leftmost;
    protected HashMap<String,Node> fbtNodes; // Node.toLabel(), Node instance (FBTNode, AddressNode, DataNode)
    protected InetSocketAddress nextMachine;
    protected InetSocketAddress prevMachine;

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
	    String name = text[4];
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
		AddressNode anode = deleteFBTNodeChildren(fbt, fnode, current, deleteNode);
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

    public InetSocketAddress getPrevMachine() {
	return this.prevMachine;
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

    public NodeStatus[] searchData(MessageSender sender, ID[] range) throws IOException {
	// System.err.println("DEPLICATE FatBtree#searchData(ID[])");
	ArrayList<NodeStatus> status = new ArrayList<NodeStatus>();
	DataNode node = null;
	if (range[0] == null) {
	    node = getFirstDataNode();
	}
	else {
	    // node = (DataNode)searchKey(sender, range[0]);
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
	    if (count > 5) return null; count++;
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
		    // if (res.compareTo("true0") == 0) {
		    // 	modifyingRoot.shareAddress.remove(addr);
		    // }
		    // else if (res.compareTo("true") == 0) {
			// latchedRemote.add(addr);
		    // }
		    // else {
		    // 	System.err.println("WARNING unknown response latchXTree");
		    // }
		    // 
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



	// FBTNode trueModifyingRoot = null;
	// String modifyingLabel = null;
	// ArrayList<NodeStatus> status = null;
	// ArrayList<InetSocketAddress> remote = null;
	// NodeStatus s = null;
	// int loop = 0;
	// // int aa = Main.random.nextInt(5);
	// int aa = 2;
	// while (true) {
	//     s = this.lock.latchXNode();
	//     if (s == null) {
	// 	if (loop >= aa) return null;
	// 	loop++;
	// 	continue;
	//     }
	//     trueModifyingRoot = null;
	//     NodeStatus trueRootStatus = null;
	//     while (true) {
	// 	FBTNode modifyingRoot = (FBTNode)dataNode.getParent();
	// 	while (true) {
	// 	    if (modifyingRoot.isFull() && modifyingRoot.parent != null) {
	// 		modifyingRoot = modifyingRoot.parent;
	// 	    }
	// 	    else {
	// 		break;
	// 	    }
	// 	}
	// 	if (trueModifyingRoot == null) {
	// 	    trueModifyingRoot = modifyingRoot;
	// 	    // trueRootStatus = trueModifyingRoot.updateData();
	// 	    // 
	// 	    trueRootStatus = trueModifyingRoot.latchXNode();
	// 	    if (trueRootStatus == null) {
	// 		trueModifyingRoot = null;
	// 	    }
	// 	    // 
	// 	    continue;
	// 	}
	// 	if (trueModifyingRoot == modifyingRoot) {
	// 	    break;
	// 	}
	// 	endUpdateData(sender, trueRootStatus);
	// 	trueModifyingRoot = modifyingRoot;
	// 	// trueRootStatus = trueModifyingRoot.updateData();
	// 	// 
	// 	trueRootStatus = trueModifyingRoot.latchXNode();
	// 	if (trueRootStatus == null) {
	// 	    trueModifyingRoot = null;
	// 	}
	// 	// 
	//     }
	//     modifyingLabel = trueModifyingRoot.toLabel();
	//     status = latchXTree(trueModifyingRoot);
	//     status.add(trueRootStatus);
	//     remote = new ArrayList<InetSocketAddress>();
	//     boolean flag = true;
	//     InetSocketAddress[] temp = trueModifyingRoot.shareAddress.toArray(new InetSocketAddress[0]);
	//     for (InetSocketAddress addr: temp) {
	// 	try {
	// 	    String res = sender.sendAndReceive("message latchXTree " + sender.getResponsePort() + " " +
	// 					       modifyingLabel, addr);
	// 	    if (res.compareTo("false") == 0) {
	// 		System.err.println("MESSAGE remote X latch not success");
	// 		flag = false;
	// 		break;
	// 	    }
	// 	    remote.add(addr);
	// 	}
	// 	catch (IOException e) {
	// 	    e.printStackTrace();
	// 	}
	//     }
	//     if (flag == false) {
	// 	System.err.println("MESSAGE unlatch remote and local. retry.");
	// 	for (InetSocketAddress addr: remote) {
	// 	    try {
	// 		String res = sender.sendAndReceive("message unlatchXTree " + sender.getResponsePort() + " " +
	// 						   modifyingLabel, addr);
	// 		// if (res.compareTo("false") == 0) {
	// 		// 	System.err.println("MESSAGE remote X latch not success");
	// 		// 	flag = false;
	// 		// 	break;
	// 		// }
	// 		// remoteLatch.add(addr);
	// 	    }
	// 	    catch (IOException e) {
	// 		e.printStackTrace();
	// 	    }
	// 	}
	// 	endUpdateData(sender, status.toArray(new NodeStatus[0]));
	// 	endUpdateData(sender, s);
	// 	if (loop >= aa) return null;
	// 	try {
	// 	    // Thread.sleep(Main.random.nextInt(10 * 1000) / ((loop < 10) ? 10 - loop : 1));
	// 	    Thread.sleep((Main.random.nextInt(8) == 0 ? 1 : 3 * 1000));
	// 	}
	// 	catch (InterruptedException e) {
	// 	    e.printStackTrace();
	// 	}
	// 	loop++;
	// 	continue;
	//     }
	//     break;
	// }

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
}

final class FBTNode implements Node {
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

    // public void ackSearch(Node node) {
    // }

    // private FBTNode split() {
    // }

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
