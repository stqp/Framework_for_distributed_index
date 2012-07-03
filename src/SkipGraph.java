// SkipGraph.java

import java.util.ArrayList;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class SkipGraph implements DistributedIndex {
    private static final String NAME = "SkipGraph";
    public String getName() {return NAME;}

    private final static int MAX_LEVEL = 5;
    private final static int MV_LENGTH = 8;

    private ID id;
    private SkipGraphNode rootNode;
    private SkipGraphNode[] interNodes;
    private int[] m;
    // private boolean deleteFlag;

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
	    // this.deleteFlag = false;

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
	    // this.deleteFlag = deleteFlag;

	    this.store = distIndex.store;
	}
    }

    public SkipGraph toInstance(String[] text, ID id) {
	return SkipGraph._toInstance(text, id);
    }

    public static SkipGraph _toInstance(String[] text, ID id) {
	int i = 0;
	ID sgid = id.toInstance(text[i]); i++;
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
		    System.err.println("MESSAGE status id");
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
		    System.err.println("MESSAGE status id");
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
		    System.err.println("MESSAGE status id");
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
		    System.err.println("MESSAGE status id");
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
	// System.err.println("DEPLICATE SkipGraph#updateData(ID[])");
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
