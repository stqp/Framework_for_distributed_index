// PRing.java

import java.util.ArrayList;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class PRing implements DistributedIndex {
    private final static String NAME = "PRing";
    public String getName() {return NAME;}

    private ID id;

    private PRingNode rootNode;
    private ArrayList<PRingNode> interNodes;

    private LocalStore store;

    public PRing() {}

    public String handleMessge(InetAddress host, ID id, String[] text) {
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
	    // PRingNode level0 = this.interNodes.get(0);
	    // this.interNodes = new ArrayList<PRingNode>();
	    // this.interNodes.add(level0);
	    // if (addrNodes[addrNodes.length - 1].getAddress() != null) {
	    // 	if (this.interNodes.size() == 1) {
	    // 	    PRingNode newNode = new PRingNode(1);
	    // 	    newNode.childNode = this.interNodes.get(0);
	    // 	    newNode.successors[0] = addrNodes[addrNodes.length - 1];
	    // 	    this.interNodes.add(newNode);
	    // 	    this.rootNode = newNode;
	    // 	    // this.interNodes.get(1).successors[0] = addrNodes[addrNodes.length - 1];
	    // 	}
	    // 	// this.interNodes.get(1).successors[0] = addrNodes[addrNodes.length - 1];
	    // }

	    // boolean flag = false;
	    // for (int i = 0; i < this.interNodes.size(); i++) {
	    // 	AddressNode[] temp = this.interNodes.get(i).successors;
	    // 	for (AddressNode a: temp) {
	    // 	    if (a.getAddress() == null) break;
	    // 	}
	    // }

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
	    splitedIndex.store = this.store.splitResponsibleRange(range, status); // TODO: split fetch, easy

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
