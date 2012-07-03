// TreeLocalStore.java

import java.util.ArrayList;

// note: not use synchronized (this) {} in each function
// will use alternate exclusion access control or not lock this at all
public final class TreeLocalStore implements LocalStore {
    private final static String NAME = "TreeLocalStore";
    public String getName() {return NAME;}

    private TreeNode rootObj;
    private TreeNode root;
    private DataNode leftmost;

    public TreeLocalStore() {
	this.rootObj = new TreeNode(this);
	this.root = new TreeNode(this);
	DataNode dataNode = new DataNode(this.root, null, null);
	this.root.children[0] = dataNode;
	this.leftmost = dataNode;
    }


    public TreeLocalStore toInstance(String[] text, ID id) {
	return TreeLocalStore._toInstance(text, id);
    }

    public static TreeLocalStore _toInstance(String[] text, ID id) {
	int i = 0;
	ArrayList<DataNode> data = new ArrayList<DataNode>();
	while (i < text.length) {
	    if (text[i].compareTo(DataNode.NAME) == 0) {
		i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		DataNode dataNode = DataNode.toInstance(temp, id); i += n;
		data.add(dataNode);
	    }
	    else {
		System.err.println("WARNING TreeLocalStore#_toInstance");
	    }
	}

	// need not exclusion access control
	TreeLocalStore store = new TreeLocalStore();
	for (DataNode dataNode: data) {
	    ID[] idArray = dataNode.getAll();
	    for (ID tempID: idArray) {
		DataNode temp = store.searchKey(tempID);
		temp.add(null, tempID);
	    }
	}

	return store;
    }

    public String toMessage() {
	StringBuilder sb = new StringBuilder();
	DataNode current = this.leftmost;
	while (current != null) {
	    sb.append(current.toMessage() + " ");
	    current = getNextDataNode(current);
	}
	sb.delete(sb.length() - 1, sb.length());

	String msg = sb.toString();
	return NAME + " " + msg.split(" ").length + " " + msg;
    }

    public DataNode getFirstDataNode() {
	return this.leftmost;
    }

    public DataNode getNextDataNode(DataNode dataNode) {
	return dataNode.getNext();
    }

    public ID[] getRange(DataNode dataNode) {
	TreeNode treeNode = (TreeNode)dataNode.getParent();
	return treeNode.getRange(dataNode);
    }

    public DataNode searchKey(ID key) {
	NodeStatus status = this.rootObj.searchData();
	Node current = this.root;
	while (current instanceof TreeNode) {
	    TreeNode treeNode = (TreeNode)current;
	    for (int i = 0; i < treeNode.data.length; i++) {
		if (treeNode.data[i] == null || key.compareTo(treeNode.data[i]) < 0) {
		    current = treeNode.children[i];
		    break;
		}
	    }
	}
	endSearchData(status);
	return (DataNode)current;
    }

    public NodeStatus searchData(DataNode dataNode) {
	return this.rootObj.searchData();
    }

    public NodeStatus[] searchData(DataNode[] dataNodes) {
	NodeStatus[] status = new NodeStatus[1];
	status[0] = this.rootObj.searchData();
	return status;
    }

    public void endSearchData(NodeStatus status) {
	status.getNode().endSearchData(status);
    }


    public DataNode updateKey(ID key) {
	NodeStatus status = this.rootObj.updateData();
	Node current = this.root;
	while (current instanceof TreeNode) {
	    TreeNode treeNode = (TreeNode)current;
	    for (int i = 0; i < treeNode.data.length; i++) {
		if (treeNode.data[i] == null || key.compareTo(treeNode.data[i]) < 0) {
		    current = treeNode.children[i];
		    break;
		}
	    }
	}
	endUpdateData(status);
	return (DataNode)current;
    }

    public NodeStatus updateData(DataNode dataNode) {
	return this.rootObj.updateData();
    }

    public NodeStatus[] updateData(DataNode[] dataNodes) {
	NodeStatus[] status = new NodeStatus[1];
	status[0] = this.rootObj.updateData();
	return status;
    }

    public void endUpdateData(NodeStatus status) {
	status.getNode().endUpdateData(status);
    }


    public LocalStore splitResponsibleRange(ID[] range, NodeStatus[] status) {
	DataNode current = this.searchKey(range[0]);

	TreeLocalStore store = new TreeLocalStore();
	while (current != null) {
	    ID[] r = this.getRange(current);
	    if (range[1] != null && r[0] != null && r[0].compareTo(range[1]) >= 0) break;
	    ID[] idArray = current.getAll();
	    for (ID id: idArray) {
		if ((range[0] == null || id.compareTo(range[0]) >= 0) && (range[1] == null || id.compareTo(range[1]) < 0)) {
		    DataNode temp = store.updateKey(id);
		    temp.add(null, id);
		    current.remove(null, id);
		}
	    }
	    current = current.getNext();
	}

	return store;
    }

    public TreeNode setRoot(TreeNode root) {
	TreeNode old = this.root;
	this.root = root;
	return old;
    }

    public DataNode setLeftmost(DataNode dataNode) {
	DataNode old = this.leftmost;
	this.leftmost = dataNode;
	return old;
    }
}

final class TreeNode implements Node {
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
	int center = MAX_CHILDREN_NODES / 2;
	for (int i = 0, j = center; j < MAX_CHILDREN_NODES; i++, j++) {
	    treeNode.data[i] = this.data[j]; this.data[j] = null;
	}
	this.data[center - 1] = null;
	for (int i = 0, j = center; j < MAX_CHILDREN_NODES + 1; i++, j++) {
	    treeNode.children[i] = this.children[j]; this.children[j] = null;
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
	// int index = -1;
	// for (int i = 0, j = 0; i < MAX_CHILDREN_NODES; i++, j++) {
	//     if (this.children[j] == childrenNode) {
	// 	index = i;
	// 	i++;
	// 	if (i >= MAX_CHILDREN_NODES) break;
	//     }
	//     this.data[i] = this.data[j];
	// }
	this.data[index] =
	    (splitedNode instanceof DataNode) ?
	    ((DataNode)splitedNode).getMinID() :
	    ((TreeNode)splitedNode).data[0];
	this.children[index + 1] = splitedNode;
	// for (int i = 0, j = 0; i < MAX_CHILDREN_NODES + 1; i++, j++) {
	//     this.children[i] = this.children[j];
	//     if (this.children[j] == childrenNode) {
	// 	i++;
	// 	index = i;
	//     }
	// }
	// this.children[index] = splitedNode;

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
