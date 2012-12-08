package store;

import node.DataNode;
import node.Node;
import util.ID;
import util.LatchUtil;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;


public final class TreeNode implements Node {
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

