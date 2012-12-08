package store;
// TreeLocalStore.java


import java.util.ArrayList;

import util.ID;
import util.LatchUtil;
import util.LocalStore;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;

import node.DataNode;
import node.Node;

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
	
	public void setFirstDataNode(DataNode dn){
		this.leftmost = dn;
	}


	public TreeLocalStore toInstance(String[] text, ID id) {
		return TreeLocalStore._toInstance(text, id);
	}

	public static TreeLocalStore _toInstance(String[] text, ID id) {
		int i = 0;
		ArrayList<DataNode> data = new ArrayList<DataNode>();
		while (i < text.length) {
			if (text[i].compareTo(DataNode.NAME) == 0) {
				/*
				 * text -> DataNode n * * * * * * ...　 DataNode m * * * * * * * DataNode l * * * * * * *
				 * から順次DataNodeの文字列にぶつかるたびにDataNodeオブジェクトを作ってリストに追加している。
				 *
				 * ちなみにidは値は何でもよくって、AlphanumericIDの_toInstanceを使いたいだけ。
				 *
				 * temp -> null null null null null ... -> n こ
				 * temp -> * * * * * * ...
				 */

				i++;
				int n = Integer.parseInt(text[i]); i++;
				String[] temp = new String[n];
				System.arraycopy(text, i, temp, 0, n);
				/*
				 * temp は idの配列じゃないかな
				 * by tokuda
				 */
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
