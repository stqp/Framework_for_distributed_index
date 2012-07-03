// EasyLocalStore.java

import java.util.ArrayList;

// 
// !!! WARNING !!!
// OBSOLETE
// not maintain
// not runnable
// 

// note: not use synchronized (this) {} in each function
// will use alternate exclusion access control or not lock this at all
public final class EasyLocalStore implements LocalStore {
    private ID[] range;

    private ArrayList<DataNode> data;

    public EasyLocalStore(ID[] range) {
    	this.range = range;

    	this.data = new ArrayList<DataNode>();
    	this.data.add(new DataNode(this.range));
    }

    public EasyLocalStore(ID start, ID end) {
	this.range = new ID[2];
	this.range[0] = start; this.range[1] = end;

	this.data = new ArrayList<DataNode>();
	this.data.add(new DataNode(this.range));
    }

    public static LocalStore toInstance(String[] text, ID id) {
	int i = 0;
	ID start = (text[i].length() > 0) ? id.toInstance(text[i]) : null; i++;
	ID end = (text[i].length() > 0) ? id.toInstance(text[i]) : null; i++;
	ArrayList<DataNode> data = new ArrayList<DataNode>();
	while (i < text.length) {
	    if (text[i].compareTo("DataNode") == 0) {
		i++;
		int n = Integer.parseInt(text[i]); i++;
		String[] temp = new String[n];
		System.arraycopy(text, i, temp, 0, n);
		DataNode dataNode = DataNode._toInstance(temp, id); i += n;
		data.add(dataNode);
	    }
	    else {
		System.err.println("ERROR LocalStore#toInstance");
		return new LocalStore(start, end);
	    }
	}

	LocalStore store = new LocalStore(start, end);
	store.data = data;

	return store;
    }

    public String toMessage() {
	StringBuilder sb = new StringBuilder();
	sb.append(((this.range[0] != null) ? this.range[0].toMessage() : "") + " ");
	sb.append(((this.range[1] != null) ? this.range[1].toMessage() : "") + " ");
	for (int i = 0; i < this.data.size(); i++) {
	    sb.append(this.data.get(i).toMessage() + " ");
	}
	sb.delete(sb.length() - 1, sb.length());
	String msg = sb.toString();
	String[] temp = msg.split(" ");
	return "LocalStore " + temp.length + " " + msg;
    }

    public DataNode getFirstDataNode() {
	return this.data.get(0);
    }

    public DataNode getNextDataNode(DataNode dataNode) {
	int i = this.data.indexOf(dataNode) + 1;
	if (i >= this.data.size()) return null;
	return this.data.get(i);
    }

    public DataNode searchKey(ID key) {
	if (key.compareTo(this.range[0]) < 0) {
	    return getFirstDataNode();
	}

	for (DataNode node: this.data) {
	    ID[] r = node.getRange();
	    boolean s = (r[0] == null || key.compareTo(r[0]) >= 0);
	    boolean e = (r[1] == null || key.compareTo(r[1]) < 0);
	    if (s && e) {
		return node;
	    }
	}

	return null;
    }

    public NodeStatus searchData(DataNode dataNode) {
	return dataNode.searchData();
    }

    public void endSearchData(NodeStatus status) {
	DataNode node = (DataNode)status.getNode();
	node.endSearchData(status);
    }

    public NodeStatus updateData(DataNode dataNode) {
	return dataNode.updateData();
    }

    public void endUpdateData(NodeStatus status) {
	DataNode node = (DataNode)status.getNode();
	node.endUpdateData(status);
    }

    public LocalStore splitResponsibleRange(NodeStatus[] status, ID[] range) {
	LocalStore store = new LocalStore(range);
	for (int i = 0; i < status.length; i++) {
	    Node node = status[i].getNode();
	    if (node instanceof DataNode) {
		DataNode dataNode = (DataNode)node;
		ID[] dataRange = dataNode.getRange();
		if (range[0].compareTo(dataRange[0]) <= 0 && range[1].compareTo(dataRange[1]) >= 0) {
		    store.data.get(0).addAll(dataNode.getIDList());
		    this.data.remove(dataNode);
		}
		else {
		    for (ID id: dataNode.getIDList()) {
			if (id.compareTo(range[0]) >= 0 && id.compareTo(range[1]) < 0) {
			    store.data.get(0).add(id);
			    dataNode.remove(id);
			}
		    }
		}
	    }
	}

	return store;
    }
}
