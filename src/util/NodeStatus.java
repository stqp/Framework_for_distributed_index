package util;

import java.io.Serializable;

import node.Node;
// NodeStatus.java

// immutable
public class NodeStatus implements Serializable{
    private Node node;
    private int type;

    public NodeStatus(Node node, int type) {
    	this.node = node;
    	this.type = type;
    }

    public Node getNode() {return this.node;}
    public int getType() {return this.type;}
}
