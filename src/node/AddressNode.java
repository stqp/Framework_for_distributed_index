package node;
// AddressNode.java


import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import util.ID;
import util.LatchUtil;
import util.MessageSender;
import util.NodeStatus;

public class AddressNode implements Node {
    private static final String NAME = "AddressNode";
    public String getName() {return NAME;}

    private InetSocketAddress addr;
    
    /*
     * textの使い方は手法によって異なります。
     * textにはあるIDを文字列にしたものだったり、
     * minIDとmaxIDを文字列にして、それを「,」で連結したものだったりします。
     * 
     *　できたら手法毎にこのクラスを拡張した方がよかったです。
     */
    protected String text;

    /*
     * ラッチを使ってスレッド間でのアクセスを制御するための状態変数
     */
    private int[] status;

    public AddressNode(InetSocketAddress addr, String text) {
	this.addr = addr;
	this.text = (text != null) ? text : "";

	this.status = LatchUtil.newLatch();
    }

    public AddressNode(InetAddress host, int port, String text) {
	this.addr = new InetSocketAddress(host, port);
	this.text = (text != null) ? text : "";

	this.status = LatchUtil.newLatch();
    }


    public AddressNode toInstance(String[] text, ID id) {
	return AddressNode._toInstance(text, id);
    }

    public static AddressNode _toInstance(String[] text, ID id) {
	InetSocketAddress a = null;
	if (text[0].length() > 0) {
	    String[] items = text[0].split(":", 2);
	    try {
		InetAddress host = InetAddress.getByName(items[0]);
		a = new InetSocketAddress(host, Integer.parseInt(items[1]));
	    }
	    catch (UnknownHostException e) {
		e.printStackTrace();
	    }
	}

	return new AddressNode(a, text[1]);
    }


    public void ackUpdate(MessageSender sender, Node node) {
	System.err.println("WARNING AddressNode#ackUpdate");
    }

    public String toMessage() {
	String a =
	    (this.addr == null) ?
	    "" :
	    this.addr.getAddress().getHostAddress() + ":" + this.addr.getPort();
	return NAME + " 2 " + a + " " + this.text;
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

    public InetSocketAddress getAddress() {return this.addr;}
    public InetSocketAddress setAddress(InetSocketAddress addr) {
	InetSocketAddress old = this.addr;
	this.addr = addr;
	return old;
    }

    public String getText() {return this.text;}

    public String toLabel() {
	return NAME +
	    ((this.addr != null) ? this.addr.getAddress().getHostAddress() + ":" + this.addr.getPort() : "") + "," +
	    this.text;
    }

    public String toString() {
	return ((this.addr == null) ?
		null :
		this.addr.getAddress().getHostAddress() + ":" + this.addr.getPort()) +
	    " (" + this.text + ")";
    }
}
