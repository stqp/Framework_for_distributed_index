package distributedIndexTest;


import java.net.InetSocketAddress;
import java.rmi.MarshalException;

import loadBalance.LoadInfoTable;

import node.DataNode;

import org.junit.Test;

import util.AlphanumericID;
import util.MessageHandler;
import util.MessageReceiver;
import util.MessageSender;

import distributedIndex.*;

import static org.junit.Assert.*;

public class FatBtreeTest {
	private FatBtree fakeFatBtree ;
	
	@Test
	public void testCheckLoad(){
		
		fakeFatBtree = new FakeFatBtree();
		
		
		MessageSender sender = new MessageSender(null);
		LoadInfoTable table = new FakeLoadInfoTable("test");
		fakeFatBtree.startLoadBalance();
	}
}





class FakeLoadInfoTable extends LoadInfoTable{
	public FakeLoadInfoTable(String master) {
		super();
		// TODO 自動生成されたコンストラクター・スタブ
	}

	public void reCalcAverage(){
	}
	
}





class FakeFatBtree extends FatBtree{
	
	public FakeFatBtree(){
		this.prevMachine = new InetSocketAddress(100);
		this.nextMachine = new InetSocketAddress(200);
		
		setData();
	}
	
	public void setData(){
		DataNode data1  =new DataNode();
		data1.add(new AlphanumericID("id1"));
		data1.add(new AlphanumericID("id2"));
		
		DataNode data2  =new DataNode();
		data2.add(new AlphanumericID("id3"));
		data2.add(new AlphanumericID("id4"));
		
		data1.setNext(data2);
		data2.setPrev(data1);
		this.leftmost = data1;
	}
	

}