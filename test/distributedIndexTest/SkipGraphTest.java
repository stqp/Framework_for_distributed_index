package distributedIndexTest;


import static org.junit.Assert.*;

import org.junit.Test;

import util.AlphanumericID;
import util.ID;

import distributedIndex.DistributedIndex;
import distributedIndex.SkipGraph;

public class SkipGraphTest {

	private SkipGraph skipGraph;
	private ID id;
	
	
	public SkipGraphTest() {
		skipGraph = new SkipGraph();
		id = new AlphanumericID(AlphanumericID._getRandomID().toString());
		skipGraph.initialize(id);
	}
	
	
	
	@Test
	public void testInitialize(){
		//example : id.toString → XG42qthu62nhtaqEVfetMibWha6y9ztbNtWbOtVFtyHtsuJ7sfmleCxppqUGZyY6
		assertEquals(id.toString(), skipGraph.getID().toString());
	
	}
	
	@Test	
	public void testToMessage(){
		/*
		 * ソースではStringBuilderのappendを使ってメッセージをためて、split(" ")によって分割して返します。
		 * 従って各メッセージ間は" "によって区切られています。
		 */
		String distributedIndexName = "SkipGraph ";
		String numberOfMessage = "56 ";
		String idName = id.toString() + " ";
		
		String nodeToMessage = "9 0 AddressNode 2  0 AddressNode 2  0 9 1 AddressNode 2  1 " +
				"AddressNode 2  1 9 2 AddressNode 2  2 AddressNode 2  2 9 3 AddressNode 2  3 " +
				"AddressNode 2  3 9 4 AddressNode 2  4 AddressNode 2  4 ";
		
		String memberShipVector = "11110001 ";
		String storeToMessage = "TreeLocalStore 2 DataNode 0 ";

		
		assertEquals(
				distributedIndexName 
				+ numberOfMessage 
				+ idName 
				+ nodeToMessage
				+ memberShipVector
				+ storeToMessage 
				, skipGraph.toMessage());
	}
	
	@Test
	public void test_toInstance(){
		String[] tests = {"test"};
		assertEquals("", skipGraph._toInstance(tests, id));
	}
	
	@Test
	public void testGetResponsibleRange(){
	//	assertEquals("",skipGraph.getResponsibleRange(sender));
	}

}



















