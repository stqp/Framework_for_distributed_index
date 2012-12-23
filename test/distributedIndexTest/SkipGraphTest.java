package distributedIndexTest;


import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;

import main.Main;
import node.DataNode;
import node.Node;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.AlphanumericID;
import util.ID;
import util.MyUtil;
import util.NodeStatus;
import utilTest.DataSetFile;

import distributedIndex.AbstractDistributedIndex;
import distributedIndex.DistributedIndex;
import distributedIndex.SkipGraph;

public class SkipGraphTest extends MyUtil{

	private AbstractDistributedIndex distributedIndex;
	private ID id;
	private final static String currentPath = new File("").getAbsolutePath();
	private FakeMessageSender fsender = new FakeMessageSender(null);	
	private static final int putDataSize=5000;

	
	public void	addPutData(AbstractDistributedIndex dist,int num){
		dist.initialize(new AlphanumericID("user1000"));
		File putData = new File(connectFilePaths(currentPath,"testset","testset2","fakeput"+putDataSize+"/4","put"+num+".dat"));
		try {
			BufferedReader bufReader = new BufferedReader(new FileReader(putData));
			String line;
			String text = null;
			FakeMessageSender fsender = new FakeMessageSender(null);
			while((line = bufReader.readLine())!=null){
				AlphanumericID key = new AlphanumericID(line.split(" ")[1]);
				Node node = dist.updateKey(fsender, key, text);
				if(node instanceof DataNode){
					DataNode dn = (DataNode) node;
					NodeStatus status = dist.updateData(fsender, dn);
					dn.add(fsender, key);
					dist.endUpdateData(fsender, status);
				}
			}
			bufReader.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void	addPutData(AbstractDistributedIndex dist){
		dist.initialize(new AlphanumericID("user1000"));
		File putData = new File(connectFilePaths(currentPath,"testset","testset2","fakeput"+putDataSize+"/4","put0.dat"));
		try {
			BufferedReader bufReader = new BufferedReader(new FileReader(putData));
			String line;
			String text = null;
			FakeMessageSender fsender = new FakeMessageSender(null);
			while((line = bufReader.readLine())!=null){
				AlphanumericID key = new AlphanumericID(line.split(" ")[1]);
				Node node = dist.updateKey(fsender, key, text);
				if(node instanceof DataNode){
					DataNode dn = (DataNode) node;
					NodeStatus status = dist.updateData(fsender, dn);
					dn.add(fsender, key);
					dist.endUpdateData(fsender, status);
				}
			}
			bufReader.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Before
	public void beforeSetUp(){
		distributedIndex = new SkipGraph();
	}

	
	@Test
	public void testSendData(){
		AbstractDistributedIndex receiver = new SkipGraph();
		addPutData(receiver, 1);
		
		addPutData(distributedIndex);
		String commandLine="";
		String res = ((AbstractDistributedIndex)this.distributedIndex).receiveData(commandLine);
		assertEquals(putDataSize, distributedIndex.getTotalDataSizeByB_link());
	}
	
	

	@Test
	public void testPut(){
		addPutData(distributedIndex);
		assertEquals(putDataSize, distributedIndex.getTotalDataSizeByB_link());
	}


	@Test
	public void testGet(){
		int counter=0;
		addPutData(distributedIndex);
		File getData = new File(connectFilePaths(currentPath,"testset","testset2","get5000hit/32","get0.dat"));
		try {
			BufferedReader bufReader = new BufferedReader(new FileReader(getData));
			String line;
			String text = null;
			FakeMessageSender fsender = new FakeMessageSender(null);
			while((line = bufReader.readLine())!=null){
				AlphanumericID key = new AlphanumericID(line.split(" ")[1]);
				Node node = distributedIndex.updateKey(fsender, key, text);
				
				if(node instanceof DataNode){
					DataNode dn = (DataNode) node;
					NodeStatus status = distributedIndex.searchData(fsender, dn);
					if (dn.contains(key)) {
						counter++;
					}
					distributedIndex.endUpdateData(fsender, status);
				}
			}
			bufReader.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(putDataSize, counter);
	}



	@Test
	public void testDelete() throws IOException{
		addPutData(distributedIndex);
		assertEquals(putDataSize, distributedIndex.getTotalDataSizeByB_link());
		int rightToLeft =0;
		DataNode r= distributedIndex.getRightMostDataNode();
		while(r!=null){
			rightToLeft+=r.size();
			r=r.getPrev();
		}
		assertEquals(putDataSize, rightToLeft);
		
		/*
		 * 左から削除してみます
		 */
		int numberOfKeysToDelete=0;
		DataNode firstDataNode = distributedIndex.getFirstDataNode();
		ArrayList<DataNode> arrlistLeftToRight = new ArrayList<DataNode>();
		for(int i=0;i<100;i++){
			arrlistLeftToRight.add(firstDataNode);
			firstDataNode = firstDataNode.getNext();
		}
		
		for(DataNode dn : arrlistLeftToRight){
			numberOfKeysToDelete+= dn.size();
		}
		for(DataNode dn : arrlistLeftToRight){
			ID[] ids = dn.getAll();
			for(ID id: ids){
				Node target = distributedIndex.searchKey(fsender, id);
				if(target instanceof DataNode){
					DataNode targetDataNode = (DataNode)target;
					targetDataNode.remove(fsender, id);
				}
			}
		}
		assertEquals(putDataSize-numberOfKeysToDelete, distributedIndex.getTotalDataSizeByB_link());

		/*
		 * 右から削除してみます。
		 */
		DataNode rigthMost = distributedIndex.getRightMostDataNode();
		ArrayList<DataNode> arrlistRightToLeft = new ArrayList<DataNode>();
		for(int i=0;i<100;i++){
			arrlistRightToLeft.add(rigthMost);
			rigthMost = rigthMost.getPrev();
		}
		for(DataNode dn : arrlistRightToLeft){
			numberOfKeysToDelete+= dn.size();
		}
		int tempCount=0;
		for(DataNode dn : arrlistRightToLeft){
			ID[] ids = dn.getAll();
			for(ID id: ids){
				if(id.toString().equals("user6877907175873323119")){
					pri("");
				}
				Node target = distributedIndex.searchKey(fsender, id);
				if(target instanceof DataNode){
					DataNode targetDataNode = (DataNode)target;
					int total = distributedIndex.getTotalDataSizeByB_link();
					
					boolean res = targetDataNode.remove(fsender, id);
					tempCount++;
				}
			}
		}
		assertEquals(putDataSize-numberOfKeysToDelete, distributedIndex.getTotalDataSizeByB_link());
	}

	
	
	
	@After
	public void afterSetUp(){
	}




	public void testToMessage(){
		/*
		 * �\�[�X�ł�StringBuilder��append���g���ă��b�Z�[�W�����߂āAsplit(" ")�ɂ���ĕ������ĕԂ��܂��B
		 * �]���Ċe���b�Z�[�W�Ԃ�" "�ɂ���ċ�؂��Ă��܂��B
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
				, distributedIndex.toMessage());
	}


}



















