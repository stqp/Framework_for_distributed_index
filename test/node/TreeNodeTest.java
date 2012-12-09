package node;

import static org.junit.Assert.*;

import java.util.ArrayList;

import javax.xml.crypto.Data;

import org.junit.Test;

import store.TreeLocalStore;
import store.TreeNode;
import util.AlphanumericID;
import util.ID;
import util.MyUtil;

public class TreeNodeTest extends MyUtil{
	
	
	@Test
	public void testRemoveDataNode(){
		TreeLocalStore store = new TreeLocalStore();
		TreeNode treeNode = new TreeNode(store);
		
		DataNode d1; //= new DataNode(treeNode,null,null);
		DataNode d2 = null;// = new DataNode(treeNode,d1,null)
		DataNode d3 = null;
		DataNode d4 = null;
		
		d1 = new DataNode(treeNode, null, d2);
		addId(d1, "1");
		addId(d1, "2");
		addId(d1, "3");
		d2 = new DataNode(treeNode, d1, d3);
		addId(d2, "4");
		addId(d2, "5");
		addId(d2, "6");
		d3 = new DataNode(treeNode, d2, d4);
		addId(d3, "7");
		addId(d3, "8");
		addId(d3, "9");
		d4 = new DataNode(treeNode, d3, null);
		addId(d4, "10");
		addId(d4, "11");
		addId(d4, "12");

		
		DataNode[] children = new DataNode[4];
		children[0] = d1;
		children[1] = d2;
		children[2] = d3;
		children[3] = d4;
		
		
		treeNode.replaceChildren(children);
		
		pri(treeNode.getChildrenSize());
		
		pri(d1.getMinID().toString());
		
		treeNode.removeDataNode(d2);
		
		pri(treeNode.getChildrenSize());
	}
	
	
	@Test
	public void testAddDataNode(){
		TreeLocalStore store = new TreeLocalStore();
		TreeNode treeNode = new TreeNode(store);
		
		/*
		 * もとから入っていたデータノード
		 */
		DataNode d1; //= new DataNode(treeNode,null,null);
		DataNode d2 = null;// = new DataNode(treeNode,d1,null)
		DataNode d3 = null;
		DataNode d4 = null;
		
		d1 = new DataNode(treeNode, null, null);
		addId(d1, "19");
		addId(d1, "20");
		addId(d1, "30");
		d2 = new DataNode(treeNode, d1, d3);
		addId(d2, "40");
		addId(d2, "50");
		addId(d2, "60");
		d3 = new DataNode(treeNode, d2, d4);
		addId(d3, "70");
		addId(d3, "80");
		addId(d3, "90");
		d4 = new DataNode(treeNode, d3, null);
		addId(d4, "100");
		addId(d4, "110");
		addId(d4, "120");
		
		d1.setNext(d2);
		d2.setNext(d3);
		d3.setNext(d4);
		
		d2.setPrev(d1);
		d3.setPrev(d2);
		d4.setPrev(d3);
		
		
		/*
		 * 新しく追加するデータノード
		 */
		DataNode d5 = new DataNode(treeNode, null,null);
		addId(d5 ,"130");
		addId(d5 ,"140");
		addId(d5 ,"150");
		addId(d5 ,"160");
		DataNode d6 = new DataNode(treeNode, null,null);
		addId(d6 ,"200");
		addId(d6 ,"210");
		addId(d6 ,"220");
		addId(d6 ,"230");
		
	
		
		DataNode[] children = new DataNode[4];
		children[0] = d1;
		children[1] = d2;
		children[2] = d3;
		children[3] = d4;

		DataNode[] dnsToAdd = new DataNode[2];
		dnsToAdd[0] = d5;
		dnsToAdd[1] = d6;
		
		
		treeNode.replaceChildren(children);

		assertEquals(4, treeNode.getChildrenSize());
		assertEquals(12, treeNode.getDataSize());
		
		
		/*
		 * @test1
		 * ・データノードを複数追加
		 * ・右端に追加
		 */
		treeNode.addDataNodes(dnsToAdd);
		assertEquals(6, treeNode.getChildrenSize());
		assertEquals(20, treeNode.getDataSize());
		
		
		/*
		 * B-Link構造の更新テスト
		 */
		int count = 0;
		DataNode dn = treeNode.getFirstDataNode();
		while(dn != null){
			count+= dn.size();
			dn = dn.getNext();
		}
		
		assertEquals(20, count);
		
		
		
		treeNode =  new TreeNode(store);
		
		/*
		 * 新しく追加するデータノード２
		 */
		DataNode d7 = new DataNode(treeNode, null,null);
		addId(d7 ,"101");
		addId(d7 ,"107");
		addId(d7 ,"108");
		addId(d7 ,"109");
		
		dnsToAdd = new DataNode[1];
		dnsToAdd[0] = d7;
		
		
		/*
		 * childrenに前のテストのリンクが残っているので削除しておく。
		 */
		children[0].setPrev(null);
		children[children.length-1].setNext(null);
		
		
		treeNode.replaceChildren(children);
		assertEquals(4, treeNode.getChildrenSize());
		assertEquals(12, treeNode.getDataSize());

		
		/*
		 * @test2
		 * ・データノード１つだけ追加
		 * ・データノードを左端に追加
		 */
		treeNode.addDataNodes(dnsToAdd);
		assertEquals(5, treeNode.getChildrenSize());
		assertEquals(16, treeNode.getDataSize());
		
	
		
		/*
		 * B-Link構造の更新テスト
		 */
		count = 0;
		dn = treeNode.getFirstDataNode();
		while(dn != null){
			count+= dn.size();
			dn = dn.getNext();
		}
		assertEquals(16, count);
		
		
		
		
		
		
		/*
		 * @test
		 * アドレスノードを混ぜてもうまく動くことをテストします。
		 */
		treeNode = new TreeNode(store);
		/*
		 * 最初からセットしておくノード
		 */
		d1.setNext(d2);
		d2.setNext(d3);
		d3.setNext(d4);
		d4.setNext(null);
		d1.setPrev(null);
		d2.setPrev(d1);
		d3.setPrev(d2);
		d4.setPrev(d3);
		AddressNode an1 = new AddressNode(null, null);
		AddressNode an2 = new AddressNode(null, null);
		/*
		 * パターンとしては
		 * ・途中にアドレスノード
		 * ・端にアドレスノード
		 */
		Node[] baseChildren = new Node[6];
		baseChildren[0] = d1;
		baseChildren[1] = an1;
		baseChildren[2] = d2;
		baseChildren[3] = d3;
		baseChildren[4] = d4;
		baseChildren[5] = an2;
		
		
		
		/*
		 * 追加するノード
		 */
		d5.setPrev(null);
		d5.setNext(null);
		d6.setPrev(null);
		d6.setNext(null);
		dnsToAdd = new DataNode[2];
		dnsToAdd[0] = d5;
		dnsToAdd[1] = d6;
		
	
		
		
		treeNode.replaceChildren(baseChildren);
		/*
		 * 初期ノード追加時のテスト
		 */
		assertEquals(6, treeNode.getChildrenSize());
		assertEquals(12, treeNode.getDataSize());

		/*
		 * B-Link構造の更新テスト
		 */
		count = 0;
		dn = treeNode.getFirstDataNode();
		while(dn != null){
			count+= dn.size();
			dn = dn.getNext();
		}
		assertEquals(12, count);
		
		
		
		treeNode.addDataNodes(dnsToAdd);
		/*
		 * さらにデータノード追加時のテスト
		 */
		assertEquals(8, treeNode.getChildrenSize());
		assertEquals(20, treeNode.getDataSize());

		/*
		 * B-Link構造の更新テスト
		 */
		count = 0;
		dn = treeNode.getFirstDataNode();
		while(dn != null){
			count+= dn.size();
			dn = dn.getNext();
		}
		assertEquals(20, count);
		
	}

	
	@Test
	public void testArrayListToArray(){
		ArrayList<DataNode> arrlist = new ArrayList<DataNode>();
		
		TreeLocalStore store = new TreeLocalStore();
		TreeNode treeNode = new TreeNode(store);
		
		DataNode d1; //= new DataNode(treeNode,null,null);
		DataNode d2 = null;// = new DataNode(treeNode,d1,null)
		
		d1 = new DataNode(treeNode, null, d2);
		addId(d1, "1");
		addId(d1, "2");
		addId(d1, "3");
		d2 = new DataNode(treeNode, d1, null);
		addId(d2, "4");
		addId(d2, "5");
		addId(d2, "6");
		d1.setNext(d2);
		d2.setPrev(d1);
		
		arrlist.add(d1);
		arrlist.add(d2);
		
		DataNode[] test= (DataNode[])arrlist.toArray(new DataNode[0]);

		assertEquals("4", test[0].getNext().getMinID().toString());
	}
	
	
	
	private void addId(DataNode dn, String idValues){
		dn.add(new AlphanumericID(idValues));
	}

	
}








