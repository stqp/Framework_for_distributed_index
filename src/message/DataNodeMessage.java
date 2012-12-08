package message;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

import util.AlphanumericID;
import util.ID;

import node.DataNode;


import com.google.gson.Gson;

public class DataNodeMessage implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private InetSocketAddress sender;

	private ArrayList<String> keyList;
	
	
	
	public DataNodeMessage(){

	}

	public DataNodeMessage(DataNode[] dataNodesToBeRemoved,InetSocketAddress sender){
		this.sender = sender;
		this.keyList = this.toStringFromDataNodes(dataNodesToBeRemoved);
	}

	
	
	public InetSocketAddress getSenderAddress(){
		return sender;
	}

	
	public DataNode[] getDataNodes(){
		return this.toDataNodesFromArrayList();
	}
	
	
	public String toJson(){
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
	
	/*
	 * DataNodeクラスはインターフェースを保持しているのでgsonがうまくjsonから復元できません。
	 * そこでこのクラスがDataNodeをラップします。
	 * いったんarraylistに変換して、必要なときにDataNodeクラスに復元します。
	 */
	private ArrayList<String> toStringFromDataNodes(DataNode[] dns){
		ArrayList<String > arr = new ArrayList<String>();
		for(DataNode dn: dns){
			for(ID id : dn.getAll()){
				arr.add(id.toString());
			}
			arr.add(":");
		}
		return arr;
	}
	
	private DataNode[] toDataNodesFromArrayList(){
		List<DataNode> dataNodes = new ArrayList<DataNode>();
		DataNode dataNode = new DataNode();
		try{
			for(String tempStr:this.keyList){
				if(tempStr.equals(":")){
					dataNodes.add(dataNode);
					dataNode = new DataNode();
				}else if(tempStr != null){
					dataNode.add(new AlphanumericID(tempStr));
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return (DataNode[])dataNodes.toArray(new DataNode[0]);
	}
	

}
