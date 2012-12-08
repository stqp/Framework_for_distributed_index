package message;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import node.DataNode;
import util.AlphanumericID;
import util.ID;

import com.google.gson.Gson;

import loadBalance.LoadInfoTable;

public class MessageConverter {
	
	
	/*private static <E> obj;
	
	public static String toStringFromLoadInfo( load){
		Gson gson = new Gson();
		return gson.toJson(load);
	}
	
	public static LoadInfoTable toLoadInfoFromString(String str){
		Gson gson = new Gson();
		return gson.fromJson(str, LoadIn)
	}*/
	
	

	public static String toStringFromDataNodes(DataNode[] dataNodesToBeRemoved){
		List<String> strs = new ArrayList<String>();
		Gson gson = new Gson();
		for(DataNode dt:dataNodesToBeRemoved){
			for(ID id: dt.getAll()){
				strs.add(id.toString());
			}
			strs.add(":");
		}
		return gson.toJson(strs);
	}
	
	
	public static String toLoadInfoString(LoadInfoTable load, InetSocketAddress target){
		Gson gson = new Gson();
		return "from:"+ gson.toJson(target)+ ";" +MessageConverter.toStringFromLoadInfoTable(load);
	}
	
	
	
	public static String toStringFromLoadInfoTable(LoadInfoTable load){
		Gson gson = new Gson();
		return gson.toJson(load);
	}

	public static LoadInfoTable toLoadInfoTableFromJson(String json){
		Gson gson = new Gson();
		return gson.fromJson(json, LoadInfoTable.class);
	}
	
	public static DataNode[] toDataNodesFromString(String string){
		Gson gson = new Gson();
		ArrayList<String> re = gson.fromJson(string, ArrayList.class);
		List<DataNode> dataNodes = new ArrayList<DataNode>();
		DataNode dataNode = new DataNode();
		try{
			for(String tempStr:re){
				if(tempStr.equals(":")){
					dataNodes.add(dataNode);
					dataNode = new DataNode();
				}else if(tempStr != null){
					dataNode.add(new AlphanumericID(tempStr));
				}
			}
		}catch(Exception e){
			System.out.println("ERROR: add data nodes");
			e.printStackTrace();
		}
		return (DataNode[])dataNodes.toArray(new DataNode[0]);
	}
}
