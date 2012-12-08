package loadBalance;

import node.DataNode;
import util.ID;

public class LoadInfo {

	/*
	 * データノードにおける一定期間内のアクセス回数を格納します。
	 */
	private int access;

	private int dataSize;
	
	private ID minId;
	
	private ID maxId;


	public LoadInfo(DataNode dataNode){
		this.minId = dataNode.getMinID();
		this.maxId = dataNode.getMaxID();
		this.dataSize = dataNode.size();
	}
	
	
	public ID getMinId(){
		return this.minId;
	}
	
	public ID getMaxId(){
		return this.maxId;
	}
	public int getSize(){
		return dataSize;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if(minId==null){
			sb.append("null");
		}else{
			sb.append(minId.toString());
		}
		sb.append(" ");
		if(maxId==null){
			sb.append("null");
		}else{
			sb.append(maxId.toString());
		}
		sb.append(" ");
		sb.append(dataSize);
		
		return sb.toString();
		
	}

}
