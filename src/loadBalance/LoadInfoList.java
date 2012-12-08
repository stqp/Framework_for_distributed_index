package loadBalance;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import node.DataNode;


/*
 * this class is used to collect Load information.
 * look at LoadInfoTable and LoacChecker class.
 */
public class LoadInfoList {

	LinkedHashSet<LoadInfo> loadInfoList;

	/*
	 * ある計算機の一定時間におけるアクセス数の合計を格納します。
	 */
	private int totalAccess;

	/*
	 * "capacityUtilization" is not good name...
	 * private int capacityUtilization;
	 */
	private int totalNumberOfData;
	
	
	public LoadInfoList(){
		this.loadInfoList = new LinkedHashSet<LoadInfo>();
		this.totalNumberOfData = 0;
		this.totalAccess = 0;
	}
	
	
	
	public LoadInfo add(LoadInfo loadInfo){
		totalNumberOfData += loadInfo.getSize();
		loadInfoList.add(loadInfo);
		return loadInfo;
	}
	
	
	
	public int getTotalAccess(){
		return this.totalAccess;
	}
	
	public int getTotalNumberOfData(){
		return this.totalNumberOfData;
	}
	
	
	public void clear(){
		loadInfoList.clear();
		totalNumberOfData = 0;
		totalAccess = 0;
	}
	
	public String toString(){
		
		StringBuilder sb = new StringBuilder();
		for(LoadInfo loadInfo:loadInfoList){
			sb.append(" ");
			sb.append(loadInfo.toString());
		}
		return sb.toString();
		
	}

}
