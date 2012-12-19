package distributedIndex;

import java.net.InetSocketAddress;

import node.DataNode;

import loadBalance.LoadInfoTable;

public 	class LoadDataBox{
	private int myLoad =-1;
	private int prevLoad=-1;
	private int nextLoad=-1;
	private int threshold=-1;
	private int myDataSize=-1;
	private int prevDataSize=-1;
	private int nextDataSize=-1;
	private LoadInfoTable loadInfoTable = new LoadInfoTable();
	private DataNode[] dataNodesToBeMoved = new DataNode[0];
	private boolean isMoved=false;
	private InetSocketAddress target= new InetSocketAddress(0);
	
	public boolean getIsMoved(){
		return isMoved;
	}
	public void setIsMoved(boolean isMoved){
		this.isMoved = isMoved;
	}
	public DataNode[] getDataNodesToBeMoved() {
		return dataNodesToBeMoved;
	}
	public void setDataNodesToBeMoved(DataNode[] dataNodesToBeMoved) {
		this.dataNodesToBeMoved = dataNodesToBeMoved;
	}
	public InetSocketAddress getTarget() {
		return target;
	}
	public void setTarget(InetSocketAddress target) {
		this.target = target;
	}
	public LoadInfoTable getLoadInfoTable(){
		return loadInfoTable;
	}
	public void setLoadInfoTable(LoadInfoTable loadInfoTable){
		this.loadInfoTable = loadInfoTable;
	}
	public int getMyLoad() {
		return myLoad;
	}
	public void setMyLoad(int myLoad) {
		this.myLoad = myLoad;
	}
	public int getPrevLoad() {
		return prevLoad;
	}
	public void setPrevLoad(int prevLoad) {
		this.prevLoad = prevLoad;
	}
	public int getNextLoad() {
		return nextLoad;
	}
	public void setNextLoad(int nextLoad) {
		this.nextLoad = nextLoad;
	}
	public int getThreshold() {
		return threshold;
	}
	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}
	public int getMyDataSize() {
		return myDataSize;
	}
	public void setMyDataSize(int myDataSize) {
		this.myDataSize = myDataSize;
	}
	public int getPrevDataSize() {
		return prevDataSize;
	}
	public void setPrevDataSize(int prevDataSize) {
		this.prevDataSize = prevDataSize;
	}
	public int getNextDataSize() {
		return nextDataSize;
	}
	public void setNextDataSize(int nextDataSize) {
		this.nextDataSize = nextDataSize;
	}
	private static final char returnChar = '\n';
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("loadInfoTable:"+loadInfoTable.toJson()+returnChar);
		sb.append("myLoad:"+ myLoad+returnChar);
		sb.append("prevLoad:"+ prevLoad+returnChar);
		sb.append("nextLoad:"+ nextLoad+returnChar);
		sb.append("threshold:"+ threshold+returnChar);
		sb.append("myDataSize:"+ myDataSize+returnChar);
		sb.append("prevDataSize:"+ prevDataSize+returnChar);
		sb.append("nextDataSize:"+ nextDataSize+returnChar);
		sb.append("target to which send data:"+ target+returnChar);
		sb.append("isMoved:"+isMoved+returnChar);
		sb.append("number of dataNodesToBeMoved:"+dataNodesToBeMoved.length+returnChar);
		return sb.toString();
	}
}
