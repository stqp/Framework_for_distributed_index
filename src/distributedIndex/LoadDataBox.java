package distributedIndex;

import loadBalance.LoadInfoTable;

public 	class LoadDataBox{
	private int myLoad =-1;
	private int prevLoad=-1;
	private int nextLoad=-1;
	private int threshold=-1;
	private int myDataSize=-1;
	private int prevDataSize=-1;
	private int nextDataSize=-1;
	private LoadInfoTable loadInfoTable;

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
		return sb.toString();
	}
}
