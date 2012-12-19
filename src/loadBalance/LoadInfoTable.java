package loadBalance;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;

import util.ID;


/*
 * 計算機ごとの負荷情報を管理します
 * 各計算機の負荷を数値としリストにして保持します。
 */
public class LoadInfoTable{




	//必要ない
	//private HashMap<String, LoadInfoList> map;

	private HashMap<String, Integer> loadList;

	private HashMap<String, Integer> dataSizeList;

	//ロードがセットされた時間を管理することで受け取ったロードリストと自分のロードリストの情報のどちらを優先するかを決めます。
	private HashMap<String, Long> timeCard;

	/**
	 * 計算機ごとの平均アクセス回数を格納します。
	 */
/*	protected int average = 0;
*/
	/**
	 * owner of this LoadInfoTable object.
	 * 現在のところコンピュータのアドレスをtoStringしたものを使おうと思う。
	 */
	//private String master;


	public LoadInfoTable(){
		this.loadList = new HashMap<String, Integer>();
		this.dataSizeList = new HashMap<String, Integer>();
		this.timeCard = new HashMap<String, Long>();
	}

	//donot use this constructor
	//because master is not need.
	/*public LoadInfoTable(String master){
		//this.master = master;
		this.loadList = new HashMap<String, Integer>();
		this.timeCard = new HashMap<String, Long>();
	}*/

	//マスターが変更されることは本来はない
	//したがってコンストラクターで設定して変更しないのが正しいが、インスタンス化のタイミングではマスター名がわからないのでこのメソッドを用意した。
	//＃＃＃変更してロードをセットするときにマスタを渡すようにして、このクラスはマスタを保持しないようにした。
	/*public void setMaster(String master){
		//this.master = master;
	}*/

	//public void replaceLoadInfoList(HashMap<String, Integer> newLoadList){
	//int myLoad = this.loadList.get(master);
	//this.loadList = newLoadList;
	//this.loadList.put(master, myLoad);
	//}

	//自分の負荷を更新し、すべての計算機の負荷の平均値も更新する
	//マスタがまだ設定されていないときには入れないようにします。
	public void setLoad(String master,  int load){
		if(master== null || master.length() == 0) return ;
		loadList.put(master, load);
		timeCard.put(master, System.currentTimeMillis());
	}


	/*
	 * this method is called when setLoad is called.
	 * so we don't need set the timeCard  here.
	 */
	public void setDataSize(String master, int dataSize){
		if(master== null || master.length() == 0) return ;
		dataSizeList.put(master, dataSize);
	}




	/**
	 * 隣から負荷情報を受け取ったときに自分が持っていない情報もしくは新しい情報だけ選択して追加する。
	 * ただし自分の情報は上書きさせない。
	 */
	public void updateLoadInfoList(
			String master,
			HashMap<String , Integer> passedloadList,
			HashMap<String , Long> passedTimecard,
			HashMap<String, Integer> passedDataSizeList){

		for (Iterator<String> key = passedloadList.keySet().iterator(); key.hasNext();) {
			String temp = key.next();

			if(this.loadList.get(temp) == null || this.timeCard.get(temp) < passedTimecard.get(temp)){
				this.loadList.put(temp, passedloadList.get(temp));
				this.dataSizeList.put(temp, passedDataSizeList.get(temp));
				this.timeCard.put(temp, passedTimecard.get(temp));
			}
		}

		/*int myLoad  = this.loadList.get(master);
		this.loadList = passedloadList;
		this.loadList.put(master, myLoad);*/
	}


	/**
	 * 隣から負荷情報を受け取った時に直接呼ばれるメソッド
	 * いまはどこからも呼ばれていない
	 */
	public void recieveLoadInfoTableString(String master, String loadInfoTableString){
		LoadInfoTable loadInfoTable = (new Gson()).fromJson(loadInfoTableString, LoadInfoTable.class);
		this.updateLoadInfoList(
				master ,
				loadInfoTable.getLoadList(),
				loadInfoTable.getTimeCard(),
				loadInfoTable.getDataSizeList());
	}



	//getter 群
	public HashMap<String, Integer> getDataSizeList(){
		return this.dataSizeList;
	}

	public HashMap<String, Integer> getLoadList(){
		return this.loadList;
	}

	public HashMap<String, Long> getTimeCard(){
		return this.timeCard;
	}

	public static String getTag(){
		return "loadInfoTable";
	}


	public int getAverage(){
		int loadCount = 0;
		for (Iterator<String> master = loadList.keySet().iterator(); master.hasNext();) {
			loadCount += loadList.get(master.next());
		}
		return loadList.size()==0? 0 : loadCount / loadList.size();
	}


	public String toJson(){
		return new Gson().toJson(this);
	}

	public static LoadInfoTable toObjectFromString(String str){
		return null;
	}
}

