package loadBalance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.Shell;

import distributedIndex.DistributedIndex;

import node.DataNode;
import node.Node;


/*
 * 負荷情報を定期的に調べるクラス。
 *
 * やりたい仕事としては
 * ・負荷情報のテーブルを保持する
 * ・負荷情報のテーブルから負荷分散するべきかを調べる
 * ・負荷移動するときには誰にどのデータを渡すか計算
 * ・負荷移動中のクエリはいったんキュートかに入れておく
 * ・負荷移動が終わったら更新情報を手法に応じてだれに送るか決定し、更新情報を送る。
 */

public class LoadChecker implements Runnable {


	/*
	 * loadInfoTableという計算機ごとのアクセス回数を格納したリストを用意する
	 */
	private LoadInfoTable loadInfoTable;

	public static String getLoadInfoTag(){
		return "LOAD_INFO";
	}


	/*
	 * loadInfoListというデータノード毎のアクセス回数を格納したリストを用意する。
	 */
	//private List<LoadInfo>loadInfoList;


	/*
	 * wait a few second every check load.
	 * default 5000 msec = 5s.
	 */
	private int interval = 3000;


	/*
	 * access to data node counter by this.
	 */
	private DistributedIndex distributedIndex;


	private MessageReceiver receiver;


	/*
	 * @constructor
	 */
	public LoadChecker(int interval, DistributedIndex distributedIndex, MessageReceiver receiver){
		this.interval = interval;
		this.distributedIndex = distributedIndex;
		this.receiver = receiver;

		//TODO
		//future work : nullを入れる必要があるのは分散インデックス手法の初期化のタイミングがshellからの命令に依存するからです。
		//LoadInfoTableのマスターを設定しておきたいのにここではできません。
		this.loadInfoTable = new LoadInfoTable();
	}


	public LoadInfoTable getLoadInfoTable(){
		return this.loadInfoTable;
	}





	public void setLoad(String master, LoadInfoTable load){
		synchronized (this.loadInfoTable) {
			this.loadInfoTable.updateLoadInfoList(
					master, 
					load.getLoadList(),
					load.getTimeCard(),
					load.getDataSizeList());
		}
	}

	/*
	 * たぶんスレッドにして一定時間ごとに生き返って処理を行えばいいじゃん
	 */
	public void run(){
		System.out.println("IN_LOAD_CHECKER_RUN" );
		while(true){
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.out.println("error :at "+ this.getClass().toString() + "at run at Thread.sleep");
			}


			try{
				this.distributedIndex.checkLoad(this.loadInfoTable, this.receiver.getMessageSender());
			}catch(Exception e){
				System.out.println("ERROR at LoadChecker.class at run method :");
				e.printStackTrace();
			}


			//try{



			//synchronized (distributedIndex) {

			//LoadInfoList loadInfoList = checkLoad(distributedIndex);
			//distributedIndex.checkLoad(loadInfoTable, );
			//System.out.println("LOAD_COLLECT" + loadInfoList.toString());
			//System.out.println(this.distributedIndex.getName()+".toString:"+this.distributedIndex.toString());


			/*
			 * send load information to next machine.
			 */
			/*try {
						sendLoadInfoTo(distributedIndex.getNextMachine(), loadInfoTable);
					} catch (IOException e) {
						System.out.println("at LoadChecker class at sendLoadInfoTo method");
						e.printStackTrace();
					}*/




			//updateLoadInfoTable(distributedIndex.getID(), loadInfoList);
			//moveDataNode(distributedIndex, loadInfoTable, receiver.getMessageSender());

			//}

			//}catch(Exception e){

			//}

		}
	}





	/*	private DataNode[] moveDataNode(DistributedIndex distributedIndex, LoadInfoTable loadInfoTable, MessageSender sender){
		if (isOverLoad(loadInfoTable) == true ){

	 * 移動先以外の計算機にデータ移動を伝える
	 * 移動先の計算機にデータを転送する




	 * もしdistributedIndexにデータ移動のメソッドがない場合には
	 *
	 * １．分散インデックス手法に機能を追加する
	 * ２．フレームワーク側で分散インデックス手法をいじくる
	 *
	 * 基本的には１の方法で行うべし。２の場合はしたのメソッドを実装する。
	 *
	 * sendLoadInfoTo(distributedIndex.getNextMachine(), loadInfoTable);//targetPEは隣の計算機

		}
		return null;
	}*/






	/*	private void sendLoadInfoTo(InetSocketAddress target, LoadInfoTable loadInfoTable) throws IOException{

		System.out.println("LOAD_TARGET_INFO "+target.toString());


		if(target == null || loadInfoTable == null) return ;

		receiver.getMessageSender().send(LoadChecker.getLoadInfoTag() + " " + loadInfoTable.toString(), target);
	}


	private boolean isOverLoad(LoadInfoTable LoadInfoTable){
		int ave = LoadInfoTable.getAverageAccess();

		return false;
	}


	public void updateLoadInfoTable(ID id, List loadInfoList){

	}*/




}
