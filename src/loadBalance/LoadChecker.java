package loadBalance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.MyUtil;
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

public class LoadChecker extends MyUtil implements Runnable {

	//private LoadInfoTable loadInfoTable;
	private int interval = 3000;
	private DistributedIndex distributedIndex;
	private MessageReceiver receiver;


	/*
	 * @constructor
	 */
	public LoadChecker(int interval, DistributedIndex distributedIndex, MessageReceiver receiver){
		this.interval = interval;
		this.distributedIndex = distributedIndex;
		this.receiver = receiver;
		//this.loadInfoTable = new LoadInfoTable();
	}


	public static String getLoadInfoTag(){
		return "LOAD_INFO";
	}

	/*public LoadInfoTable getLoadInfoTable(){
		//return this.loadInfoTable;
	}*/


	/*public void setLoad(String master, LoadInfoTable load){
		synchronized (this.loadInfoTable) {
			this.loadInfoTable.updateLoadInfoList(
					master,
					load.getLoadList(),
					load.getTimeCard(),
					load.getDataSizeList());
		}
	}*/

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
			}
			try{
				this.distributedIndex.startLoadBalance();
			}catch(Exception e){
				pri("ERROR at LoadChecker.class at run method :");
				e.printStackTrace();
			}
		}
	}


}
