package loadBalance;

import java.io.Serializable;

import distributedIndex.DistributedIndex;


/*
 * 負荷分散に関するメッセージをすべて受け取るクラス
 * 仕事
 * １．メッセージが何に関するものなのかを分析する
 * ２．メッセージをオブジェクトに戻す（そのオブジェクトの種類がわかればそのオブジェクトのメソッドを使えるようにする。）
 * ３．メッセージの種類に応じた処理を行う（隣の計算機からきた負荷情報テーブル、データ移動するという情報、移動結果の情報）
 */
public class LoadInfoReceiver implements Serializable{


	private DistributedIndex distributedIndex;



	public LoadInfoReceiver(){}


	public static String getLoadBalanceTag(){
		return "loadBalance";
	}

	public static String getDataMoveInfoTag(){
		return "dataMove";
	}


	/*
	 * TODO
	 *
	 */
	public void receive( String message , DistributedIndex distributedIndex){

		String[] items = message.split("\\s+");

		/*
		 * header must be LoadChecker.getLoadInfoTag().
		 * now i set the value "LOAD_INFO".
		 */
		String header = items[0];

		System.out.println("LOAD_INFO_FROM_PREVIOUS " + message);

		/*
		 * 隣の計算機からきた負荷情報テーブル
		 */
		if( message.startsWith(LoadInfoTable.getTag()) ){
			//message = message.substring(LoadInfoTable.getTag().length()+1);
			/*
			 * not yet implemented !!  TODO
			 * updateLoadInfoTable(LoadInfoTable.toObjectFromString(message));
			 */
		}

		/*
		 * データ移動するという情報
		 */
		else if ( message.startsWith(LoadInfoReceiver.getDataMoveInfoTag()) ){
			//message = message.substring(LoadInfoTable.getTag().length()+1);

			/*
			 * TODO
			 * if (移動先が自分){
				addDataNode(データノード)
			}
			else {
				updateIndex(送り主、受け主、データノード);
				updaetLoadInfoTable(送り主、受け主、データノード)；
			}*/
		}
	}


	/*
	 * TODO
	 */
	public void updateLoadInfoTable(){}
}
