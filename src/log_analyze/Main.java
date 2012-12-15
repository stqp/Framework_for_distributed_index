package log_analyze;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.processing.FilerException;
import javax.xml.crypto.Data;

import loadBalance.LoadInfoTable;

import util.AlphanumericID;
import util.ID;
import util.MyUtil;

import com.google.gson.Gson;

import node.DataNode;
import node.Node;

public class Main extends MyUtil{





	private static String current =  new File("").getAbsolutePath();

	private static String LOG_FILE_ROOT = current +"/logs";

	private static String ANALYZE_RESULT_ROOT =  current + "/analyzeResult";

	private static AnalyzerManager analyzerManager;




	/*
	 * この辺の変数は最初使っていたけど
	 * もう必要ないです。
	 */
	/*private static String METHOD_NAME = "FatBtree";
	//private static String METHOD_NAME = "SkipGraph";
	//private static String METHOD_NAME = "PRing";
	private static String YEAR = "2012";
	private static String MONTH = "11";
	private static String DAY = "08";
	private static int NODE_NUM_FOR_ANALYZE = 2;
*/



	public static void main(String[] srgs){


		File root = new File(LOG_FILE_ROOT);

		for(int i_methods = 0; i_methods< root.list().length; i_methods++){

			File method = root.listFiles()[i_methods];

			for(int i_computers=0; i_computers< method.list().length; i_computers++){

				File computer = null;

				try{
					computer = method.listFiles()[i_computers];
					/*
					 * これはlogs>edn1_already_uselessのファイルの直下のディレクトリがlogファイルになっていて
					 * ディレクトリではないため次のループでエラーとなる。
					 * よってここで判別のためにこの処理を行う。
					 */
					int test = computer.list().length;
				}catch(Exception e){
					e.printStackTrace();
					break;
				}

				for(int i_date=0; i_date< computer.list().length; i_date++){





					File log = computer.listFiles()[i_date];

					/*
					 * analizerManagerは読み込むファイルごとに新しく作り変えてください
					 */
					analyzerManager = new AnalyzerManager();
					analyzerManager.loopItemClear();


					String line;
					try{
						BufferedReader bufReader = new BufferedReader(new FileReader(log.toString()));


						/*
						 * 一行ずつマネージャーから各解析クラスに渡して解析を行う。
						 */
						while((line = bufReader.readLine())!=null){
							analyzerManager.analyze(line);
						}


						/*
						 * 解析結果をコンソールに出力
						 */
						//analyzerManager.showResult();

						/*
						 * 解析中のファイル名を出力
						 */
						// pri(ANALYZE_RESULT_ROOT  +"/"+  method.getName()+ "/" +computer.getName()+ "/" + log.getName());


						/*
						 * 解析結果をファイルに書き込む
						 */
						analyzerManager.loopItemWriteResult(ANALYZE_RESULT_ROOT  +"/"+  method.getName()+ "/" +computer.getName(), log.getName());


					}catch(FilerException e){
						System.out.println("fail to read file");
					} catch (IOException e) {
						System.out.println("io exception");
					}




				}
			}
		}
	}






	/*
	 * 今後必要ないメソッド。
	 * 使ってないから消してもよいよ。
	 */

	private static String makeFileName(String year, String month, String day){
		String FILE_NAME_PREFIX = "log";
		String FILE_NAME_SUFFIX = ".txt";
		String space = "_";

		return FILE_NAME_PREFIX
				+ space + month
				+ space + day
				+ space + year
				+ FILE_NAME_SUFFIX;
	}
}
