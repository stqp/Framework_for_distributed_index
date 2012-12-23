package dataset_analyze;


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
import log_analyze.AnalyzerManager;

import util.AlphanumericID;
import util.ID;
import util.MyUtil;

import com.google.gson.Gson;

import node.DataNode;
import node.Node;

public class Main extends MyUtil{

	private static String current =  new File("").getAbsolutePath();
	private static String TESTSET_ROOT = current +"/testset/testset2";
	private static String ANALYZE_RESULT_ROOT =  current + "/testset/analyzedTestset";
	private static DataSetAnalyzerManager dataSetAnalyzerManager;
	/*
	 * 現在担当範囲はこの値で指定している
	 */
	public static AlphanumericID range0 = new AlphanumericID("user1000053778378872380");
	public static AlphanumericID range1 = new AlphanumericID("user7073737695004966866");
	public static AlphanumericID range2 = new AlphanumericID("user8142835197696697420");
	public static AlphanumericID range3 = new AlphanumericID("user9099933036887726303");



	public static void main(String[] srgs){

		File root = new File(TESTSET_ROOT);

		PutToGetMaker ptgm = new PutToGetMaker();
		ptgm.analyze();

		System.exit(1);


		for(int i_query = 0; i_query< root.list().length; i_query++){
			File queryType = root.listFiles()[i_query];
			if(queryType.getName().equals("id.dat")){
				continue;
			}
			for(int i_nodeNum=0; i_nodeNum< queryType.list().length; i_nodeNum++){
				File nodeNum = queryType.listFiles()[i_nodeNum];
				for(int i_dateset=0; i_dateset< nodeNum.list().length; i_dateset++){

					File dataSet = nodeNum.listFiles()[i_dateset];

					dataSetAnalyzerManager = new DataSetAnalyzerManager();
					dataSetAnalyzerManager.loopItemClear();


					String line;
					try{
						BufferedReader bufReader = new BufferedReader(new FileReader(dataSet.toString()));
						/*
						 * 一行ずつマネージャーから各解析クラスに渡して解析を行う。
						 */
						while((line = bufReader.readLine())!=null){
							dataSetAnalyzerManager.analyze(line);
						}

						dataSetAnalyzerManager.loopItemWriteResult(ANALYZE_RESULT_ROOT  +"/"+  queryType.getName()+ "/" +nodeNum.getName(), dataSet.getName());


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
