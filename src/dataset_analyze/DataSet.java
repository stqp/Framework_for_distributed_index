package dataset_analyze;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import javax.annotation.processing.FilerException;
import javax.xml.crypto.Data;

import log_analyze.AnalyzerManager;
import log_analyze.MyStringBuilder;

import util.AlphanumericID;
import util.MyUtil;


public class DataSet extends MyUtil{





	private static String current =  new File("").getAbsolutePath();

	private static String testSetPath = current + "/testset/testset2/";

	private static String dataFilePrefix = "get";
	private static String dataFileSurfix = ".dat";
	
	private int count =0;
	
	public DataSet(){
		
	}

	public static void main(String[] srgs){


		File root = new File(testSetPath+"/32");

		DataSet ds = null;


		for(int i = 0; i< root.list().length; i++){

			File getData = root.listFiles()[i];
			ds = new DataSet();
			String line;

			try{
				BufferedReader bufReader = new BufferedReader(new FileReader(getData.toString()));
				while((line = bufReader.readLine())!=null){
					ds.analyze(line);
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


			}catch(FilerException e){
				System.out.println("fail to read file");
			} catch (IOException e) {
				System.out.println("io exception");
			}
		}
	}





	private void analyze(String line){

	}
	
	/*@Override
	public void writeResult(String analyzerResultDirPath, String fileName) {

		File newfile= null;
		PrintWriter printWriter= null;

		for(MyStringBuilder msb : resultList){
			String fileDir = analyzerResultDirPath+ "/"+ dirName+ "/"+ msb.getName()+ "/"+ fileName;
			newfile = createFile(fileDir);
			
			try {
				printWriter = new PrintWriter(new BufferedWriter(new FileWriter(newfile,true)));
				printWriter.write(msb.toString());
				printWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}*/





}
