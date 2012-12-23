package dataset_analyze;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import util.MyUtil;

import log_analyze.MyStringBuilder;

public class PutToGetMaker extends MyUtil{
	private static String current =  new File("").getAbsolutePath();
	private static String PUTDIR = current +"/testset/testset2/put25000/4";
	private static int GET_DATA_SIZE = 5000;
	private static String TARGETDIR =  current +"/testset/testset2/get"+GET_DATA_SIZE+"hit/32";

	public void analyze(){
		String line;
		File putDir = new File(PUTDIR);
		for(int i=0; i< putDir.list().length;i++){
			File putDataSet = putDir.listFiles()[i];
			MyStringBuilder msb = new MyStringBuilder("");
			try{
				BufferedReader bufReader = new BufferedReader(new FileReader(putDataSet));
				int count=0;
				while((line = bufReader.readLine())!=null){
					if(count >= GET_DATA_SIZE)break;
					String[] items = line.split("[ \\s]+");
					String queryName = items[0];
					String key = items[1];
					String value = items[2];
					msb.appendWithReturn("get "+key);
					count++;
				}
				bufReader.close();

				createDir(TARGETDIR);
				File newFile = createFile(TARGETDIR+"/get"+i+".dat");
				PrintWriter printWriter= null;
				printWriter = new PrintWriter(new BufferedWriter(new FileWriter(newFile,true)));
				printWriter.write(msb.toString());
				printWriter.close();

			}catch(Exception e){

			}
		}

	}


}
