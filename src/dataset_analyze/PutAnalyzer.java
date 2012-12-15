package dataset_analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import log_analyze.AbstractAnalyzer;
import log_analyze.MyStringBuilder;

public class PutAnalyzer extends AbstractAnalyzer{

	
	
	private static final String dirName = "putAnalyze";
	private MyStringBuilder putToGetResult = new MyStringBuilder("putToGet");
	
	ArrayList<MyStringBuilder> resultList = new ArrayList<MyStringBuilder>();
	
	private int count = 1;
	
	
	PutAnalyzer(){
		this.resultList.add(putToGetResult);
	}
	
	
	
	/*
	 * putのデータからgetをつくるので
	 * かならずだれかにヒットするデータセット
	 */
	private void putToGet(String line){
		String[] items = splitLine(line);
		String queryName = items[0];
		String key = items[1];
		String value = items[2];
		if(count <= 5000){
			putToGetResult.append(makeLogLineNotToRemoveTag("get "+key));
			count++;
		}
	}
	
	@Override
	public void analyze(String line) {
		if(line.startsWith("put")){
			putToGet(line);
		}
		
	}

	@Override
	public void showResult() {
	}

	@Override
	public void writeResult(String analyzerResultDirPath, String fileName) {
		File newfile= null;
		PrintWriter printWriter= null;

		for(MyStringBuilder msb : resultList){
			String fileDir = analyzerResultDirPath +"/"+ dirName+ "/"+ msb.getName()+ "/"+ fileName;
			newfile = createFile(fileDir);
			
			try {
				printWriter = new PrintWriter(new BufferedWriter(new FileWriter(newfile,true)));
				printWriter.write(msb.toString());
				printWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void clear() {
		// TODO 自動生成されたメソッド・スタブ
		
	}

	@Override
	public void beforeWriteResult(String analyzerResultDirPath,String fileName){
		for(MyStringBuilder msb : resultList){
			createDir(analyzerResultDirPath+ "/"+ dirName+ "/"+ msb.getName());
		}
	}

}
