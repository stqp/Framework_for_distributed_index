package log_analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

public class GetAnalyzer extends AbstractAnalyzer{
	
	private static String dirName = "load";
	private MyStringBuilder getCountResult= new MyStringBuilder("getCountResult");
	ArrayList<MyStringBuilder> resultList = new ArrayList<MyStringBuilder>();
	/*
	 * 負荷集計を行うインターバル中に何回のgetリクエストが来ているのかカウント
	 */
	private int getCount = 0;
	
	
	
	
	public GetAnalyzer(){
		resultList.add(this.getCountResult);
	}
	
	
	
	@Override
	public void analyze(String line) {
		if(line.startsWith("myLoad")){//AnalyzerManager.getLogCheckLoadTag())){
			if(getCount > 0){
				getCountResult.append(makeLogLineNotToRemoveTag(""+getCount));
				getCount = 0;
			}
			
		}
		/*
		 * protocol.
		 * 
		 * items[0]: tag-name
		 */
		if(line.startsWith(AnalyzerManager.getPurePutCountTag())){
			getCount++;
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
	}

	
	
	@Override
	public void clear() {
		// TODO 自動生成されたメソッド・スタブ
		
	}

	@Override
	public void beforeWriteResult(String analyzerResultDirPath,
			String fileName) {
		for(MyStringBuilder msb : resultList){
			createDir(analyzerResultDirPath+ "/"+ dirName+ "/"+ msb.getName());
		}
		
	}

}
