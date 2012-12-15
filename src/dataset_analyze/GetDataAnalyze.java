

package dataset_analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import util.AlphanumericID;

import log_analyze.*;

/*
 * データセットの　ゲット　を解析します
 * 主に
 * どの担当範囲にどれだけのゲットが送られるかとかを解析します
 */
public class GetDataAnalyze  extends AbstractAnalyzer{

	
	private static final String dirName = "getAnalyze";
	private MyStringBuilder getQeuryRateResult = new MyStringBuilder("getQeuryRate");
	private int count =1;
	ArrayList<MyStringBuilder> resultList = new ArrayList<MyStringBuilder>();


	public GetDataAnalyze(){
		resultList.add(getQeuryRateResult);
	}
	
	
	@Override
	public void analyze(String line) {
		
		if(line.startsWith("get")){
			String[] items = splitLine(line);
			AlphanumericID getId = new AlphanumericID(items[1]);
			if(getId.compareTo(Main.range0) < 0){
				getQeuryRateResult.append(makeLogLineNotToRemoveTag(count+"  -1"));
			}
			else if(getId.compareTo(Main.range1) < 0){
				getQeuryRateResult.append(makeLogLineNotToRemoveTag(count+"  0"));
			}
			else if(getId.compareTo(Main.range2) < 0){
				getQeuryRateResult.append(makeLogLineNotToRemoveTag(count+"  1"));
			}
			else if(getId.compareTo(Main.range3) < 0){
				getQeuryRateResult.append(makeLogLineNotToRemoveTag(count+"  2"));
			}
			else {
				getQeuryRateResult.append(makeLogLineNotToRemoveTag(count+"  3"));
			}
			count++;
		}
	}


	
	
	@Override
	public void showResult() {}

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
	public void clear() {}

	@Override
	public void beforeWriteResult(String analyzerResultDirPath,
			String fileName) {
		for(MyStringBuilder msb : resultList){
			createDir(analyzerResultDirPath+ "/"+ dirName+ "/"+ msb.getName());
		}

		
	}

}
