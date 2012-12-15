package log_analyze;

import java.io.File;
import java.io.IOException;

import util.MyUtil;



public abstract class AbstractAnalyzer extends MyUtil{

	
	protected static char tabChar = '\t';

	protected static char returnChar = '\n';
	

	public abstract void analyze(String line);

	public abstract void showResult();

	/*
	 * after analyze the log file, write the analyze result to the some file.
	 * that file name maybe log***.txt , but it is analyze result file, sorry.
	 */
	public abstract void writeResult(String analyzerResultDirPath, String fileName);

	public abstract void clear();
	
	public abstract void beforeWriteResult(String analyzerResultDirPath,String fileName);


	protected String[] splitLine(String line){
		return line.split("[\\s　 ]+");
	}
	
	
	/*
	 * 例
	 * when line --> test 100 200
	 * then 100\t200\n
	 */
	protected String makeLogLine(String line){
		String lineToAdd = "";
		String[] items = splitLine(line);
		for(int i=1;i<items.length;i++){
			lineToAdd += items[i]+ tabChar;
		}
		return (lineToAdd + returnChar);
	}
	
	
	/*
	 * 例
	 * when line --> test 100 200
	 * then test\t100\t200\n
	 * 
	 * here, tag is test.
	 */
	protected String makeLogLineNotToRemoveTag(String line){
		String lineToAdd = "";
		String[] items = splitLine(line);
		for(int i=0;i<items.length;i++){
			lineToAdd += items[i]+ tabChar;
		}
		return (lineToAdd + returnChar);
	}


	



}



