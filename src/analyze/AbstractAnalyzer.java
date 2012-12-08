package analyze;

import java.io.File;

import util.MyUtil;



public abstract class AbstractAnalyzer extends MyUtil{


	

	public abstract void analyze(String line);

	public abstract void showResult();

	/*
	 * after analyze the log file, write the analyze result to the some file.
	 * that file name maybe log***.txt , but it is analyze result file, sorry.
	 */
	protected abstract void writeResult(String analyzerResultPath);

	public abstract void clear();


	protected String[] splitLine(String line){
		return line.split("\\s+");
	}


	protected boolean isInteger(String num) {
		try {
			Integer.parseInt(num);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}



}



