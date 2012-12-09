package analyze;

import java.io.File;
import java.io.IOException;

import util.MyUtil;



public abstract class AbstractAnalyzer extends MyUtil{


	

	public abstract void analyze(String line);

	public abstract void showResult();

	/*
	 * after analyze the log file, write the analyze result to the some file.
	 * that file name maybe log***.txt , but it is analyze result file, sorry.
	 */
	protected abstract void writeResult(String analyzerResultPath, String fileName);

	public abstract void clear();
	
	protected abstract void beforeWriteResult(String analyzerResultDirPath,String fileName);


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
	
	public File createFile(String filePath){
		File file = new File(filePath);
		pri(filePath);
		try{
			if (file.exists()){
				file.delete();
			}
			file.createNewFile();
			return file;
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}

	public void createDir(String dirPath){
		pri(dirPath);
		File dir = new File(dirPath);
		dir.mkdirs();
	}



}



