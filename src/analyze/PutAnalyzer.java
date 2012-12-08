package analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PutAnalyzer extends AbstractAnalyzer{
	private int totalProcTime_millis;

	private int dbCount;
	private long totalDbTime_millis;

	
	
	
	public PutAnalyzer(){
	}

	
	
	
	public void analyze(String line){

		/*
		 * protocol
		 *  TIME put db startTime_millis endTime_millis diff_millis
		 */
		
		/*
		 * TODO
		 * if(line.startsWith("TIME put db") && splitLine(line).length == 6){
			try{
				totalDbTime_millis += Integer.parseInt(splitLine(line)[5]);
			}catch(Exception e){
				System.out.println("error at : string to integer");
			}
			dbCount++;
		}

		
		 * protocol
		 * TIME put arg.length startTime_millis endTime_millis diff_millis
		 			
		else if(line.startsWith("TIME put") && isInteger(splitLine(line)[2]) ){
			totalProcTime_millis += Integer.parseInt(splitLine(line)[5]);
		}
*/
	}


	public void showResult(){
		/*log("PUT:");
		log("totalProcTime_millis:"+totalProcTime_millis);
		log("db count:"+ dbCount);
		log("db proc time:"+totalDbTime_millis);*/
	}


	@Override
	public void writeResult(String analyzerResultPath) {
		File newfile = new File( analyzerResultPath);	
		try {
			PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(newfile,true)));
			//printWriter.println("put\ttest2");
			printWriter.close();
		} catch (IOException e) {
			System.err.println("erro ");
		}
	}




	@Override
	public void clear() {
		// TODO 自動生成されたメソッド・スタブ
		this.dbCount=0;
		this.totalDbTime_millis=0;
		this.totalProcTime_millis=0;
	}


}