package analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import loadBalance.LoadChecker;

public class LoadAnalyzer extends AbstractAnalyzer{

	
	
	private static char tabChar = '\t';
	private static char returnChar = '\n';

	
	private String result="result"+ returnChar;// <--いまは使ってない
	private String checkLoadResult="checkLoadResult"+ returnChar;
	private String loadResule="loadResult" + returnChar;
	private String putResult="putResult"+ returnChar;
	private String getResult="getResult"+ returnChar;
	private String rangeResult="rangeResult"+ returnChar;





	public LoadAnalyzer(){}





	public void analyze(String line){

		/*
		 * line's protocol.
		 * check load dataNode1_range_min dataNode1_range_max data_number_in_dataNode1 dataNode2_range_min ....repeat....
		 */
		if(line.startsWith("check dataLoad")){

			List<String>data_numberString_every_dataNode = new ArrayList<String>();
			result +="\n";
			String[] items = splitLine(line);


			// collect data which i want.
			for(int i=4;i<items.length;i=i+3){
				data_numberString_every_dataNode.add(items[i]);
			}

			// make result string.
			result += "dataNumber every dataNode\t";
			for(String numberString : data_numberString_every_dataNode){
				result += numberString + "\t";
			}

		}


		if(line.startsWith(LoadChecker.getLoadInfoTag())){
			//System.out.println("exactly get load info from previous machine!");
		}



		
		/*
		 * ##### explantation #####
		 * line was splited and the splited Strings were placed to items[].
		 */
		
		/*
		 * to calcurate total time for checkLoad() method.
		 * and calcurate 
		 * 1. checking time for access load and number of data(==key)
		 * 2. moving data node time for load balancing
		 * 3. time to send update information to other computers
		 * 
		 * protocol.
		 * 
		 * items[0]:<-- this is just a tag
		 * items[1]:check-load-start-time
		 * items[2]:access-count
		 * items[3]:data-size-count
		 */
		if(line.startsWith(AnalyzerManager.getLogLoadTag())){
			loadResule += makeLogLine(line);
		}


		
		/*
		 * protocol.
		 *   items[0]:OG-LOADBLANCE-CHECKLOAD-TIME <-- this is just a tag.
		 *   items[1]:checkLoad-start-time
		 *   items[2]:checking-load-time  
		 *   items[3]:moving-data-time 
		 *   items[4]:updating-index-time
		 */
		if(line.startsWith(AnalyzerManager.getLogCheckLoadTag())){
			checkLoadResult += makeLogLine(line);
		}

		/*
		 * to calcurate total time for put query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-put-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogPutTag())){
			putResult += makeLogLine(line);
		}

		/*
		 * to calcurate total time for get query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-get-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogGetTag())){
			getResult+= makeLogLine(line);
		}
		
		/*
		 * to calcurate total time for range query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-range-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogRangeTag())){
			rangeResult+= makeLogLine(line);
		}
	}





	private String makeLogLine(String line){
		String lineToAdd = "";
		String[] items = splitLine(line);
		for(int i=1;i<items.length;i++){
			lineToAdd += items[i]+ tabChar;
		}
		return (lineToAdd + returnChar);
	}



	@Override
	public void showResult() {
		pri("Load");
		pri(result);
		
		pri(checkLoadResult);
		pri(loadResule);
		pri(putResult);
		pri(getResult);
		pri(rangeResult);
	}






	@Override
	public void writeResult(String analyzerResultPath) {
		File newfile = new File( analyzerResultPath);
		try {
			PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(newfile,true)));
			printWriter.write(result);
			
			printWriter.write(checkLoadResult+ "checkLoadResultEnd"+ returnChar);
			printWriter.write(loadResule+ "loadResultEnd"+ returnChar);
			printWriter.write(putResult+ "putResultEnd"+ returnChar);
			printWriter.write(getResult+ "getResultEnd"+ returnChar);
			printWriter.write(rangeResult+ "rangeResultEnd"+ returnChar);
			
			
			
			
			//printWriter.print("dataNumber every dataNode\t");
			//for(String numberString : data_numberString_every_dataNode){
			//	printWriter.print(numberString + "\t");
			//}
			printWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}





	@Override
	public void clear() {
	}





}