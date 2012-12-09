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

	private static String dirName = "load";

	private MyStringBuilder result= new MyStringBuilder("");//"";

	private MyStringBuilder checkLoadResult= new MyStringBuilder("checkLoadResult");//"checkLoadResult"+ returnChar;

	private MyStringBuilder loadResule= new MyStringBuilder("loadResult");//"loadResult" + returnChar;

	private MyStringBuilder putResult= new MyStringBuilder("putResult");//"putResult"+ returnChar;

	private MyStringBuilder getResult= new MyStringBuilder("getResult");//"getResult"+ returnChar;

	private MyStringBuilder rangeResult= new MyStringBuilder("rangeResult");//"rangeResult"+ returnChar;


	ArrayList<MyStringBuilder> resultList = new ArrayList<MyStringBuilder>();


	public LoadAnalyzer(){
		resultList.add(this.result);
		resultList.add(this.checkLoadResult);
		resultList.add(this.loadResule);
		resultList.add(this.putResult);
		resultList.add(this.getResult);
		resultList.add(this.rangeResult);

	}

	public class MyStringBuilder{
		private StringBuilder sb = new StringBuilder();
		private String name ;
		public MyStringBuilder(String name) {
			this.name = name;
		}
		public String getName(){
			return this.name;
		}
		public MyStringBuilder append(String str){
			sb.append(str);
			return this;
		}
		public MyStringBuilder append(char c){
			sb.append(c);
			return this;
		}
		public String toString(){
			return sb.toString();
		}
	}





	public void analyze(String line){

		/*
		 * line's protocol.
		 * check load dataNode1_range_min dataNode1_range_max data_number_in_dataNode1 dataNode2_range_min ....repeat....
		 */
		if(line.startsWith("check dataLoad")){

			List<String>data_numberString_every_dataNode = new ArrayList<String>();
			result.append(returnChar);
			String[] items = splitLine(line);


			// collect data which i want.
			for(int i=4;i<items.length;i=i+3){
				data_numberString_every_dataNode.add(items[i]);
			}

			// make result string.
			result.append("dataNumber every dataNode"+tabChar);
			for(String numberString : data_numberString_every_dataNode){
				result.append(numberString + tabChar);
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
		 * protocol.
		 *   items[0]:OG-LOADBLANCE-CHECKLOAD-TIME <-- this is just a tag.
		 *   items[1]:checkLoad-start-time
		 *   items[2]:checking-load-time  
		 *   items[3]:moving-data-time 
		 *   items[4]:updating-index-time
		 */
		if(line.startsWith(AnalyzerManager.getLogLoadTag())){
			loadResule.append(makeLogLine(line));
		}


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
		if(line.startsWith(AnalyzerManager.getLogCheckLoadTag())){
			checkLoadResult.append(makeLogLine(line));
		}

		/*
		 * to calcurate total time for put query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-put-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogPutTag())){
			putResult.append(makeLogLine(line));
		}

		/*
		 * to calcurate total time for get query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-get-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogGetTag())){
			getResult.append(makeLogLine(line));
		}

		/*
		 * to calcurate total time for range query.
		 * 
		 * protocol
		 * items[0]:just a tag
		 * items[1]:one-range-process-time
		 */
		if(line.startsWith(AnalyzerManager.getLogRangeTag())){
			rangeResult.append(makeLogLine(line));
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
		pri(result.toString());

		pri(checkLoadResult.toString());
		pri(loadResule.toString());
		pri(putResult.toString());
		pri(getResult.toString());
		pri(rangeResult.toString());
	}


	/*
	 * このクラス専用のディレクトリを作成する
	 * (非 Javadoc)
	 * @see analyze.AbstractAnalyzer#beforeWriteResult(java.lang.String, java.lang.String)
	 */
	protected void beforeWriteResult(String analyzerResultDirPath,String fileName){
		for(MyStringBuilder msb : resultList){
			createDir(analyzerResultDirPath+ "/"+ dirName+ "/"+ msb.getName());
		}
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
	public void clear() {}





}