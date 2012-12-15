package log_analyze;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


/*
 *
 */
public class AnalyzerManager extends AbstractAnalyzer{


	private List<AbstractAnalyzer> analyzerList;
	


	/*
	 * use this tags for log.
	 * とりあえずタグ情報はすべてこのクラスに格納
	 */
	public static String getLogCheckLoadTag(){
		return "LOG-LOADBLANCE-CHECKLOAD-TIME";
	}
	public static String getLogLoadTag(){
		return "LOG-ACCESS-DATASIZE";
	}
	public static String getLogPutTag(){
		return "LOG-PUT-TIME";
	}
	public static String getLogGetTag(){
		return "LOG-GET-TIME";
	}
	public static String getLogRangeTag(){
		return "LOG-RANGE-TIME";
	}
	public static String getPurePutCountTag()	{
		return "LOG-GET-QUERY-ONCE";
	}





	public AnalyzerManager(){
		this.analyzerList = new ArrayList<AbstractAnalyzer>();
		this.analyzerList.add(new LoadAnalyzer());
		this.analyzerList.add(new GetAnalyzer());
	}





	public void analyze(String line){
		for(AbstractAnalyzer item: analyzerList){
			item.analyze(line);
		}
	}




	@Override
	public void showResult() {
		for(AbstractAnalyzer item: analyzerList){
			item.showResult();
		}
	}

	public void loopItemClear() {
		for(AbstractAnalyzer item: analyzerList){
			item.clear();
		}
	}


	/*
	 * loopItemWriteResultの前処理として、アナライズファイルを格納するディレクトリを
	 * 作成しておきます。
	 */
	public void beforeWriteResult(String analyzerResultDirPath,String fileName){
		createDir(analyzerResultDirPath);
	}



	/*
	 * 解析クラスのwriteResultメソッドをるーぷして呼びます。
	 */
	protected void loopItemWriteResult(String analyzerResultDirPath,String fileName){
		beforeWriteResult(analyzerResultDirPath, fileName);

		pri(analyzerResultDirPath+ "/" + fileName);
		for(AbstractAnalyzer item: analyzerList){
			item.beforeWriteResult(analyzerResultDirPath, fileName);
			item.writeResult(analyzerResultDirPath, fileName);
		}

	}



	public void writeResult(String analyzerResultPath, String fileName) {}

	@Override
	public void clear() {}







}
