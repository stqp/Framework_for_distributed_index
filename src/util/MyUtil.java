package util;

import java.io.File;
import java.io.IOException;

public class MyUtil {
	
	
	
	/*
	 * 単純にログに出力します。
	 * どこから呼ばれたか知りたいと思うので少し処理を追加しています。
	 */
	public static void pri(String str){
		StringBuilder sb = new StringBuilder() ;
		sb.append(str);
		
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		
		for(StackTraceElement ste: stackTraceElements){
			if(ste.getLineNumber() < 0) continue;
			sb.append("		<----method:" + ste.getMethodName() );
			sb.append(", class:" + ste.getClassName());
			sb.append(", line:" + ste.getLineNumber());
		}
		
		System.out.println(sb.toString());
	}
	
	
	
	public static Long getCurrentTime(){
		return System.currentTimeMillis();
	}
	
	public static void priJap(String str){
		pri("====="+str+"=====");
	}
	
	
	
	public static void pri(int number){
		pri(number + "");
	}
	
	public static void pri(boolean b){
		pri(b+"");
	}
	
	
	/*
	 * 解析用のログを出力するときはこのメソッドを呼んでください。
	 */
	public static void log(Object o){
		if(o instanceof String){
			pri((String)o);
		}
		else if(o instanceof Integer){
			pri((Integer) o);
		}
		else if(o instanceof Boolean){
			pri((Boolean) o);
		}
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













