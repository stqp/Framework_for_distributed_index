package util;

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
	
	
}













