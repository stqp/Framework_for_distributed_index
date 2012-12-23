package log_analyze;

import log_analyze.MyStringBuilder;

/*
 * 名前をつけることができるようにしたかったので。
 */
public class MyStringBuilder{
	private StringBuilder sb = new StringBuilder();
	private String name ;
	private static char  returnChar = '\n';
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
	public MyStringBuilder appendWithReturn(Object obj){
		sb.append(obj.toString()+returnChar);
		return this;
	}
	public String toString(){
		return sb.toString();
	}
}