package message;

import java.io.Serializable;

import loadBalance.LoadInfoTable;

import com.google.gson.Gson;


/*
 * caution!
 * sendeIPStringはこのメッセージクラスが使用するためのものであり、
 * 分散手法が使用しないように気をつけてください。
 * なぜならどちらのアドレスも宛先の決定に使用できますが、文字列としてみると少し違います。
 * 
 * 例）
 * getNextIPString() --> 192.168.0.102
 * getNextMachine()  --> edn2/192.168.0.102
 * 
 * 
 * ##### サブクラスにおいては、引数をとらないコンストラクターを必ず作成してください。 ######
 */
public abstract class AbstractMessage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String senderIPString;
	
	
	
	public String getSender(){
		return senderIPString;
	}
	
	
	public String toJson(){
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
}

