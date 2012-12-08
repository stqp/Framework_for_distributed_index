package message;

import java.io.Serializable;

import com.google.gson.Gson;

import loadBalance.LoadInfoTable;


public class LoadMessage implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String senderIPString;
	
	private LoadInfoTable load;
	
	public LoadMessage(){
		
	}
	
	public LoadMessage(String senderIPString, LoadInfoTable load){
		this.senderIPString = senderIPString;
		this.load = load;
	}
	
	public String getSender(){
		return senderIPString;
	}
	
	public LoadInfoTable getLoadInfoTable(){
		return this.load;
	}
	
	public String toJson(){
		Gson gson = new Gson();
		return gson.toJson(this);
	}
	
}
