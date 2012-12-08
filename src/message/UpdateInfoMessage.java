package message;

import java.net.InetSocketAddress;

import com.google.gson.Gson;

public class UpdateInfoMessage extends AbstractMessage{

	public UpdateInfoMessage(){}
	
	private InetSocketAddress senderMachine;
	
	private InetSocketAddress receiverMachine;
	
	/*
	 * データノードが終わった後の
	 * 送り手側の担当IDを文字列にして格納します
	 */
	private String senderMachineIDString;
	
	/*
	 * データノードが終わった後の
	 * 受け手側の担当IDを文字列にして格納します
	 */
	private String receverMachineIDString;
	
	
	public UpdateInfoMessage(InetSocketAddress senderMachine,
			InetSocketAddress receiverMachine, String senderMachineIDString,
			String receverMachineIDString) {
		super();
		this.senderMachine = senderMachine;
		this.receiverMachine = receiverMachine;
		this.senderMachineIDString = senderMachineIDString;
		this.receverMachineIDString = receverMachineIDString;
	}
	

	public InetSocketAddress getSenderMachine() {
		return senderMachine;
	}



	public InetSocketAddress getReceiverMachine() {
		return receiverMachine;
	}



	public String getSenderMachineIDString() {
		return senderMachineIDString;
	}



	public String getReceverMachineIDString() {
		return receverMachineIDString;
	}

	
	public static UpdateInfoMessage fromJson(String str){
		 return (new Gson()).fromJson(str, UpdateInfoMessage.class);
	}


	
	
}
