package message;

import com.google.gson.Gson;

public class DataMessage extends AbstractMessage{

	DataNodeMessage dataNodeMessage;
	public DataMessage(){}




	public DataNodeMessage getDataNodeMessage() {
		return dataNodeMessage;
	}
	public void setDataNodeMessage(DataNodeMessage dataNodeMessage) {
		this.dataNodeMessage = dataNodeMessage;
	}



}
