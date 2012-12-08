package distributedIndex;

import java.net.InetSocketAddress;

import main.Main;
import node.DataNode;

import util.MessageSender;
import util.MyUtil;

public abstract class AbstractDistributedIndex extends MyUtil implements DistributedIndex{



	//for load balance
	protected static double errorRangeRate = 1.001;

	protected static int maxDataSizeCanBeMoved = 100;


	/*
	 * 
	 */
	protected InetSocketAddress myAddress;


	
	
	
	
	
	
	
	
	
	
	
	
	/*protected abstract void collectLoad();
	
	
	public abstract void moveData(DataNode[] dataNodesToBeRemoved,
			InetSocketAddress target, MessageSender sender);
	
	
	protected abstract void updateIndex();
	
	protected abstract void sendUpdateInfomation();*/
	
	
	
	
	
	
	
	
	
	
	
	/**
	 *
	 */
	public InetSocketAddress getMyAddress(){
		return this.myAddress;
	}


	
	
	/**
	 *
	 */
	public String getMyAddressIPString(){
		if(this.getMyAddress() == null){
			return "";
		}
		return this.getMyAddress().getAddress().toString();
	}



	/**
	 *
	 */
	public void setMyAddress(InetSocketAddress address){
		this.myAddress = address;
	}



	protected String getNextMachineIPString(){
		if(this.getNextMachine() == null){
			return "";
		}
		String address = this.getNextMachine().getAddress().toString();
		return trimAddressString(address);
	}


	public MessageSender getSender(){
		return Main._handler.getMessageReceiver().getMessageSender();
	}

	public void resetLoadCounter(){
		synchronized (this) {
			DataNode dataNode = getFirstDataNode();
			while(dataNode != null){
				dataNode.resetLoadCounter();
				dataNode = dataNode.getNext();
			}
		}

	}



	public String removeTag(String str, String tag){
		return  str.substring(tag.length());
	}


	/*
	 * 二つのアドレスが等しいかどうか調べます。
	 * 調べる場所はIPアドレスのみです。
	 * ちなみに 192.168.0.102ではなく　/192.168.0.102となる必要があります。
	 */
	public boolean equalsAddress(InetSocketAddress addr1, InetSocketAddress addr2){
		if(addr1 == null || addr2 == null)return false;
		
		String adrStr1 = addr1.getAddress().toString();
		String adrStr2 = addr2.toString();
		String TrimedAddr1 = trimAddressString(adrStr1);
		String TrimedAddr2 = trimAddressString(adrStr2);
		
		return TrimedAddr1.equals(TrimedAddr2);
	}


	
	public String trimAddressString(String str){
		return str.indexOf('/') > 0? str.substring(str.indexOf('/')): str;
	}






}
