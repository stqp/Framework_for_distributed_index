package util;
// MessageSender.java


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import message.DataMessage;
import message.DataNodeMessage;
import message.MessageConverter;
import node.DataNode;

public class MessageSender implements Serializable{
	private MessageReceiver receiver;
	private String header;

	private Gson gson ;

	public MessageSender(MessageReceiver receiver) {
		this.receiver = receiver;
		this.header = "";
		this.gson = new Gson();
	}

	public MessageSender setHeader(String header){
		this.header = header;
		return this;
	}
	public String getHeader(){
		return header;
	}

	public int getResponsePort() {return this.receiver.getPort();}

	public void send(String msg, int signature, String hostname, int port) throws IOException {
		InetAddress host = InetAddress.getByName(hostname);
		InetSocketAddress addr = new InetSocketAddress(host, port);
		this.send(msg, signature, addr);
	}


	//targeterは送り主、つまり自分
	public String sendDataNodeAndReceive(DataNode[] dataNodes, InetSocketAddress targeter, InetSocketAddress target) throws IOException{
		DataNodeMessage dnm = new DataNodeMessage(dataNodes, targeter);
		DataMessage dm = new DataMessage();
		dm.setDataNodeMessage(dnm);
		return this.sendAndReceive(header+dm.toJson(), target);
	}




	public boolean send(String msg, SocketAddress addr) throws IOException {
		try{
			Socket sock = new Socket();
			sock.connect(addr, 5000);
			OutputStream outs = sock.getOutputStream();
			outs.write((header+msg + Shell.CRLF).getBytes());
			outs.flush();
			sock.close();
			return true;
		}catch(IOException ioe){
			ioe.printStackTrace();
			return false;
		}
	}


	public String customSendAndReceive(String msg, SocketAddress target) throws IOException{
		return this.sendAndReceive(header+msg, target);
	}


	public void send(String msg, int signature, SocketAddress addr) throws IOException {
		Socket sock = new Socket();
		sock.connect(addr, 5000);
		OutputStream outs = sock.getOutputStream();
		outs.write(("signature " + signature + " " + msg + Shell.CRLF).getBytes());
		outs.flush();
		sock.close();
	}

	public void sendResponse(String msg, int signature, SocketAddress addr, int port, int n) throws IOException {
		Socket sock = new Socket();
		sock.connect(addr, 5000);
		OutputStream outs = sock.getOutputStream();
		outs.write(("response " + signature + " " + port + " " + n + " " + msg + Shell.CRLF).getBytes());
		outs.flush();
		sock.close();
	}

	String sendAndReceive(String msg, String hostname, int port) throws IOException {
		InetAddress host = InetAddress.getByName(hostname);
		InetSocketAddress addr = new InetSocketAddress(host, port);
		return this.sendAndReceive(msg, addr);
	}

	public String sendAndReceive(String msg, SocketAddress addr) throws IOException {
		Socket sock = new Socket();
		sock.connect(addr, 5000);
		OutputStream outs = sock.getOutputStream();
		outs.write(("interactive" + Shell.CRLF).getBytes());
		outs.write((msg + Shell.CRLF).getBytes());
		outs.write(("exit" + Shell.CRLF).getBytes());
		outs.flush();
		BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		String res = in.readLine();
		sock.close();
		return res;
	}
}
