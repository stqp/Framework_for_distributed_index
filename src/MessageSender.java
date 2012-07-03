// MessageSender.java

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;

public final class MessageSender {
    private MessageReceiver receiver;

    public MessageSender(MessageReceiver receiver) {
	this.receiver = receiver;
    }

    public int getResponsePort() {return this.receiver.getPort();}

    void send(String msg, int signature, String hostname, int port) throws IOException {
    	InetAddress host = InetAddress.getByName(hostname);
    	InetSocketAddress addr = new InetSocketAddress(host, port);
    	this.send(msg, signature, addr);
    }

    void send(String msg, int signature, SocketAddress addr) throws IOException {
    	Socket sock = new Socket();
    	sock.connect(addr, 5000);
    	OutputStream outs = sock.getOutputStream();
    	outs.write(("signature " + signature + " " + msg + Shell.CRLF).getBytes());
    	outs.flush();
    	sock.close();
    }

    void sendResponse(String msg, int signature, SocketAddress addr, int port, int n) throws IOException {
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

    String sendAndReceive(String msg, SocketAddress addr) throws IOException {
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
