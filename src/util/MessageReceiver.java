package util;
// MessageReceiver.java


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;



public class MessageReceiver implements Runnable {
	
	
	private ServerSocket servSock;
	private MessageHandler handler;

	private Thread recvThread;

	public MessageReceiver(int port, MessageHandler handler) throws IOException {
		this.servSock = new ServerSocket();
		this.servSock.setReuseAddress(true);
		
		this.servSock.bind(new InetSocketAddress(port));

		this.handler = handler;
	}

	public void start() {
		synchronized (this) {
			if (this.recvThread == null) {
				this.recvThread = new Thread(this);
				this.recvThread.setName("MessageReceiver");
				this.recvThread.setDaemon(true);
				this.recvThread.start();
			}
		}
	}

	public void run() {
		while (true) {
			// while (Thread.activeCount() > 16) {
			// System.err.println("====");
			// Map<Thread,StackTraceElement[]> traces = Thread.getAllStackTraces();
			// for (Thread t: traces.keySet()) {
			//     StackTraceElement[] trace = traces.get(t);
			//     System.err.println("****");
			//     for (StackTraceElement e: trace) {
			// 	System.err.println(e.toString());
			//     }
			// }
			// 	try {Thread.sleep(100);}
			// 	catch (InterruptedException e) {}
			// }
			Socket sock = null;
			try {
				sock = this.servSock.accept();
			}
			catch (IOException e) {
				System.err.println("MESSAGE MessageReceiver#servSock#accept IOException");
				return;
			}

			
			try {
				Thread t = new Thread(this.handler.create(sock));
				t.setDaemon(false);
				t.start();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}

	
	public String[] getResponse(Shell shell, int signature) throws InterruptedException {
		return this.handler.getResponse(shell, signature);
	}

	public void receiveResponse(String msg, int signature) {
		this.handler.receiveResponse(msg, signature);
	}

	public int getPort() {
		return servSock.getLocalPort();
	}

	public MessageSender getMessageSender() {
		return new MessageSender(this);
	}
}
