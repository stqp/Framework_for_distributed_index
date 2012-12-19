package util;
// MessageHandler.java


import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.Socket;

import loadBalance.LoadChecker;

import command.Command;
import distributedIndex.DistributedIndex;

public class MessageHandler implements Serializable{
	private Map<String, Command> commandTable;
	private DistributedIndex distIndex;
	private ID id;
	private int port;

	private MessageReceiver receiver;
	private Shell stdioShell;

	private Map<Integer, Shell> waitingShell;
	private Map<Integer, String[]> response;


	private Object sourceShellLock = new Object();
	private Thread sourceShellThread;





	private LoadChecker loadChecker;

	public void setLoadChecker(LoadChecker loadChecker){
		this.loadChecker = loadChecker;
	}



	public MessageHandler(Map<String, Command> commandTable, DistributedIndex distIndex, ID id, int port) throws IOException {
		this.commandTable = commandTable;
		this.distIndex = distIndex;
		this.id = id;
		this.port = port;

		this.receiver = new MessageReceiver(this.port, this);
		this.stdioShell = new Shell(System.in, System.out, this.receiver, this.commandTable, this.distIndex, this.id);
		this.stdioShell.setInteractive(true);

		this.waitingShell = new HashMap<Integer, Shell>();
		this.response = new HashMap<Integer, String[]>();
	}

	public MessageReceiver getMessageReceiver() {return this.receiver;}


	public Shell getStdioShell() {return this.stdioShell;}

	//TODO
	//shellにロードチェッカーをセットして返すようにしました。
	public Runnable create(Socket sock) throws IOException {
		//this.loadChecker.getLoadInfoTable().setMaster(sock.getInetAddress().toString());
		return new Shell(sock, this.receiver, this.commandTable, this.distIndex, this.id).setLoadChecker(this.loadChecker);
	}


	public boolean runSourceShell(InputStream in, PrintStream out) {
		synchronized (this.sourceShellLock) {
			if (runningSourceShell() == true) return false;
			Shell sourceShell =  new Shell(in, out, this.receiver, this.commandTable, this.distIndex, this.id);
			sourceShell.setInteractive(true);
			sourceShell.setSourceShell(true);
			Runnable r = sourceShell;
			Thread t = new Thread(r);
			t.setDaemon(false);
			t.start();
			this.sourceShellThread = t;
			return true;
		}
	}


	public boolean runningSourceShell() {
		synchronized (this.sourceShellLock) {
			if (this.sourceShellThread == null) return false;
			Thread.State state = sourceShellThread.getState();
			if (state == Thread.State.TERMINATED) {
				return false;
			}
			return true;
		}
	}


	public boolean abortSourceShell() {
		synchronized (this.sourceShellLock) {
			if (runningSourceShell() == false) return true;
			this.sourceShellThread.interrupt();
			return true;
		}
	}


	public String[] getResponse(Shell shell, int signature) throws InterruptedException {
		String[] res = null;
		for (int i = 0; i < 60; i++) {
			synchronized (this.response) {
				res = this.response.remove(signature);
				if (res != null) {
					break;
				}
				synchronized (this.waitingShell) {
					this.waitingShell.put(signature, shell);
				}
			}				synchronized (shell) {
				shell.wait(3 * 1000);
			}
		}
		return res;
	}


	public void receiveResponse(String msg, int signature) {
		synchronized (this.response) {
			String[] oldRes = this.response.get(signature);
			int newLen = (oldRes == null) ? 1 : oldRes.length + 1;
			String[] newRes = new String[newLen];
			if (newLen > 1) {
				System.arraycopy(oldRes, 0, newRes, 0, newLen - 1);
			}
			newRes[newLen - 1] = msg;
			this.response.put(signature, newRes);
		}

		Shell shell;
		synchronized (this.waitingShell) {
			shell = this.waitingShell.remove(signature);
		}
		if (shell != null) {
			synchronized (shell) {
				shell.notify();
			}
		}
	}
}
