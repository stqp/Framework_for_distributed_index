package util;
// Shell.java

import java.util.List;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;
// import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetSocketAddress;

import com.google.gson.Gson;

import message.DataNodeMessage;
import message.LoadMessage;
import node.DataNode;

import loadBalance.LoadChecker;
import loadBalance.LoadInfoReceiver;
import loadBalance.LoadInfoTable;


import command.Command;
import distributedIndex.DistributedIndex;
import distributedIndex.FatBtree;


/**
 * MessageRecieverにメッセージのためのソケット接続がされた後に
 * このクラスが作成されスレッドとして実行される。
 *
 * このクラスはメッセージを読み込んで処理することがしごとである。
 *
 */
public final class Shell extends MyUtil implements Runnable {
	public final static String CRLF = "\r\n";

	private Socket sock;
	private MessageReceiver receiver;

	private BufferedReader in;
	private PrintStream out;

	private final Map<String, Command> commandTable;
	private final DistributedIndex distIndex;
	private final ID id;

	private boolean interactive = false;
	private int signature = 0;

	private LoadInfoReceiver loadInfoReceiver;

	private boolean isSourceShell = false;

	private LoadChecker loadChecker;


	public static List<Command> createCommandList(Class[] commandClasses) {
		List<Command> commandList = new ArrayList<Command>();
		for (Class commandClz: commandClasses) {
			Command cmd;
			try {
				cmd = (Command)commandClz.newInstance();
			}
			catch (Exception e) {
				continue;
			}
			commandList.add(cmd);
		}
		return commandList;
	}



	public static Map<String, Command> createCommandTable(List<Command> commandList) {
		Map<String, Command> commandTable = new HashMap<String, Command>();
		for (int i = commandList.size() - 1; i >= 0; i--) {
			Command cmd = commandList.get(i);
			String[] names = cmd.getNames();
			for (String name: names) {
				commandTable.put(name, cmd);
			}
		}
		return commandTable;
	}




	public Shell(Socket sock, MessageReceiver receiver, Map<String, Command> commandTable, DistributedIndex distIndex, ID id) throws IOException {
		this.sock = sock;
		this.receiver = receiver;
		// this.in = new BufferedInputStream(sock.getInputStream());
		this.in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		this.out = new PrintStream(sock.getOutputStream(), false);
		this.commandTable = commandTable;
		this.distIndex = distIndex;
		this.id = id;
	}



	public Shell(InputStream in, PrintStream out, MessageReceiver receiver, Map<String, Command> commandTable, DistributedIndex distIndex, ID id) {
		this.sock = null;
		this.receiver = receiver;
		// this.in = (in instanceof BufferedInputStream ?
		// 	   (BufferedInputStream)in :
		// 	   new BufferedInputStream(in));
		this.in = new BufferedReader(new InputStreamReader(in));
		this.out = out;
		this.commandTable = commandTable;
		this.distIndex = distIndex;
		this.id = id;
	}


	/*
	 * loadcheckerは定期的に分散インデックス手法に負荷集計を行うように命令を出します。
	 * このクラスは他の計算機からメッセージを受け取るので、負荷情報を受け取った時に
	 * loadcheckerに負荷情報を渡してあげます。
	 */
	public Shell setLoadChecker(LoadChecker loadChecker){
		this.loadChecker = loadChecker;

		if(this.loadChecker == null){
			//ここには到達しませんでした
			priJap("しかしロードチェッカーはヌルです");
		}

		return this;
	}

	public boolean isInteractive() {return this.interactive;}


	public boolean setInteractive(boolean flag) {
		boolean old = this.interactive;
		this.interactive = flag;
		return old;
	}



	public int getSignature() {return this.signature;}


	public int setSignature(int signature) {
		int old = this.signature;
		this.signature = signature;
		return old;
	}

	public InetSocketAddress getRemoteSocketAddress() {
		if (this.sock == null) return null;
		return (InetSocketAddress)this.sock.getRemoteSocketAddress();
	}




	public void setSourceShell(boolean flag) {
		this.isSourceShell = flag;
	}





	public void run() {
		long startTime = 0;

		if (this.isSourceShell) startTime = System.currentTimeMillis();


		while (true) {
			String commandLine = null;
			try {
				commandLine = this.in.readLine();
			}
			catch (IOException e) {
				e.printStackTrace();
			}

			//debug
			if(commandLine.length() > 0){
				//System.out.println("MYTEST "+commandLine);
			}


			boolean quit = this.parseALine(commandLine);
			if (quit || !this.interactive) {
				break;
			}
			Thread t = Thread.currentThread();
			if (t.isInterrupted()) {
				break;
			}
		}


		if (this.sock != null) {
			try {
				this.sock.close();
				this.sock = null;
			}
			catch (IOException e) {
				System.err.print("WARNING SocketChannel#close() IOException." + CRLF);
			}
		}


		try {
			this.in.close();
		}
		catch (IOException e) {
			System.err.print("WARNING close() IOException." + CRLF);
		}


		if (this.isSourceShell) {
			long endTime = System.currentTimeMillis();
			System.err.println("TIME source " + startTime + " " + endTime + " " + (endTime - startTime));
		}
	}


	
	
	
	private boolean parseALine(String commandLine) {
		if (commandLine == null) return true;
		if (commandLine.startsWith("interactive")) {
			setInteractive(true);
			return false;
		}

		if (commandLine.startsWith("signature ")) {
			String[] items = commandLine.split(" ", 3);
			int sig = Integer.parseInt(items[1]);
			setSignature(sig);
			commandLine = items[2];
		}
		else if (commandLine.startsWith("response ")) {
			String[] items = commandLine.split(" ", 5);
			int sig = Integer.parseInt(items[1]);
			int port = Integer.parseInt(items[2]);
			int n = Integer.parseInt(items[3]);
			InetSocketAddress addr = getRemoteSocketAddress();
			receiver.receiveResponse(addr.getHostName() + ":" + port + " " + n + " " + items[4], sig);
			//TODO 負荷分散には必要ない情報なのでコメントアウト
			//System.err.println("THREADCOUNT " + System.currentTimeMillis() + " " + Thread.activeCount());
			return false;
		}
		else if (commandLine.startsWith("message ")) {
			String[] items = commandLine.split(" ");
			String[] temp = new String[items.length - 1];
			System.arraycopy(items, 1, temp, 0, temp.length);
			String res = this.distIndex.handleMessge(getRemoteSocketAddress().getAddress(), id, temp);
			this.out.print(res);
			this.out.flush();
			return false;
		}


		/*
		 * TODO
		 * 負荷分散に関する情報が来たので
		 * １．データ移動するから私たちにデータを渡さないでねーという情報
		 * ２．君にデータを送るよーという情報
		 * ３．データ移動が終わったから結果を送るよーという情報
		 * のいずれかです。たぶん。
		 */
		/*else if(commandLine.startsWith(LoadChecker.getLoadInfoTag() )){
			new LoadInfoReceiver().receive(commandLine,this.distIndex);
		}*/


		//テスト用
		/*else if(commandLine.startsWith("check dataLoad")){


			synchronized (this.distIndex) {
				DataNode dataNode = this.distIndex.getFirstDataNode();
				String result = "check dataLoad";
				while(dataNode != null){
					String minId = dataNode.getMinID().toString();
					String maxId = dataNode.getMaxID().toString();
					int numberOfId = dataNode.size();
					result += " "+ minId +" "+ maxId + " " + numberOfId;
					dataNode = dataNode.getNext();
				}
				System.out.println(result);
				System.out.println(this.distIndex.getName()+".toString:"+this.distIndex.toString());
				System.out.println(this.distIndex.getName()+".toMessage:"+this.distIndex.toMessage());
			}


		}*/

		/*
		 * 隣から負荷情報が送られてきたとき
		 */
		else if(commandLine.startsWith("LOAD_INFO")){


			pri("===== 負荷情報を受け取りました。 ======");
			priJap("受け取ったメッセージは");
			pri(commandLine);

			String temp = commandLine.substring("LOAD_INFO".length());

			priJap("トリミングした後のメッセージは");
			pri(temp);
			Gson gson = new Gson();
			LoadMessage loadmes = gson.fromJson(temp, LoadMessage.class);

			priJap("送り主は");
			pri(loadmes.getSender().toString());
			priJap("データは");
			pri(loadmes.getLoadInfoTable().toJson());

			if(this.distIndex.getMyAddressIPString().length() == 0 || loadmes.getLoadInfoTable() == null) return false;
			this.loadChecker.setLoad(this.distIndex.getMyAddressIPString(),loadmes.getLoadInfoTable());

			return false;

		}

		/*
		 * 負荷分散のためのデータ移動で
		 * データノードが来たとき
		 */
		else if(commandLine.startsWith("LOAD_MOVE_DATA_NODES")){

			priJap("データノードを受け取りました");
			priJap("受け取ったメッセージは");
			pri(commandLine);

			String substring = commandLine.substring("LOAD_MOVE_DATA_NODES".length());
			DataNodeMessage dtnMes = (new Gson()).fromJson(substring, DataNodeMessage.class);

			priJap("送り主は");
			pri(dtnMes.getSenderAddress().toString());

			String res = this.distIndex.recieveAndUpdateDataForLoadMove(dtnMes.getDataNodes(),dtnMes.getSenderAddress());
			this.out.print(res);
			this.out.flush();
			return false;
		}




		/*
		 * 負荷分散のためのデータ移動で
		 * 他の計算機の担当範囲が変わるので
		 * その更新情報が来たとき
		 */
		else if(commandLine.startsWith("LOAD_MOVE_INFO")){
			String temp = commandLine.substring("LOAD_MOVE_INFO".length());
		}



		//テスト用
		/*else if(commandLine.startsWith("MOVE_DATA_NODE")){
			System.out.println(commandLine);
			if(commandLine.startsWith("MOVE_DATA_NODE_TO_RIGHT")){
				System.out.println(commandLine);
				String temp = commandLine.substring("MOVE_DATA_NODE_TO_RIGHT".length());
				System.out.println(temp);
				Gson gson = new Gson();
				ArrayList<String> re = gson.fromJson(temp, ArrayList.class);// <- result string which header string removed.
				for(String str:re){
					System.out.println("GSON: to arraylist from string:" + str);
				}


				List<DataNode> dataNodes = new ArrayList<DataNode>();
				DataNode dataNode = new DataNode();
				try{
					for(String str:re){
						if(str.equals(":")){
							System.out.println("DEBUG_ADDED_DATANODE_SIZE:"+dataNode.size());
							System.out.println("DEBUG_ADDED_DATANODE_toString:"+dataNode.toString());
							dataNodes.add(dataNode);
							dataNode = new DataNode();
						}else if(str != null){
							System.out.println("DEBUG:dataNode_ADDed_this:"+str);

							boolean ack = dataNode.add(new AlphanumericID(str));
							System.out.println("DEBUG_data_ACK:"+ ack);
						}
					}
				}catch(Exception e){
					System.out.println("ERROR: add data nodes");
					e.printStackTrace();
				}




				synchronized (this.distIndex) {
					System.out.println("DEBUG_START_ADD_DATANODE");
					System.out.println("DEBUG:numberOfDataNodes:"+ dataNodes.size());
					System.out.println("DEBUG:"+dataNodes.get(0));
					System.out.println("DEBUG:"+dataNodes.get(1));
					//this.distIndex.addPassedDataNodes(true, dataNodes);
				}
			}else if(commandLine.startsWith("MOVE_DATA_NODE_TO_LEFT")){

			}





		}


		//test用
		else if(commandLine.startsWith("FORCE_MOVE_DATA")){

			System.out.println(commandLine);

			synchronized (this.distIndex) {
				
				 * edn1の右端２つのデータノードをedn2に移動させます。
				 
				DataNode dataNode = this.distIndex.getFirstDataNode();
				while(dataNode.getNext() != null){
					dataNode = dataNode.getNext();
				}
				DataNode[] dataNodes = {dataNode.getPrev(),dataNode};

				try {
					System.out.println(dataNode.getParent().toString());
					System.out.println(dataNode.getParent().toMessage());
					System.out.println(this.distIndex.getNextMachine().toString());
				} catch (Exception e1) {
					e1.printStackTrace();
				}


				//this.distIndex.checkLoad(this.receiver.getMessageSender());

				//this.distIndex.moveRightMostDataNodes(dataNodes, this.distIndex.getNextMachine(), receiver.getMessageSender().setHeader("MOVE_DATA_NODE_TO_RIGHT"));
				
				 * usage:
				 * this.distIndex.moveLeftMostDataNodes(dataNodes, this.distIndex.getNextMachine(), receiver.getMessageSender().setHeader("MOVE_DATA_NODE_TO_LEFT"));
				 

			}
		}
*/



		//set distributedIndex own address.
		if(this.distIndex.getMyAddress() == null && this.sock != null){
			pri(  ((InetSocketAddress) this.sock.getLocalSocketAddress()).toString()  );
			priJap("分散インデックス手法のアドレスを設定");
			this.distIndex.setMyAddress((InetSocketAddress) this.sock.getLocalSocketAddress());
			pri(this.distIndex.getMyAddressIPString());
		}


		//TODO 負荷分散には必要ない情報なのでコメントアウト
		//System.err.println("THREADCOUNT " + System.currentTimeMillis() + " " + Thread.activeCount());


		//  \\s+ は空白、タブ、フォーム フィードなどの任意の空白文字と一致します。[ \f\n\r\t\v] と同じ意味です。
		String[] tokens = commandLine.split("\\s+");

		//　コマンドが入っていることを確認している
		int k = 0;
		for (; k < tokens.length; k++) {
			if (tokens[k].length() > 0) break;
		}
		if (k >= tokens.length) {
			return false;
		}
		String cmdToken = tokens[k];
		if (cmdToken.startsWith("#") || cmdToken.startsWith("%") || cmdToken.startsWith("//")) {
			return false;
		}

		//コマンドを選択している
		Command command = this.commandTable.get(cmdToken);
		if (command == null) {
			return false;
		}

		/*
		 * 　メッセージの先頭部分を取り除いて、コマンドの中身だけを取り出します。
		 * そしてコマンドクラスに中身を渡して実行させています。
		 */
		k++;
		String[] args = new String[tokens.length - k];
		for (int i = 0; i < args.length; i++, k++) {
			args[i] = tokens[k];
		}



		boolean quit = false;
		try {

			command
			.setReceiver(this.receiver)
			.setDistIndex(this.distIndex)
			.setId(this.id)
			.setPrintStream(this.out)
			.setShell(this)
			.setStringArray(args);

			quit = command.execute();


		}
		catch (Throwable e) {
			this.out.println("ERROR execute() Exception" + CRLF);
			e.printStackTrace();
			e.printStackTrace(this.out);
			this.out.flush();
		}
		return quit;
	}
}















/*package util;
// Shell.java

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;
// import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetSocketAddress;


import command.Command;
import distributedIndex.DistributedIndex;

public final class Shell implements Runnable {
	public final static String CRLF = "\r\n";

	private Socket sock;
	private MessageReceiver receiver;

	// private BufferedInputStream in;
	private BufferedReader in;
	private PrintStream out;

	private final Map<String, Command> commandTable;
	private final DistributedIndex distIndex;
	private final ID id;

	private boolean interactive = false;
	private int signature = 0;

	private boolean isSourceShell = false;


	public static List<Command> createCommandList(Class[] commandClasses) {
		List<Command> commandList = new ArrayList<Command>();
		for (Class commandClz: commandClasses) {
			Command cmd;
			try {
				cmd = (Command)commandClz.newInstance();
			}
			catch (Exception e) {
				continue;
			}
			commandList.add(cmd);
		}
		return commandList;
	}



	public static Map<String, Command> createCommandTable(List<Command> commandList) {
		Map<String, Command> commandTable = new HashMap<String, Command>();
		for (int i = commandList.size() - 1; i >= 0; i--) {
			Command cmd = commandList.get(i);
			String[] names = cmd.getNames();
			for (String name: names) {
				commandTable.put(name, cmd);
			}
		}
		return commandTable;
	}




	public Shell(Socket sock, MessageReceiver receiver, Map<String, Command> commandTable, DistributedIndex distIndex, ID id) throws IOException {
		this.sock = sock;
		this.receiver = receiver;
		// this.in = new BufferedInputStream(sock.getInputStream());
		this.in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
		this.out = new PrintStream(sock.getOutputStream(), false);
		this.commandTable = commandTable;
		this.distIndex = distIndex;
		this.id = id;
	}



	public Shell(InputStream in, PrintStream out, MessageReceiver receiver, Map<String, Command> commandTable, DistributedIndex distIndex, ID id) {
		this.sock = null;
		this.receiver = receiver;
		// this.in = (in instanceof BufferedInputStream ?
		// 	   (BufferedInputStream)in :
		// 	   new BufferedInputStream(in));
		this.in = new BufferedReader(new InputStreamReader(in));
		this.out = out;
		this.commandTable = commandTable;
		this.distIndex = distIndex;
		this.id = id;
	}


	public boolean isInteractive() {return this.interactive;}


	public boolean setInteractive(boolean flag) {
		boolean old = this.interactive;
		this.interactive = flag;
		return old;
	}



	public int getSignature() {return this.signature;}


	public int setSignature(int signature) {
		int old = this.signature;
		this.signature = signature;
		return old;
	}

	public InetSocketAddress getRemoteSocketAddress() {
		if (this.sock == null) return null;
		return (InetSocketAddress)this.sock.getRemoteSocketAddress();
	}




	public void setSourceShell(boolean flag) {
		this.isSourceShell = flag;
	}





	public void run() {
		long startTime = 0;

		if (this.isSourceShell) startTime = System.currentTimeMillis();


		while (true) {
			// System.err.println("THREADCOUNT " + System.currentTimeMillis() + " " + Thread.activeCount());
			String commandLine = null;
			try {
				commandLine = this.in.readLine();
			}
			catch (IOException e) {
				e.printStackTrace();
			}

			boolean quit = this.parseALine(commandLine);
			if (quit || !this.interactive) {
				break;
			}
			Thread t = Thread.currentThread();
			if (t.isInterrupted()) {
				break;
			}
		}


		if (this.sock != null) {
			try {
				this.sock.close();
				this.sock = null;
			}
			catch (IOException e) {
				System.err.print("WARNING SocketChannel#close() IOException." + CRLF);
			}
		}


		try {
			this.in.close();
		}
		catch (IOException e) {
			System.err.print("WARNING close() IOException." + CRLF);
		}


		if (this.isSourceShell) {
			long endTime = System.currentTimeMillis();
			System.err.println("TIME source " + startTime + " " + endTime + " " + (endTime - startTime));
		}
	}

	// private int bufSize = 8;
	// private byte[] buf = new byte[bufSize];
	// private synchronized String readLineString() throws IOException {
	// 	int len = this.readLineBytes(0, false);
	// 	if (len == -1) return null;
	// 	return new String(buf, 0, len, ShellServer.ENCODING);
	// }
	// private int readLineBytes(int minLen, boolean recognizeEscape) throws IOException {
	// 	int index = 0;
	// 	loop: while (true) {
	// 	    int b = this.in.read();
	// 	    if (b == -1) {
	// 		if (index == 0) index = -1;
	// 		break loop;
	// 	    }
	// 	    if (index >= minLen) {
	// 		// recognize LF, CR, CR LF, CR <NUL> as line terminator
	// 		if (b == 0x0a) { // LF
	// 		    break loop;
	// 		}
	// 		else if (b == 0x0d) { // CR
	// 		    this.in.mark(1);
	// 		    int next = this.in.read();
	// 		    if (next != 0x0a && next != 0) { // next != LF && next != <NUL>
	// 			this.in.reset();
	// 		    }
	// 		    break loop;
	// 		}
	// 		else if (recognizeEscape && b == 0x5c) { // \
	// 		    int next = this.in.read();
	// 		    switch (next) {
	// 		    case 0x62:	// \b -> BS
	// 			b = 0x08; break;
	// 		    case 0x74:	// \t -> TAB
	// 			b = 0x09; break;
	// 		    case 0x6e:	// \n -> LF
	// 			b = 0x0a; break;
	// 		    case 0x66:	// \f -> FF
	// 			b = 0x0c; break;
	// 		    // case 0x22:	// \" -> "
	// 		    // 	b = 0x22; break;
	// 		    // case 0x27:	// \' -> '
	// 		    // 	b = 0x27; break;
	// 		    case 0x5c:	// \\ -> \
	// 			b = 0x5c; break;
	// 		    case 0x72:	// \r -> CR
	// 			b = 0x0d; break;
	// 		    default:
	// 			// b = 0x5c;
	// 			break;
	// 		    }
	// 		}
	// 	    }
	// 	    if (index >= bufSize) {
	// 		byte[] newBuf = null;
	// 		newBuf = new byte[bufSize << 1];
	// 		System.arraycopy(buf, 0, newBuf, 0, bufSize);
	// 		bufSize <<= 1;
	// 		buf = newBuf;
	// 	    }
	// 	    buf[index++] = (byte)b;
	// 	} // loop: while (true)
	// 	return index;
	// }

	private boolean parseALine(String commandLine) {
		if (commandLine == null) return true;
		if (commandLine.startsWith("interactive")) {
			setInteractive(true);
			return false;
		}

		if (commandLine.startsWith("signature ")) {
			String[] items = commandLine.split(" ", 3);
			int sig = Integer.parseInt(items[1]);
			setSignature(sig);
			commandLine = items[2];
		}
		else if (commandLine.startsWith("response ")) {
			String[] items = commandLine.split(" ", 5);
			int sig = Integer.parseInt(items[1]);
			int port = Integer.parseInt(items[2]);
			int n = Integer.parseInt(items[3]);
			InetSocketAddress addr = getRemoteSocketAddress();
			receiver.receiveResponse(addr.getHostName() + ":" + port + " " + n + " " + items[4], sig);
			System.err.println("THREADCOUNT " + System.currentTimeMillis() + " " + Thread.activeCount());
			return false;
		}
		else if (commandLine.startsWith("message ")) {
			String[] items = commandLine.split(" ");
			String[] temp = new String[items.length - 1];
			System.arraycopy(items, 1, temp, 0, temp.length);
			String res = this.distIndex.handleMessge(getRemoteSocketAddress().getAddress(), id, temp);
			this.out.print(res);
			this.out.flush();
			return false;
		}

		System.err.println("THREADCOUNT " + System.currentTimeMillis() + " " + Thread.activeCount());

		// if (commandLine.length() > 0 && commandLine.charAt(0) == '\0') {
		//     commandLine = commandLine.substring(1);
		// }

		//  \\s+ は空白、タブ、フォーム フィードなどの任意の空白文字と一致します。[ \f\n\r\t\v] と同じ意味です。
		String[] tokens = commandLine.split("\\s+");

		//　コマンドが入っていることを確認している
		int k = 0;
		for (; k < tokens.length; k++) {
			if (tokens[k].length() > 0) break;
		}
		if (k >= tokens.length) {
			return false;
		}
		String cmdToken = tokens[k];
		if (cmdToken.startsWith("#") || cmdToken.startsWith("%") || cmdToken.startsWith("//")) {
			return false;
		}

		//コマンドを選択している
		Command command = this.commandTable.get(cmdToken);
		if (command == null) {
			return false;
		}


 * 　メッセージの先頭部分を取り除いて、コマンドの中身だけを取り出します。
 * そしてコマンドクラスに中身を渡して実行させています。

		k++;
		String[] args = new String[tokens.length - k];
		for (int i = 0; i < args.length; i++, k++) {
			args[i] = tokens[k];
		}

		ShellContext context = new ShellContext(this.receiver, this, this.distIndex, this.id, this.out, cmdToken, args);
		boolean quit = false;
		try {
			quit = command.execute(context);
		}
		catch (Throwable e) {
			this.out.println("ERROR execute() Exception" + CRLF);
			e.printStackTrace();
			e.printStackTrace(this.out);
			this.out.flush();
		}
		return quit;
	}
}
 */