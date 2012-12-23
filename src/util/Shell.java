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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.InetSocketAddress;

import org.apache.commons.lang.SerializationUtils;

import com.google.gson.Gson;

import message.DataNodeMessage;
import message.LoadMessage;
import node.DataNode;

import loadBalance.LoadChecker;
import loadBalance.LoadInfoReceiver;
import loadBalance.LoadInfoTable;


import command.Command;
import distributedIndex.AbstractDistributedIndex;
import distributedIndex.DistributedIndex;
import distributedIndex.FatBtree;
import distributedIndex.PRing;
import distributedIndex.SkipGraph;


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

	volatile private boolean canMoveData=true;

	synchronized private void startMoveData(){
		canMoveData=false;
	}
	synchronized private void endMoveData(){
		canMoveData=true;
	}



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
			/*pri("===== 負荷情報を受け取りました。 ======");
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

			this.loadChecker.setLoad(this.distIndex.getMyAddressIPString(),loadmes.getLoadInfoTable());*/
			((AbstractDistributedIndex)this.distIndex).receiveLoadInfo(commandLine);
			return false;
		}
		/*
		 * 負荷分散のためのデータ移動で
		 * データノードが来たとき
		 */
		else if(commandLine.startsWith("LOAD_MOVE_DATA_NODES")){
			priJap("データを受け取りました");
			/*priJap("受け取ったメッセージは");
			pri(commandLine);
			String substring = commandLine.substring("LOAD_MOVE_DATA_NODES".length());
			DataNodeMessage dtnMes = (new Gson()).fromJson(substring, DataNodeMessage.class);
			priJap("送り主は");
			pri(dtnMes.getSenderAddress().toString());
*/
			
			String res = ((AbstractDistributedIndex)this.distIndex).receiveData(commandLine);


			//String res = this.distIndex.recieveAndUpdateDataForLoadMove(dtnMes.getDataNodes(),dtnMes.getSenderAddress());
			this.out.print(res);
			this.out.flush();
			canMoveData = true;
			return false;
		}
		/*
		 * ゲットが始まる時間をセットします。
		 */
		else if(commandLine.startsWith("get start")){
			pri("get start");
			pri("time for put. sec:"+MyUtil.getElapsedTimeFromQueryStart());
			MyUtil.setQueryTime();

		}
		else if(commandLine.startsWith("range start")){
			pri("range start");
			pri("time for get. sec:"+MyUtil.getElapsedTimeFromQueryStart());//クエリ時間をセットしていないのでgetの終了時間を出力できます。
			MyUtil.setQueryTime();
		}

		else if(commandLine.startsWith("put start")){
			pri("put start");
			pri(MyUtil.getElapsedTimeFromQueryStart());
			MyUtil.setQueryTime();
		}


		/*
		 * オブジェクトを文字列かします
		 */
		else if(commandLine.startsWith("forDebug")){
			priJap("distIndexを文字列か");
			pri("ファイルエンコード:"+System.getProperty("file.encoding"));
			String current =  new File("").getAbsolutePath();
			String filePathForSerializeObject = "object";
			try {

				File file = new File(current+filePathForSerializeObject);
				try{
					if (file.exists()){
						file.delete();
					}
					file.mkdirs();
					file.createNewFile();
				}catch(IOException e){
					e.printStackTrace();
				}

				/*
				 * インデックスのfirstデータノードを文字列化してみます。
				 */
				File firstDataNodeFile = new File(current+ "/"+ filePathForSerializeObject+"/dataNode");
				if(!firstDataNodeFile.exists()){
					firstDataNodeFile.createNewFile();
				}
				FileOutputStream out = new FileOutputStream(firstDataNodeFile);
				ObjectOutputStream outObject = new ObjectOutputStream(out);
				outObject.writeObject(this.distIndex.getFirstDataNode());
				pri(this.distIndex.getFirstDataNode().toMessage());
				pri("size:"+this.distIndex.getFirstDataNode().size());
				outObject.close();
				out.close();


				/*
				 * インデックスを文字列化してみます
				 */
				FileOutputStream outFile = new FileOutputStream("object.txt");
				outObject = new ObjectOutputStream(outFile);
				outObject.writeObject(this.distIndex);
				outObject.close();
				outFile.close();


				pri("文字列化したオブジェクトをもとに戻してみます。");
				FileInputStream inFile = new FileInputStream("object.txt");
				ObjectInputStream inObject = new ObjectInputStream(inFile);
				DistributedIndex skip = null;
				try {
					skip = (SkipGraph)inObject.readObject();
					pri(skip.getName());
				} catch (ClassNotFoundException e) {
					// TODO 自動生成された catch ブロック
					e.printStackTrace();
				}
				inObject.close();
				inFile.close();

				if(skip!=null){
					pri(skip.toMessage());
				}

				inFile = new FileInputStream(current+ "/"+ filePathForSerializeObject+"/dataNode");
				inObject = new ObjectInputStream(inFile);
				DataNode firstDataNode = null;
				try {
					firstDataNode = (DataNode)inObject.readObject();
					pri(firstDataNode.getName());
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				inObject.close();
				inFile.close();

			} catch (IOException e1) {
				e1.printStackTrace();
			}
			pri("文字列化の実験終わり");
			// byte[] by = SerializationUtils.serialize(this.distIndex);
			// System.out.println(by);
		}


		/*
		 * 自分がデータ移動中でないかを調べます。
		 */
		else if(commandLine.startsWith("LOAD_MOVE_QUESTION")){
			priJap("LOAD_MOVE_QUESTION");
			if(canMoveData==true) {
				canMoveData=false;
				this.out.print("OK");
			}else{
				this.out.print("NO");
			}
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











