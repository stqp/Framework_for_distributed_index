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

    private boolean isSourceShell = false;
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
		// commandLine = this.readLineString();
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
	String[] tokens = commandLine.split("\\s+");
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

	Command command = this.commandTable.get(cmdToken);
	if (command == null) {
	    return false;
	}

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
