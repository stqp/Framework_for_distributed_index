// PutCommand.java

// import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.sql.PreparedStatement;

public final class PutCommand implements Command {
    private final static String[] NAMES = {"put"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	// put id value : text port : host
	long startTime = System.currentTimeMillis();
	MessageReceiver receiver = context.getMessageReceiver();
	Shell shell = context.getShell();
	DistributedIndex distIndex = context.getDistributedIndex();
	ID id = context.getID();
	PrintStream out = context.getOutputStream();
	String[] args = context.getArguments();

	boolean interactive = shell.isInteractive();
	MessageSender sender = receiver.getMessageSender();

	if (args.length < 2) {
	    if (interactive) {
		out.print("ERROR put" + Shell.CRLF);
		out.flush();
	    }
	    return false;
	}

	StringBuilder sb = new StringBuilder();
	// main algorithm
	try {
	    ID putID = id.getID(args[0]);
	    String value = args[1];
	    String text = null;
	    int putPort = receiver.getPort();
	    InetSocketAddress putAddr = null;
	    if (args.length >= 4) {
		text = args[2];
		putPort = Integer.parseInt(args[3]);
	    }
	    if (args.length == 4) {
		putAddr = shell.getRemoteSocketAddress();
		putAddr = new InetSocketAddress(putAddr.getAddress(), putPort);
	    }
	    else if (args.length == 5) {
		InetAddress putHost = InetAddress.getByName(args[4]);
		putAddr = new InetSocketAddress(putHost, putPort);
	    }
	    Node node = distIndex.updateKey(sender, putID, text);
	    if (node instanceof DataNode) {
		DataNode dataNode = (DataNode)node;
		NodeStatus status = distIndex.updateData(sender, dataNode);
		dataNode.add(sender, putID);
		long dbStartTime = System.currentTimeMillis();
		PreparedStatement put_statement = Main.createPutStatement();
		put_statement.setString(1, putID.toString());
		put_statement.setString(2, value);
		put_statement.execute();
		put_statement.close();
		long dbEndTime = System.currentTimeMillis();
		distIndex.endUpdateData(sender, status);
		if (putAddr == null) {
		    System.err.println("DATALOCATE put local");
		    sb.append("put" + Shell.CRLF);
		}
		else {
		    String msg = "put";
		    sender.sendResponse(msg, shell.getSignature(), putAddr, receiver.getPort(), 0);
		}
		System.err.println("TIME put db " + dbStartTime + " " + dbEndTime + " " + (dbEndTime - dbStartTime));
	    }
	    else {
		if (node == null) {
		    System.err.println("WARNING put none");
		    if (args.length == 2) {
		    }
		    else {
			String msg = "put";
			sender.sendResponse(msg, shell.getSignature(), putAddr, receiver.getPort(), 0);
		    }
		}
		else {
		System.err.println("TENSO put");
		AddressNode addrNode = (AddressNode)node;
		if (args.length == 2) {
		    int sig = Main.random.nextInt();
		    String msg = "put " + args[0] + " " + args[1] + " " + addrNode.getText() + " " + putPort;
		    long waitStartTime = System.currentTimeMillis();
		    sender.send(msg, sig, addrNode.getAddress());
		    String[] responses = receiver.getResponse(shell, sig); // resHost:resPort n msg
		    long waitEndTime = System.currentTimeMillis();
		    if (responses != null) {
			String[] res = responses[0].split(" ", 3);
			sb.append(res[2] + Shell.CRLF);
		    }
		    else {
		    	System.err.println("WARNING put timeout");
		    }
		    System.err.println("TIME put wait " + waitStartTime + " " + waitEndTime + " " + (waitEndTime - waitStartTime));
		}
		else {
		    String msg = "put " + args[0] + " " + args[1] + " " + addrNode.getText() + " " + putPort + " " + putAddr.getAddress().getHostAddress();
		    sender.send(msg, shell.getSignature(), addrNode.getAddress());
		}
		}
	    }
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
	// 
	if (interactive) {
	    out.print(sb);
	    out.flush();
	}

	long endTime = System.currentTimeMillis();
	System.err.println("TIME put " + args.length + " " + startTime + " " + endTime + " " + (endTime - startTime));

	return false;
    }
}
