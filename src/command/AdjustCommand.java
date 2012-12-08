package command;
// AdjustCommand.java

// import java.io.IOException;


import java.io.PrintStream;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.Shell;
import util.ShellContext;
// import java.net.InetAddress;
// import java.net.InetSocketAddress;

import distributedIndex.DistributedIndex;


public final class AdjustCommand extends AbstractCommand implements Command {


	private final static String[] NAMES = {"adjust"};

	public String[] getNames() {return NAMES;}





	public boolean execute() {
		
		// long startTime = System.currentTimeMillis();
		boolean interactive = shell.isInteractive();
		MessageSender sender = receiver.getMessageSender();
		StringBuilder sb = new StringBuilder();
		
		
		try {
			distIndex.adjustCmd(sender);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		if (interactive) {
			out.print(sb);
			out.flush();
		}

		// long endTime = System.currentTimeMillis();
		// System.err.println("TIME adjust " + args.length + " " + startTime + " " + endTime + " " + (endTime - startTime));

		return false;
	}
}



/*package command;
// AdjustCommand.java

// import java.io.IOException;


import java.io.PrintStream;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.Shell;
import util.ShellContext;
// import java.net.InetAddress;
// import java.net.InetSocketAddress;

import distributedIndex.DistributedIndex;

// import java.sql.PreparedStatement;
// import java.sql.ResultSet;

public final class AdjustCommand implements Command {
    private final static String[] NAMES = {"adjust"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	// adjust
	// long startTime = System.currentTimeMillis();
	MessageReceiver receiver = context.getMessageReceiver();
	Shell shell = context.getShell();
	DistributedIndex distIndex = context.getDistributedIndex();
	ID id = context.getID();
	PrintStream out = context.getOutputStream();
	String[] args = context.getArguments();

	boolean interactive = shell.isInteractive();
	MessageSender sender = receiver.getMessageSender();

	// if (args.length == 0) {
	//     if (interactive) {
	// 	out.print("ERROR get" + Shell.CRLF);
	// 	out.flush();
	//     }
	//     return false;
	// }

	StringBuilder sb = new StringBuilder();
	// main algorithm
	try {
	    distIndex.adjustCmd(sender);
	    // ID getID = id.getID(args[0]);
	    // String text = null;
	    // int getPort = receiver.getPort();
	    // InetSocketAddress getAddr = null;
	    // if (args.length >= 3) {
	    // 	text = args[1];
	    // 	getPort = Integer.parseInt(args[2]);
	    // }
	    // if (args.length == 3) {
	    // 	getAddr = shell.getRemoteSocketAddress();
	    // 	getAddr = new InetSocketAddress(getAddr.getAddress(), getPort);
	    // }
	    // else if (args.length == 4) {
	    // 	InetAddress getHost = InetAddress.getByName(args[3]);
	    // 	getAddr = new InetSocketAddress(getHost, getPort);
	    // }
	    // Node node = distIndex.searchKey(sender, getID, text);
	    // if (node instanceof DataNode) {
	    // 	DataNode dataNode = (DataNode)node;
	    // 	NodeStatus status = distIndex.searchData(sender, dataNode);
	    // 	String value = "";
	    // 	if (dataNode.contains(getID)) {
	    // 	    StringBuilder temp = new StringBuilder();
	    // 	    long dbStartTime = System.currentTimeMillis();
	    // 	    PreparedStatement get_statement = Main.createGetStatement();
	    // 	    get_statement.setString(1, getID.toString());
	    // 	    ResultSet sql_result = get_statement.executeQuery();
	    // 	    while (sql_result.next()) { // but, always get one result
	    // 		temp.append(sql_result.getString("value") + " ");
	    // 	    }
	    // 	    sql_result.close();
	    // 	    get_statement.close();
	    // 	    long dbEndTime = System.currentTimeMillis();
	    // 	    if (sb.length() > 0) sb.delete(sb.length() - 1, sb.length());
	    // 	    value = temp.toString();
	    // 	    System.err.println("TIME get db " + dbStartTime + " " + dbEndTime + " " + (dbEndTime - dbStartTime));
	    // 	}
	    // 	distIndex.endSearchData(sender, status);
	    // 	if (getAddr == null) {
	    // 	    System.err.println("DATALOCATE get local");
	    // 	    sb.append(value + Shell.CRLF);
	    // 	}
	    // 	else {
	    // 	    String msg = value;
	    // 	    sender.sendResponse(msg, shell.getSignature(), getAddr, receiver.getPort(), 0);
	    // 	}
	    // }
	    // else {
	    // 	if (node == null) {
	    // 	    System.err.println("WARNING get none");
	    // 	    if (args.length == 1) {
	    // 	    }
	    // 	    else {
	    // 		String msg = "_none_";
	    // 		sender.sendResponse(msg, shell.getSignature(), getAddr, receiver.getPort(), 0);
	    // 	    }
	    // 	}
	    // 	else {
	    // 	System.err.println("TENSO get");
	    // 	AddressNode addrNode = (AddressNode)node;
	    // 	if (args.length == 1) {
	    // 	    int sig = Main.random.nextInt();
	    // 	    String msg = "get " + args[0] + " " + addrNode.getText() + " " + getPort;
	    // 	    long waitStartTime = System.currentTimeMillis();
	    // 	    sender.send(msg, sig, addrNode.getAddress());
	    // 	    String[] responses = receiver.getResponse(shell, sig); // resHost:resPort n msg
	    // 	    long waitEndTime = System.currentTimeMillis();
	    // 	    if (responses != null && responses[0] != null) {
	    // 		String[] res = responses[0].split(" ", 3);
	    // 		sb.append(res[2] + Shell.CRLF);
	    // 	    }
	    // 	    System.err.println("TIME get wait " + waitStartTime + " " + waitEndTime + " " + (waitEndTime - waitStartTime));
	    // 	}
	    // 	else {
	    // 	    String msg = "get " + args[0] + " " + addrNode.getText() + " " + getPort + " " + getAddr.getAddress().getHostAddress();
	    // 	    sender.send(msg, shell.getSignature(), addrNode.getAddress());
	    // 	}
	    // 	}
	    // }
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
	// 
	if (interactive) {
	    out.print(sb);
	    out.flush();
	}

	// long endTime = System.currentTimeMillis();
	// System.err.println("TIME adjust " + args.length + " " + startTime + " " + endTime + " " + (endTime - startTime));

	return false;
    }
}
 */