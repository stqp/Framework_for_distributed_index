// RangeCommand.java

import java.util.ArrayList;
import java.util.HashMap;
// import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class RangeCommand implements Command {
    private final static String[] NAMES = {"range"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	// range id_start id_end : text port : host
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
		out.print("ERROR range" + Shell.CRLF);
		out.flush();
	    }
	    return false;
	}

	StringBuilder sb = new StringBuilder();
	// main algorithm
	try {
	    ID startID = id.getID(args[0]);
	    ID endID = id.getID(args[1]);
	    String text = null;
	    int clientReceiverPort = -1;
	    InetSocketAddress clientAddr = null;
	    if (args.length >= 4) {
		text = args[2];
		clientReceiverPort = Integer.parseInt(args[3]);
	    }
	    else {
		int sig = Main.random.nextInt();
		shell.setSignature(sig);
		clientReceiverPort = receiver.getPort();
	    }
	    if (args.length == 4) {
		clientAddr = shell.getRemoteSocketAddress();
		clientAddr = new InetSocketAddress(clientAddr.getAddress(), clientReceiverPort);
	    }
	    else if (args.length == 5) {
		InetAddress clientHost = InetAddress.getByName(args[4]);
		clientAddr = new InetSocketAddress(clientHost, clientReceiverPort);
	    }
	    Node node = distIndex.searchKey(sender, startID, text);
	    if (node instanceof DataNode) {
		ID[] range = distIndex.getResponsibleRange(sender);
		boolean flag = false;
		if (range[0] != null && range[1] != null && range[0].compareTo(range[1]) > 0) {
		    // for circular ring
		    if (startID.compareTo(range[0]) >= 0) {
		    }
		    else if (/* startID.compareTo(range[0]) < 0 &&*/ startID.compareTo(range[1]) >= 0) {
		    }
		    else if (endID.compareTo(range[1]) < 0) {
		    }
		    else if (args.length == 2) {
			flag = true;
		    }
		    // if (endID.compareTo(range[1]) >= 0) {
		    // 	flag = true;
		    // 	if (endID.compareTo(range[0]) >= 0) {
		    // 	    args[1] = range[0].toMessage();
		    // 	}
		    // }
		}
		else if (range[1] != null && endID.compareTo(range[1]) >= 0) {
		    flag = true;
		}
		if (flag) {
		    System.err.println("NEXTMACHINE range");
		    InetSocketAddress nextAddr = distIndex.getNextMachine();
		    if (nextAddr != null) {
			if (args.length == 2) {
			    String msg = "range " + args[0] + " " + args[1] + " " + "_first_" + " " + clientReceiverPort;
			    sender.send(msg, shell.getSignature(), nextAddr);
			}
			else {
			    String msg = "range " + args[0] + " " + args[1] + " " + "_first_" + " " + clientReceiverPort + " " + clientAddr.getAddress().getHostAddress();
			    sender.send(msg, shell.getSignature(), nextAddr);
			}
		    }
		}
		ArrayList<DataNode> dataNodes = new ArrayList<DataNode>();
		if (range[0] != null && range[1] != null && range[0].compareTo(range[1]) > 0 &&
		    startID.compareTo(range[0]) < 0 && startID.compareTo(range[1]) < 0 &&
		    endID.compareTo(range[1]) >= 0 &&
		    args.length != 2) {
		    return false;
		}
		// if (range[0] != null && range[1] != null && range[0].compareTo(range[1]) > 0) {
		//     node = distIndex.getFirstDataNode();
		//     while (node instanceof DataNode) {
		// 	DataNode dataNode = (DataNode)node;
		// 	ID[] r = distIndex.getDataNodeRange(dataNode);
		// 	if ((r[1] != null && startID.compareTo(r[1]) >= 0) ||
		// 	    (r[0] != null && endID.compareTo(r[0]) < 0)) {
		// 	    continue;
		// 	}
		// 	dataNodes.add(dataNode);
		// 	node = distIndex.getNextDataNode(dataNode);
		//     }
		// }
		// else {
		    while (node instanceof DataNode) {
			DataNode dataNode = (DataNode)node;
			ID[] r = distIndex.getDataNodeRange(dataNode);
			if ((r[1] != null && startID.compareTo(r[1]) >= 0) ||
			    (r[0] != null && endID.compareTo(r[0]) < 0)) {
			    break;
			}
			dataNodes.add(dataNode);
			node = distIndex.getNextDataNode(dataNode);
		    }
		// }
		NodeStatus[] status = distIndex.searchData(sender, dataNodes.toArray(new DataNode[0]));
		StringBuilder sbRes = new StringBuilder();
		// sbRes.append("keys: ");
		// for (DataNode dataNode: dataNodes) {
		//     ID[] keys = dataNode.getAll();
		//     for (ID key: keys) {
		// 	if (key.compareTo(startID) >= 0 && key.compareTo(endID) <= 0) {
		// 	    sbRes.append(key + " ");
		// 	}
		//     }
		// }
		// sb.append("values: ");
		long dbStartTime = System.currentTimeMillis();
		PreparedStatement range_statement = Main.createRangeStatement();
		range_statement.setString(1, startID.toString());
		range_statement.setString(2, endID.toString());
		ResultSet sql_result = range_statement.executeQuery();
		while (sql_result.next()) {
		    sbRes.append(sql_result.getString("value") + " ");
		}
		sql_result.close();
		long dbEndTime = System.currentTimeMillis();
		distIndex.endSearchData(sender, status);
		if (sbRes.length() > 0) sbRes.delete(sbRes.length() - 1, sbRes.length());
		// sbRes.delete(sbRes.length() - 1, sbRes.length());
		if (sbRes.length() > 16) {
		    sbRes.delete(16, sbRes.length());
		}
		sbRes.insert(0, " ");
		sbRes.insert(0, (range[1] != null) ? range[1].toMessage() : "");
		sbRes.insert(0, " ");
		sbRes.insert(0, (range[0] != null) ? range[0].toMessage() : "");
		String msg = sbRes.toString();
		if (clientAddr != null) {
		    sender.sendResponse(msg, shell.getSignature(), clientAddr, receiver.getPort(), 0);
		}
		else {
		    InetSocketAddress localAddr = new InetSocketAddress(InetAddress.getLocalHost(), clientReceiverPort);
		    sender.sendResponse(msg, shell.getSignature(), localAddr, receiver.getPort(), 0);
		}
		System.err.println("TIME range db " + dbStartTime + " " + dbEndTime + " " + (dbEndTime - dbStartTime));
	    }
	    else {
		if (node == null) {
		    System.err.println("WARNING range none");
		    String msg = "  _none_";
		    if (clientAddr != null) {
			sender.sendResponse(msg, shell.getSignature(), clientAddr, receiver.getPort(), 0);
		    }
		    else {
			InetSocketAddress localAddr = new InetSocketAddress(InetAddress.getLocalHost(), clientReceiverPort);
			sender.sendResponse(msg, shell.getSignature(), localAddr, receiver.getPort(), 0);
		    }
		}
		else {
		System.err.println("TENSO range");
		AddressNode addrNode = (AddressNode)node;
		if (args.length == 2) {
		    String msg = "range " + args[0] + " " + args[1] + " " + addrNode.getText() + " " + clientReceiverPort;
		    sender.send(msg, shell.getSignature(), addrNode.getAddress());
		}
		else {
		    String msg = "range " + args[0] + " " + args[1] + " " + addrNode.getText() + " " + clientReceiverPort + " " + clientAddr.getAddress().getHostAddress();
		    sender.send(msg, shell.getSignature(), addrNode.getAddress());
		}
		}
	    }
	    if (clientAddr == null) {
		long waitStartTime = System.currentTimeMillis();
		// wait
		ArrayList<ID> startIDs = new ArrayList<ID>();
		HashMap<ID,ID> resIDs = new HashMap<ID,ID>();
		while (true) {
		    String[] responses = receiver.getResponse(shell, shell.getSignature()); // resHost:resPort n msg(start end value)
		    for (String response: responses) {
			String[] res = response.split(" ", 5);
			ID resStartID = (res[2].compareTo("") != 0) ? id.toInstance(res[2]) : null;
			ID resEndID = (res[3].compareTo("") != 0) ? id.toInstance(res[3]) : null;
			if (resStartID != null && resEndID != null && resStartID.compareTo(resEndID) > 0) {
			    startIDs.add(resStartID);
			    startIDs.add(null);
			    resIDs.put(resStartID, null);
			    resIDs.put(null, resEndID);
			}
			else {
			    startIDs.add(resStartID);
			    resIDs.put(resStartID, resEndID);
			}
			if (res[4].length() > 0) sb.append(res[4] + " ");
		    }
		    while (true) {
			ArrayList<ID> temp = new ArrayList<ID>();
			boolean flag = true;
			for (ID s: startIDs) {
			    if (!resIDs.containsKey(s)) continue;
			    ID e = resIDs.get(s);
			    if (e != null && resIDs.containsKey(e)) {
				ID t = resIDs.remove(e);
				resIDs.put(s, t);
				flag = false;
			    }
			    temp.add(s);
			}
			startIDs = temp;
			if (flag) break;
		    }
		    boolean flag = false;
		    for (ID s: startIDs) {
			ID e = resIDs.get(s);
			boolean sf = (s == null || s.compareTo(startID) <= 0);
			boolean ef = (e == null || e.compareTo(endID) >= 0);
			flag = sf && ef;
			if (flag) break;
		    }
		    if (flag) break;
		}
		long waitEndTime = System.currentTimeMillis();
		if (sb.length() > 0) sb.delete(sb.length() - 1, sb.length());
		sb.append(Shell.CRLF);
		System.err.println("TIME range wait " + waitStartTime + " " + waitEndTime + " " + (waitEndTime - waitStartTime));
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
	System.err.println("TIME range " + args.length + " " + startTime + " " + endTime + " " + (endTime - startTime));

	return false;
    }
}
