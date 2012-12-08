package command;
// StatusCommand.java



import util.ID;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;

import node.DataNode;

public final class StatusCommand extends AbstractCommand implements Command {
	
	
	private final static String[] NAMES = {"status"};
	public String[] getNames() {return NAMES;}

	public boolean execute() {

		boolean interactive = shell.isInteractive();
		MessageSender sender = receiver.getMessageSender();

		
		
		if (args.length == 0) {
			String info = distIndex.toString();
			if (interactive) {
				out.print(info + Shell.CRLF);
				out.flush();
			}
			return false;
		}

		StringBuilder sb = new StringBuilder();
		
		
		try {
			
			for (int i = 0; i < args.length; i++) {
				
				
				String item = args[i];
				
				
				if (item.compareTo("id") == 0) {
					sb.append(distIndex.getID() + Shell.CRLF);
				}
				else if (item.compareTo("data") == 0) {
					ID[] range = {null, null};
					NodeStatus[] status = distIndex.searchData(sender, range);
					DataNode current = distIndex.getFirstDataNode();
					while (current != null) {
						sb.append(current.toMessage() + shell.CRLF);
						current = distIndex.getNextDataNode(current);
					}
					distIndex.endSearchData(sender, status);
				}
				else if (item.compareTo("adjustinfo") == 0) {
					sb.append(distIndex.getAdjustCmdInfo() + Shell.CRLF);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		if (interactive) {
			out.print(sb);
			out.flush();
		}

		return false;
	}
}



/*package command;
// StatusCommand.java


import java.io.PrintStream;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.NodeStatus;
import util.Shell;
import util.ShellContext;

import distributedIndex.DistributedIndex;

import node.DataNode;

public final class StatusCommand implements Command {
	private final static String[] NAMES = {"status"};
	public String[] getNames() {return NAMES;}

	public boolean execute(ShellContext context) {
		// status [distIndex item] ...
		// item => id
		MessageReceiver receiver = context.getMessageReceiver();
		Shell shell = context.getShell();
		DistributedIndex distIndex = context.getDistributedIndex();
		ID id = context.getID();
		PrintStream out = context.getOutputStream();
		String[] args = context.getArguments();

		boolean interactive = shell.isInteractive();
		MessageSender sender = receiver.getMessageSender();

		if (args.length == 0) {
			String info = distIndex.toString();
			if (interactive) {
				out.print(info + Shell.CRLF);
				out.flush();
			}
			return false;
		}

		StringBuilder sb = new StringBuilder();
		try {
			for (int i = 0; i < args.length; i++) {
				String item = args[i];
				if (item.compareTo("id") == 0) {
					sb.append(distIndex.getID() + Shell.CRLF);
				}
				else if (item.compareTo("data") == 0) {
					ID[] range = {null, null};
					NodeStatus[] status = distIndex.searchData(sender, range);
					DataNode current = distIndex.getFirstDataNode();
					while (current != null) {
						sb.append(current.toMessage() + shell.CRLF);
						current = distIndex.getNextDataNode(current);
					}
					// for (NodeStatus s: status) {
					// 	Node node = s.getNode();
					// 	if (node instanceof DataNode) {
					// 	    DataNode dataNode = (DataNode)node;
					// 	    sb.append(dataNode.toMessage() + Shell.CRLF);
					// 	}
					// }
					distIndex.endSearchData(sender, status);
				}
				else if (item.compareTo("adjustinfo") == 0) {
					sb.append(distIndex.getAdjustCmdInfo() + Shell.CRLF);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		if (interactive) {
			out.print(sb);
			out.flush();
		}

		return false;
	}
}
*/