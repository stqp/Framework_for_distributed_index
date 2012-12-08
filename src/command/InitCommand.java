package command;
// InitCommand.java


import java.util.ArrayList;
// import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.swing.border.AbstractBorder;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.Shell;
import util.ShellContext;

import distributedIndex.DistributedIndex;

import main.Main;
import node.AddressNode;
import node.DataNode;
import node.Node;
import util.NodeStatus;

public final class InitCommand extends AbstractCommand implements Command {

	private final static String[] NAMES = {"init"};

	public String[] getNames() {return NAMES;}


	/*
	 * (非 Javadoc)
	 * shell.java　の parseALine(string)から呼ばれる。
	 *
	 * @see command.Command#execute(util.ShellContext)
	 */
	public boolean execute() {
		// init new_id first_host first_port : new_port : new_ip : text : info


		boolean interactive = shell.isInteractive();

		MessageSender sender = receiver.getMessageSender();

		System.out.println("INIT_ARGS : " + args.toString());
		for(String arg : args){
			System.out.println(arg);
		}
		System.out.println("INIT_args.length : " + args.length);

		if (args.length == 1) {
			ID newID = id.getID(args[0]);
			distIndex.initialize(newID);
			return false;
		}
		/*
		 * when length is 2.
		 */
		else if (args.length < 3) {
			if (interactive) {
				out.print("ERROR init" + Shell.CRLF);
				out.flush();
			}
			return false;
		}

		StringBuilder sb = new StringBuilder();

		
		
		
		/*
		 * arguments[0] is added computer's id.
		 * arguments[1] is host name. to determine host IP.
		 * arguments[2] is port number.
		 */
		try {
			ID newID = id.getID(args[0]);
			InetAddress host = InetAddress.getByName(args[1]);
			InetSocketAddress firstAddr = new InetSocketAddress(host, Integer.parseInt(args[2]));
			
			if (args.length == 3) {
				int sig = 0;
				int newPort = receiver.getPort();
				// need exclusion access control for a joining machine

				synchronized (distIndex) {

					priJap("args.lengthが3のとき");
					distIndex.initialize(newID);
					//distIndex.setMyAddress(firstAddr);//TODO
					

					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort;
					sig = Main.random.nextInt();
					sender.send(msg, sig, firstAddr);


					String[] responses = receiver.getResponse(shell, sig);
					String[] res = responses[0].split(" ");

					int i = 0;
					String[] items = res[i].split(":"); i++;
					InetAddress resHost = InetAddress.getByName(items[0]);
					InetSocketAddress resAddr = new InetSocketAddress(resHost, Integer.parseInt(items[1]));
					int n = Integer.parseInt(res[i]); i++;
					ID resID = id.getID(res[i]); i++;
					String distName = res[i]; i++;
					int distLength = Integer.parseInt(res[i]); i++;


					items = new String[distLength];
					System.arraycopy(res, i, items, 0, distLength); i += distLength;

					DistributedIndex newIndex = distIndex.toInstance(items, id);
					distIndex.initialize(newIndex, resAddr, resID);

					Statement st = Main.createStatement();
					st.execute("DELETE FROM data");
					st.close();

					PreparedStatement put_statement = Main.createPutStatement();

					
					for (; i < res.length; i++) {
						put_statement.setString(1, res[i]);
						if(i+1 < res.length){
							/*
							 * ここで配列の外までアクセスしていてエラーが出ていたので、
							 * if文を使って制御するように変更しました。
							 */
							put_statement.setString(2, res[i + 1]);
						}
						put_statement.execute();
						i++;
					}
					put_statement.close();
				}

				int n = 0;
				InetSocketAddress[] addrs = null;
				String msg = null;
				synchronized (distIndex) {
					addrs = distIndex.getAckMachine();
					msg = "init " + distIndex.getID() + " " + args[1] + " " + args[2] + " " + newPort + " " + "null" + " 0 " + distIndex.toAdjustInfo();
				}
				for (InetSocketAddress addr: addrs) {
					n++;
					sender.send(msg, sig, addr);
				}


				Thread.sleep(5 * 1000);
				while (n != 0) {
					String[] responses = receiver.getResponse(shell, sig);
					for (String response: responses) {
						n--;
						String[] res = response.split(" ", 4);
						String[] items = res[0].split(":");
						InetAddress resHost = InetAddress.getByName(items[0]);
						InetSocketAddress resAddr = new InetSocketAddress(resHost, Integer.parseInt(items[1]));
						n += Integer.parseInt(res[1]);
						ID resID = id.getID(res[2]);
						String text = res[3];
						distIndex.adjust(text, resID, resAddr, null);
					}
				}
				sb.append("init" + Shell.CRLF);
			}



			/*
			 * FatBtreeのとき
			 * このコマンドが実行されるのは計算機それぞれで1回だけだった。
			 * 
			 * end1 edn2の2台で実験したときは
			 * edn1がargs.length 4で
			 * edn2がargs.length 3だった。
			 */

			else if (args.length == 4 || args.length == 6) {
				int newPort = Integer.parseInt(args[3]);
				InetSocketAddress newAddr = null;
				if (args.length == 4) {
					InetSocketAddress remote = shell.getRemoteSocketAddress();
					newAddr = new InetSocketAddress(remote.getAddress(), newPort);
				}
				else {
					InetAddress newHost = InetAddress.getByName(args[4]);
					newAddr = new InetSocketAddress(newHost, newPort);
				}
				
				//distIndex.setMyAddress(newAddr);//TODO
				
				
				System.out.println("INIT_NEW_ADDR : "+ newAddr);
				
				String text = (args.length == 6) ? args[5] : null;
				Node node = distIndex.searchKey(sender, newID, text);
				
				
				
				if (node instanceof DataNode) {
					ID[] newRange = distIndex.getRangeForNew(newID);
					NodeStatus[] status = distIndex.updateData(sender, newRange);
					DistributedIndex newDistIndex = distIndex.splitResponsibleRange(sender, newRange, newID, status, newAddr);
					distIndex.endUpdateData(sender, status);
					// String msg = distIndex.getID() + " " + newDistIndex.toMessage();
					StringBuilder sbMsg = new StringBuilder();
					sbMsg.append(distIndex.getID() + " " + newDistIndex.toMessage());
					ID[] trueNewRange = newDistIndex.getResponsibleRange(sender);
					String where_sql = "";
					if (trueNewRange[0] != null && trueNewRange[1] != null) {
						where_sql = "WHERE key >= '" + trueNewRange[0] + "' AND key < '" + trueNewRange[1] + "'";
					}
					else if (trueNewRange[0] != null) {
						where_sql = "WHERE key >= '" + trueNewRange[0] + "'";
					}
					else if (trueNewRange[1] != null) {
						where_sql = "WHERE key < '" + trueNewRange[1] + "'";
					}
					else {
						System.err.println("WARNING InitCommand#execute trueNewRange");
					}
					String sql = "SELECT key, value FROM data " + where_sql;
					Statement st = Main.createStatement();
					ResultSet res = st.executeQuery(sql);
					while (res.next()) {
						sbMsg.append(" " + res.getString("key") + " " + res.getString("value"));
					}
					st.execute("DELETE FROM data " + where_sql);
					String msg = sbMsg.toString();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 0);
				}
				else {
					AddressNode addrNode = (AddressNode)node;
					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort + " " + newAddr.getAddress().getHostAddress() + " " + addrNode.getText();
					sender.send(msg, shell.getSignature(), addrNode.getAddress());
				}
			}
			else if (args.length == 7) {
				int newPort = Integer.parseInt(args[3]);
				InetSocketAddress newAddr = null;
				if (args[4].compareTo("null") == 0) {
					InetSocketAddress remote = shell.getRemoteSocketAddress();
					newAddr = new InetSocketAddress(remote.getAddress(), newPort);
				}
				else {
					InetAddress newHost = InetAddress.getByName(args[4]);
					newAddr = new InetSocketAddress(newHost, newPort);
				}
				String text = args[5];
				String info = args[6];
				// synchronized (distIndex) {
				AddressNode addrNode = distIndex.adjust(text, newID, newAddr, info);
				// }
				if (addrNode.getAddress() != null) {
					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort + " " + newAddr.getAddress().getHostAddress() + " " + addrNode.getText() + " " + args[6];
					sender.send(msg, shell.getSignature(), addrNode.getAddress());
					msg = distIndex.getID() + " " + addrNode.getText();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 1);
				}
				else {
					String msg = distIndex.getID() + " " + addrNode.getText();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 0);
				}
			}
			else {
				System.err.println("WARNING InitCommand#execute");
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

		return false;
	}
}


/*package command;
// InitCommand.java


import java.util.ArrayList;
// import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import util.ID;
import util.MessageReceiver;
import util.MessageSender;
import util.Shell;
import util.ShellContext;

import distributedIndex.DistributedIndex;

import main.Main;
import node.AddressNode;
import node.DataNode;
import node.Node;
import util.NodeStatus;

public final class InitCommand implements Command {

	private final static String[] NAMES = {"init"};

	public String[] getNames() {return NAMES;}



	 * (非 Javadoc)
	 * shell.java　の parseALine(string)から呼ばれる。
	 *
	 * @see command.Command#execute(util.ShellContext)

	public boolean execute(ShellContext context) {
		// init new_id first_host first_port : new_port : new_ip : text : info

		MessageReceiver receiver = context.getMessageReceiver();

		Shell shell = context.getShell();

		DistributedIndex distIndex = context.getDistributedIndex();

		ID id = context.getID();

		PrintStream out = context.getOutputStream();

		String[] args = context.getArguments();

		boolean interactive = shell.isInteractive();

		MessageSender sender = receiver.getMessageSender();



		if (args.length == 1) {
			ID newID = id.getID(args[0]);
			distIndex.initialize(newID);
			return false;
		}

		 * when length is 2.

		else if (args.length < 3) {
			if (interactive) {
				out.print("ERROR init" + Shell.CRLF);
				out.flush();
			}
			return false;
		}

		StringBuilder sb = new StringBuilder();
		// main algorithm

		 * arguments[0] is added computer's id.
		 * arguments[1] is host name. to determine host IP.
		 * arguments[2] is port number.

		try {
			ID newID = id.getID(args[0]);
			InetAddress host = InetAddress.getByName(args[1]);
			InetSocketAddress firstAddr = new InetSocketAddress(host, Integer.parseInt(args[2]));

			if (args.length == 3) {
				int sig = 0;
				int newPort = receiver.getPort();
				// need exclusion access control for a joining machine
				synchronized (distIndex) {
					distIndex.initialize(newID);
					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort;
					sig = Main.random.nextInt();
					sender.send(msg, sig, firstAddr);
					String[] responses = receiver.getResponse(shell, sig);
					String[] res = responses[0].split(" ");
					int i = 0;
					String[] items = res[i].split(":"); i++;
					InetAddress resHost = InetAddress.getByName(items[0]);
					InetSocketAddress resAddr = new InetSocketAddress(resHost, Integer.parseInt(items[1]));
					int n = Integer.parseInt(res[i]); i++;
					ID resID = id.getID(res[i]); i++;
					String distName = res[i]; i++;
					int distLength = Integer.parseInt(res[i]); i++;
					// int i = 1 + 1 + 1 + 2; // above, resHost:resPort n resID "distIndex Type" length
					// items = new String[res.length - i];
					items = new String[distLength];
					// System.arraycopy(res, i, items, 0, items.length);
					System.arraycopy(res, i, items, 0, distLength); i += distLength;
					DistributedIndex newIndex = distIndex.toInstance(items, id);
					distIndex.initialize(newIndex, resAddr, resID);
					Statement st = Main.createStatement();
					st.execute("DELETE FROM data");
					st.close();
					PreparedStatement put_statement = Main.createPutStatement();
					for (; i < res.length; i++) {
						put_statement.setString(1, res[i]);
						put_statement.setString(2, res[i + 1]);
						put_statement.execute();
						i++;
					}
					put_statement.close();
				}
				int n = 0;
				InetSocketAddress[] addrs = null;
				String msg = null;
				synchronized (distIndex) {
					addrs = distIndex.getAckMachine();
					msg = "init " + distIndex.getID() + " " + args[1] + " " + args[2] + " " + newPort + " " + "null" + " 0 " + distIndex.toAdjustInfo();
				}
				for (InetSocketAddress addr: addrs) {
					n++;
					sender.send(msg, sig, addr);
				}
				Thread.sleep(5 * 1000);
				while (n != 0) {
					String[] responses = receiver.getResponse(shell, sig);
					for (String response: responses) {
						n--;
						String[] res = response.split(" ", 4);
						String[] items = res[0].split(":");
						InetAddress resHost = InetAddress.getByName(items[0]);
						InetSocketAddress resAddr = new InetSocketAddress(resHost, Integer.parseInt(items[1]));
						n += Integer.parseInt(res[1]);
						ID resID = id.getID(res[2]);
						String text = res[3];
						// synchronized (distIndex) {
						distIndex.adjust(text, resID, resAddr, null);
						// }
					}
				}
				sb.append("init" + Shell.CRLF);
			}
			else if (args.length == 4 || args.length == 6) {
				int newPort = Integer.parseInt(args[3]);
				InetSocketAddress newAddr = null;
				if (args.length == 4) {
					InetSocketAddress remote = shell.getRemoteSocketAddress();
					newAddr = new InetSocketAddress(remote.getAddress(), newPort);
				}
				else {
					InetAddress newHost = InetAddress.getByName(args[4]);
					newAddr = new InetSocketAddress(newHost, newPort);
				}
				String text = (args.length == 6) ? args[5] : null;
				Node node = distIndex.searchKey(sender, newID, text);
				if (node instanceof DataNode) {
					ID[] newRange = distIndex.getRangeForNew(newID);
					NodeStatus[] status = distIndex.updateData(sender, newRange);
					DistributedIndex newDistIndex = distIndex.splitResponsibleRange(sender, newRange, newID, status, newAddr);
					distIndex.endUpdateData(sender, status);
					// String msg = distIndex.getID() + " " + newDistIndex.toMessage();
					StringBuilder sbMsg = new StringBuilder();
					sbMsg.append(distIndex.getID() + " " + newDistIndex.toMessage());
					ID[] trueNewRange = newDistIndex.getResponsibleRange(sender);
					String where_sql = "";
					if (trueNewRange[0] != null && trueNewRange[1] != null) {
						where_sql = "WHERE key >= '" + trueNewRange[0] + "' AND key < '" + trueNewRange[1] + "'";
					}
					else if (trueNewRange[0] != null) {
						where_sql = "WHERE key >= '" + trueNewRange[0] + "'";
					}
					else if (trueNewRange[1] != null) {
						where_sql = "WHERE key < '" + trueNewRange[1] + "'";
					}
					else {
						System.err.println("WARNING InitCommand#execute trueNewRange");
					}
					String sql = "SELECT key, value FROM data " + where_sql;
					Statement st = Main.createStatement();
					ResultSet res = st.executeQuery(sql);
					while (res.next()) {
						sbMsg.append(" " + res.getString("key") + " " + res.getString("value"));
					}
					st.execute("DELETE FROM data " + where_sql);
					String msg = sbMsg.toString();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 0);
				}
				else {
					AddressNode addrNode = (AddressNode)node;
					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort + " " + newAddr.getAddress().getHostAddress() + " " + addrNode.getText();
					sender.send(msg, shell.getSignature(), addrNode.getAddress());
				}
			}
			else if (args.length == 7) {
				int newPort = Integer.parseInt(args[3]);
				InetSocketAddress newAddr = null;
				if (args[4].compareTo("null") == 0) {
					InetSocketAddress remote = shell.getRemoteSocketAddress();
					newAddr = new InetSocketAddress(remote.getAddress(), newPort);
				}
				else {
					InetAddress newHost = InetAddress.getByName(args[4]);
					newAddr = new InetSocketAddress(newHost, newPort);
				}
				String text = args[5];
				String info = args[6];
				// synchronized (distIndex) {
				AddressNode addrNode = distIndex.adjust(text, newID, newAddr, info);
				// }
				if (addrNode.getAddress() != null) {
					String msg = "init " + args[0] + " " + args[1] + " " + args[2] + " " + newPort + " " + newAddr.getAddress().getHostAddress() + " " + addrNode.getText() + " " + args[6];
					sender.send(msg, shell.getSignature(), addrNode.getAddress());
					msg = distIndex.getID() + " " + addrNode.getText();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 1);
				}
				else {
					String msg = distIndex.getID() + " " + addrNode.getText();
					sender.sendResponse(msg, shell.getSignature(), newAddr, receiver.getPort(), 0);
				}
			}
			else {
				System.err.println("WARNING InitCommand#execute");
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

		return false;
	}
}
*/