package util;
// ShellContext.java

import java.io.PrintStream;


import distributedIndex.DistributedIndex;

// immutable
public class ShellContext {
	private MessageReceiver receiver;
	private Shell shell;
	private DistributedIndex distIndex;
	private ID id;
	private PrintStream out;
	private String command;
	private String args[];


	public ShellContext(MessageReceiver receiver, Shell shell, DistributedIndex distIndex, ID id, PrintStream out, String command, String[] args) {
		this.receiver = receiver;
		this.shell = shell;
		this.distIndex = distIndex;
		this.id = id;
		this.out = out;
		this.command = command;
		this.args = args;
	}

	public MessageReceiver getMessageReceiver() {return this.receiver;}
	public Shell getShell() {return this.shell;}
	public DistributedIndex getDistributedIndex() {return this.distIndex;}
	public ID getID() {return this.id;}
	public PrintStream getOutputStream() {return this.out;}
	//public String getCommand() {return this.command;}
	public String[] getArguments() {return this.args;}



}














/*package util;
// ShellContext.java

import java.io.PrintStream;


import distributedIndex.DistributedIndex;

// immutable
public class ShellContext {
	private MessageReceiver receiver;
	private Shell shell;
	private DistributedIndex distIndex;
	private ID id;
	private PrintStream out;
	private String command;
	private String args[];

	private DBConnector dbConnector = null;

	public ShellContext(MessageReceiver receiver, Shell shell, DistributedIndex distIndex, ID id, PrintStream out, String command, String[] args) {
		this.receiver = receiver;
		this.shell = shell;
		this.distIndex = distIndex;
		this.id = id;
		this.out = out;
		this.command = command;
		this.args = args;
	}

	public MessageReceiver getMessageReceiver() {return this.receiver;}
	public Shell getShell() {return this.shell;}
	public DistributedIndex getDistributedIndex() {return this.distIndex;}
	public ID getID() {return this.id;}
	public PrintStream getOutputStream() {return this.out;}
	public String getCommand() {return this.command;}
	public String[] getArguments() {return this.args;}


	public ShellContext setDBConnector(DBConnector co){
		dbConnector = co;
		return this;
	}
	public DBConnector getDBConnector(){
		return dbConnector;
	}
}
*/