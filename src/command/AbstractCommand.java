package command;

import java.io.PrintStream;

import util.ID;
import util.MessageReceiver;
import util.MyUtil;
import util.Shell;
import distributedIndex.DistributedIndex;

public abstract class AbstractCommand extends MyUtil implements Command{

	protected MessageReceiver receiver;
	protected Shell shell;
	protected DistributedIndex distIndex ;
	protected ID id ;
	protected PrintStream out ;
	protected String[] args ; 
	
	public Command setReceiver(MessageReceiver re){
		receiver = re;return this;
	}
	public Command setShell(Shell sh){
		shell = sh;return this;
	}
	public Command setDistIndex(DistributedIndex dist){
		distIndex = dist;return this;
	}
	public Command setId(ID id){
		this.id = id;return this;
	}
	public Command setPrintStream(PrintStream out){
		this.out = out;return this;
	}
	public Command setStringArray(String[] args){
		this.args = args;return this;
	}
}
