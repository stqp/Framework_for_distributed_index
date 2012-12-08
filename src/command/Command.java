package command;
import java.io.PrintStream;

import distributedIndex.DistributedIndex;
import util.ID;
import util.MessageReceiver;
import util.Shell;

// Command.java

public interface Command {
	
	
    String[] getNames();
    
    
    
    boolean execute() throws Exception;
    
    
    public Command setReceiver(MessageReceiver re);
	public Command setShell(Shell sh);
	public Command setDistIndex(DistributedIndex dist);
	public Command setId(ID id);
	public Command setPrintStream(PrintStream out);
	public Command setStringArray(String[] args);
}
