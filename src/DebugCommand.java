// DebugCommand.java

// 
// OBSOLETE not maintain
// 

// import java.io.IOException;
import java.io.PrintStream;

public final class DebugCommand implements Command {
    private final static String[] NAMES = {"debug"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	System.err.println("Now, debug command is not used.");
	MessageReceiver receiver = context.getMessageReceiver();
	Shell shell = context.getShell();
	DistributedIndex distIndex = context.getDistributedIndex();
	// ID id = context.getID();
	String[] args = context.getArguments();
	boolean interactive = shell.isInteractive(); // not interactive -> send result message
	MessageSender sender = receiver.getMessageSender();
	try {
	    if (receiver.getPort() == 8081) {
		sender.send("debug hoge bar hoo", 0, "localhost", 8082);
	    }
	    else {
		Thread.sleep(10 * 1000);
	    }
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
	StringBuilder sb = new StringBuilder(distIndex.getName() + Shell.CRLF);
	for (String arg: args) {
	    sb.append(arg);
	    sb.append(" ");
	}
	if (sb.length() > 0) {
	    sb.replace(sb.length() - 1, sb.length(), Shell.CRLF);
	}
	PrintStream out = context.getOutputStream();
	out.print("debug command execute" + Shell.CRLF);
	out.print(sb);
	out.flush();
	System.out.println("Debug Command executed.");
	return false;
    }
}
