// SourceCommand.java

// import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.PrintStream;

public final class SourceCommand implements Command {
    private final static String[] NAMES = {"source", "batch"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	// source filename
	// long startTime = System.currentTimeMillis();
	MessageReceiver receiver = context.getMessageReceiver();
	Shell shell = context.getShell();
	DistributedIndex distIndex = context.getDistributedIndex();
	ID id = context.getID();
	PrintStream out = context.getOutputStream();
	String[] args = context.getArguments();

	boolean interactive = shell.isInteractive();
	MessageSender sender = receiver.getMessageSender();

	if (args.length == 0) {
	    if (interactive) {
		out.print("ERROR source" + Shell.CRLF);
		out.flush();
	    }
	    return false;
	}

	StringBuilder sb = new StringBuilder();
	// 
	// boolean isFile = false;
	if (args[0].compareTo("_status_") == 0) {
	    sb.append("SOURCE: isSourceRun: ");
	    boolean f = Main.runningSourceShell();
	    if (f) {
		sb.append("true" + Shell.CRLF);
	    }
	    else {
		sb.append("false" + Shell.CRLF);
	    }
	}
	else if (args[0].compareTo("_abort_") == 0) {
	    Main.abortSourceShell();
	    sb.append("source" + Shell.CRLF);
	}
	else {
	    try {
		FileInputStream in = new FileInputStream(args[0]);
		Main.runSourceShell(in, System.out);
		// in.close();
	    }
	    catch (FileNotFoundException e) {
		e.printStackTrace();
	    }
	    // catch (IOException e) {
	    // 	e.printStackTrace();
	    // }
	    sb.append("source" + Shell.CRLF);
	    // isFile = true;
	}
	// 
	if (interactive) {
	    out.print(sb);
	    out.flush();
	}

	// long endTime = System.currentTimeMillis();
	// if (isFile) System.err.println("TIME source " + args.length + " " + startTime + " " + endTime + " " + (endTime - startTime));

	return false;
    }
}
