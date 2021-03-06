package command;
import util.ShellContext;


public final class HaltCommand extends AbstractCommand implements Command {
    private final static String[] NAMES = {"halt", "stop"};
    public String[] getNames() {return NAMES;}

    public boolean execute() {
	// message receiver will stop
	// all shell threads will stop
	System.err.println("WARNING halt command not implement");
	return true;
    }
}


/*package command;
import util.ShellContext;

// HaltCommand.java

public final class HaltCommand implements Command {
    private final static String[] NAMES = {"halt", "stop"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	// message receiver will stop
	// all shell threads will stop
	System.err.println("WARNING halt command not implement");
	return true;
    }
}
*/