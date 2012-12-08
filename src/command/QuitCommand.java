package command;
import util.ShellContext;


public final class QuitCommand extends AbstractCommand implements Command {
	
    private final static String[] NAMES = {"quit", "exit"};
    public String[] getNames() {return NAMES;}

    public boolean execute() {
	return true;
    }
}


/*package command;
import util.ShellContext;

// QuitCommand.java

public final class QuitCommand implements Command {
    private final static String[] NAMES = {"quit", "exit"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	return true;
    }
}
*/