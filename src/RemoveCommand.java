// RemoveCommand.java

public final class RemoveCommand implements Command {
    private final static String[] NAMES = {"remove", "delete"};
    public String[] getNames() {return NAMES;}

    public boolean execute(ShellContext context) {
	System.err.println("WARNING remove command not implement");
	return false;
    }
}
