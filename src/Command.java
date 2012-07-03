// Command.java

public interface Command {
    String[] getNames();
    boolean execute(ShellContext context) throws Exception;
}
