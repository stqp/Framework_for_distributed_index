// Main.java

import java.io.InputStream;
import java.io.PrintStream;

import java.util.List;
import java.util.Map;
import java.util.Random;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;

public final class Main {
    private final static Class[] COMMANDS = {
	InitCommand.class, StatusCommand.class,
	GetCommand.class, RangeCommand.class, PutCommand.class,
	// GetCommand.class, RangeCommand.class, PutCommand.class, RemoveCommand.class,
	AdjustCommand.class, SourceCommand.class,
	QuitCommand.class,// HaltCommand.class,
    };

    private final static List<Command> commandList;
    private final static Map<String, Command> commandTable;
    public final static Random random;

    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/data_text_text";
    private static final String JDBC_USER = "naoki";
    private static final String JDBC_PASSWORD = "";
    private static Connection connection;

    static {
	commandList = Shell.createCommandList(COMMANDS);
	commandTable = Shell.createCommandTable(commandList);
	random = new Random(0);
    }

    public static Statement createStatement() throws SQLException {
	return Main.connection.createStatement();
    }

    public static PreparedStatement createGetStatement() throws SQLException {
	return Main.connection.prepareStatement("SELECT value FROM data WHERE key = ?");
    }

    public static PreparedStatement createRangeStatement() throws SQLException {
	return Main.connection.prepareStatement("SELECT value FROM data WHERE key >= ? AND key <= ?");
    }

    public static PreparedStatement createPutStatement() throws SQLException {
	return Main.connection.prepareStatement("INSERT INTO data (key, value) VALUES (?, ?)");
    }

    public static PreparedStatement createRemoveStatement() throws SQLException {
	return Main.connection.prepareStatement("DELETE FROM data WHERE key = ?");
    }

    private static MessageHandler _handler;
    public static boolean runSourceShell(InputStream in, PrintStream out) {
	return Main._handler.runSourceShell(in, out);
    }

    public static boolean runningSourceShell() {
	return Main._handler.runningSourceShell();
    }

    public static boolean abortSourceShell() {
	return Main._handler.abortSourceShell();
    }

    public static void main(String[] args) {
	(new Main()).start(args);
    }

    public void start(String[] args) {
	if (args.length != 4) {
	    System.err.println("args: method_class id_class port seed");
	    System.exit(1);
	}

	DistributedIndex distIndex = null;
	ID id = null;
	int port = 0;
	int seed = 0;
	try {
	    Class methodClz = Class.forName(args[0]);
	    distIndex = (DistributedIndex)methodClz.newInstance();
	    Class idClz = Class.forName(args[1]);
	    id = (ID)idClz.newInstance();
	    port = Integer.parseInt(args[2]);
	    seed = args[3].hashCode();

	    Class.forName("org.postgresql.Driver");
	    connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
	}
	catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	Main.random.setSeed(seed);

	MessageHandler handler = null;
	try {
	    handler = new MessageHandler(commandTable, distIndex, id, port);
	    Main._handler = handler;
	}
	catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	MessageReceiver mr = handler.getMessageReceiver();
	mr.start();

	Shell stdioShell = handler.getStdioShell();
	stdioShell.run();

	try {
	    connection.close();
	}
	catch (SQLException e) {
	    e.printStackTrace();
	}
    }
}
