package main;
// Main.java


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;

import java.util.List;
import java.util.Map;
import java.util.Random;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sound.midi.Receiver;
import javax.xml.ws.handler.MessageContext.Scope;

import com.google.gson.Gson;

import util.DBConnector;
import util.ID;
import util.MessageHandler;
import util.MessageReceiver;
import util.Shell;

import command.AdjustCommand;
import command.Command;
import command.CommandManager;
import command.GetCommand;
import command.InitCommand;
import command.PutCommand;
import command.QuitCommand;
import command.RangeCommand;
import command.SourceCommand;
import command.StatusCommand;
import distributedIndex.DistributedIndex;

import loadBalance.*;

public final class Main {


	public final static Random random = new Random(0);
	public static MessageHandler _handler;

	private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/tokuda";
	private static final String JDBC_USER = "tokuda";
	private static final String JDBC_PASSWORD = "tokuda";


	private static DBConnector connection;



	public static Statement createStatement() throws SQLException {
		return Main.connection.createStatement();
	}

	public static PreparedStatement createGetStatement() throws SQLException {
		return DBConnector.createGetStatement();
		//		return Main.connection.prepareStatement("SELECT value FROM data WHERE key = ?");
	}

	public static PreparedStatement createRangeStatement() throws SQLException {
		return DBConnector.createRangeStatement();
		//return Main.connection.prepareStatement("SELECT value FROM data WHERE key >= ? AND key <= ?");
	}

	public static PreparedStatement createPutStatement() throws SQLException {
		return DBConnector.createPutStatement();
		//return Main.connection.prepareStatement("INSERT INTO data (key, value) VALUES (?, ?)");
	}

	public static PreparedStatement createRemoveStatement() throws SQLException {
		return DBConnector.createRemoveStatement();
		//return Main.connection.prepareStatement("DELETE FROM data WHERE key = ?");
	}



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

		CommandManager comManager = new CommandManager();

		DistributedIndex distIndex = null;

		ID id = null;

		int port = Integer.parseInt(args[2]);

		int seed = args[3].hashCode();

		MessageHandler handler = null;


		try {
			DistributedIndex dis = (DistributedIndex)Class.forName("distributedIndex.SkipGraph").newInstance();
			distIndex = (DistributedIndex)Class.forName(args[0]).newInstance();
			id = (ID)Class.forName(args[1]).newInstance();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}


		/*
		 * connect to SQL server.
		 */
		try {
			Class.forName("org.postgresql.Driver");

		}  catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try{
			connection = new DBConnector(JDBC_URL, JDBC_USER, JDBC_PASSWORD).connect();
			//connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
		}catch (SQLException e) {
			System.out.println("error : cannot connect to SQL");
			e.printStackTrace();
		}




		Main.random.setSeed(seed);



		try {
			handler = new MessageHandler(comManager.getCommandTable(), distIndex, id, port);
			Main._handler = handler;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}


		/*
		 * start waiting message
		 */
		MessageReceiver mr = handler.getMessageReceiver();
		mr.start();


		/*
		 *
		 */

		final int loadCheckInterval = 1000;
		System.out.println("NEW LoacChecker here");
		LoadChecker loadChecker = new LoadChecker(loadCheckInterval,distIndex, handler.getMessageReceiver());
		Thread loadCheckerThread = new Thread( loadChecker );
		loadCheckerThread.setDaemon(true);
		loadCheckerThread.start();

		handler.setLoadChecker(loadChecker);

		/*
		 * start ???
		 */
		Shell stdioShell = handler.getStdioShell();
		stdioShell.run();






		/*
		 * here is end of the process.
		 */
		try {
			connection.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}


}






























/*package main;
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

import util.ID;
import util.MessageHandler;
import util.MessageReceiver;
import util.Shell;

import command.AdjustCommand;
import command.Command;
import command.GetCommand;
import command.InitCommand;
import command.PutCommand;
import command.QuitCommand;
import command.RangeCommand;
import command.SourceCommand;
import command.StatusCommand;
import distributedIndex.DistributedIndex;

public final class Main {
	private final static Class[] COMMANDS = {
		InitCommand.class, StatusCommand.class,
		GetCommand.class, RangeCommand.class, PutCommand.class,
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

 *//**
 * 分散インデックスの名前
 * FatBtree or PRing or SkipGraph
 *//*
		DistributedIndex distIndex = null;

		ID id = null;

  *//**
  * port number
  *//*
		int port = 0;

   *//**
   * an argument for function which make random number.
   *//*
		int seed = 0;


		try {
			distIndex = (DistributedIndex)Class.forName(args[0]).newInstance();
			id = (ID)Class.forName(args[1]).newInstance();
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
    */