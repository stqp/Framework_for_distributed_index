package util;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;


public final class DBConnector {

	private static Connection connection;

	private String url;

	private String user;

	private String password;

	public DBConnector(String url, String user, String password){
		this.url = url;
		this.user = user;
		this.password = password;
	}

	public DBConnector connect() throws SQLException{
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		connection = DriverManager.getConnection(url, user, password);
		return this;
	}


	/*
	 * 
	 */
	public static String get(String key) {
		StringBuilder result = new StringBuilder();
		try {
			PreparedStatement statement = createGetStatement();
			statement.setString(1, key);
			ResultSet sql_result = statement.executeQuery();
			while (sql_result.next()) { // but, always get one result
				result.append(sql_result.getString("value") + " ");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result.toString();
	}

	
	public static String remove(String key){
		StringBuilder result = new StringBuilder();
		try {
			PreparedStatement statement = createRemoveStatement();
			statement.setString(1, key);
			ResultSet sql_result = statement.executeQuery();
			while (sql_result.next()) { // but, always get one result
				result.append(sql_result.getString("value") + " ");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	

	public static Statement createStatement() throws SQLException {
		return connection.createStatement();
	}

	public static PreparedStatement createGetStatement() throws SQLException {
		return connection.prepareStatement("SELECT value FROM data WHERE key = ?");
	}

	public static PreparedStatement createRangeStatement() throws SQLException {
		return connection.prepareStatement("SELECT value FROM data WHERE key >= ? AND key <= ?");
	}

	public static PreparedStatement createPutStatement() throws SQLException {
		return connection.prepareStatement("INSERT INTO data (key, value) VALUES (?, ?)");
	}

	public static PreparedStatement createRemoveStatement() throws SQLException {
		return connection.prepareStatement("DELETE FROM data WHERE key = ?");
	}

	public void close() throws SQLException{
		connection.close();
	}


}
