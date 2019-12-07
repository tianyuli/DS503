package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryExecutor {
	private static String driver = "org.apache.hive.jdbc.HiveDriver";
	private static String url;
	private static String user = "";
	private static String password = "";
	private static Connection connect;
	
	public static void initialize(String db) throws SQLException {
		url = "jdbc:hive2://localhost:10000/" + db;
		
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		connect = DriverManager.getConnection(url, user, password);
	}
	
	public static ResultSet execute(String query) throws SQLException {
		Statement state = connect.createStatement();
		ResultSet result = state.executeQuery(query);
		return result;
	}
	
	public static void main(String[] args) throws SQLException {
		initialize("default");
		ResultSet result = execute("show tables");
		System.out.println(result.getString(1) + result.getString(2));
	}
}
