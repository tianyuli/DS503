package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class QueryExecutor {
	private static String driver = "org.apache.hive.jdbc.HiveDriver";
	private static String url;
	private static String user = "";
	private static String password = "";
	private static Connection connect;

	/**
	 * Constructor of query executor
	 * 
	 * @param db		The name of Hive database
	 */
	public QueryExecutor(String db) {
		try {
			initialize(db);
			System.out.println("Connection Success");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Build connection to the Hive server
	 * 
	 * @param db		The name of Hive database
	 */
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

	/**
	 * Execute a simple hive query
	 * 
	 * @param query			The input Hive query
	 * @throws SQLException
	 */
	public void execute(String query) throws SQLException {
		Statement state;
		state = connect.createStatement();
		state.execute(query);
	}

	/**
	 * Execute a hive query with multiple aggregation function
	 * 
	 * @param query			The input Hive query
	 * @param len			Number of aggregation function
	 * @return				Result of the query
	 */
	public HashMap<String, Double[]> execute(String query, int len) {
		System.out.println("Running: " + query);
		Statement state;
		ResultSet result;
		HashMap<String, Double[]> resultMap = new HashMap<String, Double[]>();
		
		try {
			state = connect.createStatement();
			result = state.executeQuery(query);

			while (result.next()) {
				Double[] v = new Double[len];
				String k = result.getString(1);
				System.out.print(k);
				StringBuffer sb = new StringBuffer();
				
				for (int i = 0; i < len; i++) {
					v[i] = Double.valueOf(result.getString(2 + i));
					sb.append("\t" + v[i]);
					System.out.print("\t" + v[i]);
				}
				
				resultMap.put(k, v);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return resultMap;
	}
}