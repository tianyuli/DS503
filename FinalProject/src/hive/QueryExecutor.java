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
	public QueryExecutor(String db) {
		try {
			initialize(db);
			System.out.println("Connection Success!!!");
//			execute("drop table transact");
//			execute("create table transact(Tid int, Cid int, Total float, Num int, Day String, Month String, DayOfWeek String) \n" + 
//					"row format delimited \n" + 
//					"fields terminated by ','");
//			execute("load data inpath '/input/Transactions.csv' into table transact");
//			System.out.println("Done!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
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
	
	public  HashMap<String , Double[] > execute(String query, int len) {
		System.out.println("Running: "+query);
		Statement state;
		ResultSet result;
		HashMap<String , Double[] >  rst = new HashMap<String , Double[]> ();
		try {
			state = connect.createStatement();
			result = state.executeQuery(query);
			
			while(result.next()) {
				Double[] v = new Double[len]; 
				String k = result.getString(1);
				System.out.print(k);
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < len; i++) {
					v[i] = Double.valueOf(result.getString(2+i));
					sb.append("\t" + v[i]);
					System.out.print("\t"+v[i]);
				}
				System.out.println();				
				rst.put(k, v);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return rst;
	}
	public   void execute(String query) throws SQLException {
		Statement state;
		state = connect.createStatement();
		state.execute(query);
	}

}