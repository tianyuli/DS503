package hive;

import java.io.FileWriter;
import java.io.IOException;
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
	private int index = 0;
	public QueryExecutor(String db) {
		try {
			initialize(db);
			System.out.println("Connection Success!!!");
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
		System.out.println("Runing: "+query);
		index ++;
		Statement state;
		ResultSet result;
		HashMap<String , Double[] >  rst = new HashMap<String , Double[] > ();
		try {
			state = connect.createStatement();
			result = state.executeQuery(query);
			
			FileWriter csvWriter = new FileWriter(index+"Temp.txt");
			while(result.next()) {
				Double[] v = new Double[len]; 
				String k = result.getString(1);
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < len; i++) {
					v[i] = Double.valueOf(result.getString(2+i));
					sb.append("\t" + v[i]);
				}
				csvWriter.append(k + sb.toString() + "\n");
				 
				rst.put(k, v);
			}
			System.out.println(index+"Temp.txt Saved");
			csvWriter.flush();
			csvWriter.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return rst;
	}
	public static ResultSet execute(String query) throws SQLException {
		Statement state;
		state = connect.createStatement();
		return state.executeQuery(query);		
		
	}
	
//	public static void main(String[] args) throws SQLException {
//		initialize("default");
//////		execute("drop table transaction");
////		 execute("create table tra(Tid int, Cid int, Total float, Num int, Day String, Month String, DayOfWeek String) \n" + 
////				"row format delimited \n" + 
////				"fields terminated by ','");
////		
////		execute("load data inpath '/input/Transactions.csv' into table tra");
//		ResultSet result = execute("select DayOfWeek, sum(Num), count(Num), avg(Num) from tra group by DayOfWeek");
//		while(result.next()) {
//			System.out.print(result.getString(1));
//			System.out.print("\t"+result.getString(2));
//			System.out.print("\t"+result.getString(3));
//			System.out.println("\t"+result.getString(4));
//		}
////		
//	}
}