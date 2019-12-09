package hive;

import java.sql.SQLException;
import java.util.HashMap;

public class ProgressiveApproximate {
	private QueryExecutor qe;
	private String[] SQL = new String[2];
	private String query = "";
	private double Threshold;
	private int Alpha;
	private int index = 0;
	private Result result;
	private String SEED;
	
	public ProgressiveApproximate(String sql, double threshold, int alpha) {
		System.out.println(sql);
		qe = new QueryExecutor("default");
		Threshold = threshold;
		Alpha = alpha;
		analyzeQl(sql);
	}
	private void analyzeQl(String sql) {
		String[] sp = sql.split("group by");
		SQL[0] = sp[0] + "TABLESAMPLE(";
		SQL[1] = " PERCENT) group by " + sp[1];
		boolean SUM = false;
		boolean COUNT = false;
		boolean AVG = false; 
		sp = sql.split("sum");
		if(sp.length > 1 && sp[1].charAt(0) =='(')	SUM = true;
		sp = sql.split("count");
		if(sp.length > 1 && sp[1].charAt(0) =='(')	COUNT = true;
		sp = sql.split("avg");
		if(sp.length > 1 && sp[1].charAt(0) =='(')	AVG = true;
		result = new Result(SUM, COUNT, AVG);
		System.out.println(SUM);
		System.out.println(COUNT);
		System.out.println(AVG);
	}
	
	public void run() throws SQLException {
		System.out.println("Run");
		
		while (index * Alpha <= 100) {
//		while (true) {
			System.out.println("--------------------------");
			System.out.println("Round " + index);
			int  i = result.getNoField();
			index++;
			addingseed(Alpha*index);
			
			int seed = (int)(Math.random() * 50000 + 1);
			String randomseed = "set hive.sample.seednumber=" + seed;
			System.out.println("Execute Query: " + randomseed);
			qe.execute(randomseed);
			
			HashMap<String , Double[]> newResult = qe.execute(query,i);
			
			if (index == 1) {
				result.setResult(newResult);
				result.reportResult(Alpha*index);
				continue;
			}
			if (result.compareResult(newResult) <= Threshold) {
				break;
			}
		}
		System.out.println("************************************************");
		System.out.println("FINAL RESULT");
		System.out.println("Round " + index);
		result.reportResult(Alpha*index);		
	}
	private void addingseed(int i) {
		int seed = (int)(Math.random() * 50000 + 1);
		String set = "set hive.sample.seednumber=<" + seed+"INTEGER>";
		query = SQL[0] + i + SQL[1];
	}
}

