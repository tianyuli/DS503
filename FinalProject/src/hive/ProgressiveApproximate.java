package hive;

import java.sql.SQLException;
import java.util.HashMap;

public class ProgressiveApproximate {
	private QueryExecutor qe;
	private String SQL = "";
	private double Threshold;
	private int Alpha;
	private int index = 0;
	private Result result;
	
	
	public ProgressiveApproximate(String sql, double threshold, int alpha) {
		System.out.println(sql);
		qe = new QueryExecutor("default");
		Threshold = threshold;
		Alpha = alpha;
		analyzeQl(sql);
	}
	private void analyzeQl(String sql) {
		String[] sp = sql.split("group by");
		SQL = sp[0] + "TABLESAMPLE("+ Alpha +" PERCENT) group by " + sp[1];
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
	}
	
	public void run() throws SQLException {
		System.out.println("Run");
		
		while (index * Alpha <= 100) {
			System.out.println("--------------------------");
			System.out.println("Round " + index);
			int  i = result.getNoField();
			index++;
			
			int seed = (int)(Math.random() * 10000 + 500);;
			String randomseed = "set hive.sample.seednumber=" + seed;
			System.out.println("Execute Query: " + randomseed);
			qe.execute(randomseed);
			
			HashMap<String , Double[]> newResult = qe.execute(SQL,i);
			System.out.println("Query Done");
			if (index == 1) {
				result.setResult(newResult);
				continue;
			}
			if (result.compareResult(newResult) <= Threshold) {
				break;
			}
		}
		System.out.println("************************************************");
		System.out.println("FINAL RESULT");
		if(index * Alpha >= 100){
			index --;
		}
		result.reportResult(Alpha*index);		
	}
}


