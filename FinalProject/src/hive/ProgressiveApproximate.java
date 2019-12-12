package hive;

import java.sql.SQLException;
import java.util.HashMap;

public class ProgressiveApproximate {
	private QueryExecutor qe;
	private String SQL = "";
	private double threshold;
	private int alpha;
	private int index = 0;
	private Result result;
	private Method method;
	
	/**
	 * Constructor for Progressive Approximation
	 * 
	 * @param sql			The input SQL
	 * @param threshold		Threshold
	 * @param alpha			Sampling rate
	 * @param method		Sampling method
	 */
	public ProgressiveApproximate(String sql, double threshold, int alpha, Method method) {
		qe = new QueryExecutor("default");
		
		this.threshold = threshold;
		this.alpha = alpha;
		this.method = method;
		
		analyzeQL(sql);
	}
	
	/**
	 * Translate SQL to Hive QL
	 * 
	 * @param sql	The input SQL
	 */
	private void analyzeQL(String sql) {
		String[] sp = sql.split("group by");
		
		// Process SQL query based on sampling method
		switch (method) {
		case BLOCK:
			SQL = sp[0] + "TABLESAMPLE("+ alpha +" PERCENT) group by " + sp[1];
			
		case BUCKET:
			SQL = sp[0] + "TABLESAMPLE(BUCKET "+ alpha +" OUT OF 100 ON bucket) group by " + sp[1];
		}

		// Process aggregator
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
	
	/**
	 * Execute the approximation
	 * 
	 * @throws SQLException
	 */
	public void run() throws SQLException {
		System.out.println("Strat Running");
		
		while (index * alpha <= 100) {
			System.out.println("--------------------------");
			System.out.println("Round " + index);
			int i = result.getNoField();
			index++;
			
			// Set random random seed for block sampling
			int seed = (int)(Math.random() * 10000 + 500);;
			String randomseed = "set hive.sample.seednumber=" + seed;
			System.out.println("Execute Query: " + randomseed);
			qe.execute(randomseed);
			
			// Execute the query
			HashMap<String , Double[]> newResult = qe.execute(SQL, i);
			System.out.println("Query Done");
			if (index == 1) {
				result.setResult(newResult);
				continue;
			}
			
			// Loop until reach threshold
			if (result.compareResult(newResult) <= threshold) {
				break;
			}
		}
		
		System.out.println("************************************************");
		System.out.println("FINAL RESULT");
		if(index * alpha >= 100){
			index --;
		}
		result.reportResult(alpha * index);		
	}
}


