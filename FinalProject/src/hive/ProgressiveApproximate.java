package hive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.stream.Stream;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;

public class ProgressiveApproximate {
	private QueryExecutor qe;
	private String SQL;
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
		StringBuffer sb = new StringBuffer();
		sb.append(sp[0]);
		sb.append("TABLESAMPLE(" + Alpha +" PERCENT)[REPEATABLE(SEED)] group by ");
		SEED = "SEED";
		sb.append(sp[1]);
		SQL = sb.toString();
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
	
	public void run() {
		System.out.println("Run");
		
		while (index * Alpha <= 100) {
//		while (true) {
			System.out.println("--------------------------");
			System.out.println("Round " + index);
			System.out.println(SQL);
			int  i = result.getNoField();
			addingseed();
			HashMap<String , Double[]> newResult = qe.execute(SQL,i);
			index++;
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
	private void addingseed() {
		int newSeed = (int)(Math.random() * 50000 + 1);
		String[] sp= SQL.split(SEED);
		SQL = sp[0] + newSeed+sp[1];
		SEED = ""+newSeed;
	}
}


