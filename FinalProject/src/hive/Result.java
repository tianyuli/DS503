package hive;

import java.util.HashMap;
import java.util.HashSet;

public class Result {
	private HashSet<String> Key = new HashSet<String> ();
	private HashMap<String , Double> sumResult;
	private HashMap<String , Double> countResult;
	private HashMap<String , Double> avgResult;
	private boolean SUM = false;
	private boolean COUNT = false;
	private boolean AVG = false;
	private int noField = 0;
	private int iter = 0;

	public Result(boolean sum, boolean count, boolean avg) {
		if(sum) {
			sumResult = new HashMap<String , Double> ();
			SUM = true;
		}
		if(count) {
			countResult = new HashMap<String , Double> ();
			COUNT = true;
		}
		if(avg) {
			avgResult = new HashMap<String , Double> ();
			AVG = true;
		}
		setNoField();
	}
	
	private void setNoField() {
		noField = 0;
		if(SUM) noField++;
		if(COUNT) noField++;
		if(AVG) noField++;
	}
	public int getNoField() {
		return noField;
	}
	
	public void setResult(HashMap<String , Double[]> result) {
		for (String s: result.keySet()) {
			Key.add(s);
			Double[] d = result.get(s);
			int i = 0;
			if(SUM) sumResult.put(s, d[i++]);
			if(COUNT) countResult.put(s, d[i++]);
			if(AVG) avgResult.put(s,d[i++]);
		}
	}
	
	public void reportResult(int p) {
		for (String s: Key) {
			System.out.print(s);
			if(SUM)	System.out.print("\t" +  sumResult.get(s)*100/p);	
			if(COUNT) System.out.print("\t" +  countResult.get(s)*100/p);
			if(AVG) System.out.print("\t" +  avgResult.get(s));
			System.out.println();
		}
	}
	
	public double compareResult(HashMap<String , Double[]> result) {
		//load data
		HashMap<String , Double> sum = new HashMap<String , Double> ();
		HashMap<String , Double> count = new HashMap<String , Double> ();
		HashMap<String , Double> avg = new HashMap<String , Double> ();
		iter ++;
		for(String s: result.keySet()) {
			Key.add(s);
			Double[] d = result.get(s);
			int i = 0;
			if(SUM)	sum.put(s, d[i++]);
			if(COUNT) count.put(s, d[i++]);
			if(AVG) avg.put(s, d[i++]);
		}
		
		//compare
		double diff = 0;
		if(SUM)	diff += compareSum(sum);
		if(COUNT) diff += compareCount(count);
		if(AVG) diff += compareAVG(avg);
		System.out.print("Difference: ");
		System.out.println(diff / noField);
		return diff / noField;
	}
	private double compareSum(HashMap<String , Double> result) {
		double d = 0;
		int i = 0;
		boolean hasNew = false;
		for(String s: Key) {
			Double o = sumResult.get(s);
			Double n = result.get(s);
			if(o == null) {
				hasNew = true;
				System.out.println("NEWWWWWWWW");
				sumResult.put(s, n);
			}
			if(!hasNew) 	if(n == null) n = (double) 0; // new result didnt have this key
		
			sumResult.put(s, n+o);
//			System.out.println((n*iter - o) / o );
			d += Math.abs(n*iter - o) / o * 100;
			i++;
		}
		if(hasNew)	return 100;
		return d / i;
	}

	private double compareCount(HashMap<String , Double> result) {
		double d = 0;
		int i = 0;
		boolean hasNew = false;
		for(String s: Key) {
			Double o = countResult.get(s);
			Double n = result.get(s);
			if(o == null) {
				hasNew = true;
				countResult.put(s, n);
			}
			if(!hasNew) {
				if(n == null) n = (double) 0;
			}
			countResult.put(s, n+o);
			d += Math.abs(n*iter - o) / o * 100;
			i++;
		}
		if(hasNew)	return 100;
		return d / i;
	}

	private double compareAVG(HashMap<String , Double> result) {
		double d = 0;
		int i = 0;
		boolean hasNew = false;
		for(String s: Key) {
			Double o = avgResult.get(s);
			Double n = result.get(s);
			if(o == null) {
				hasNew = true;
				avgResult.put(s, n/iter);
			}
			if(!hasNew) {
				if(n == null) n = (double) 0;
			}
			avgResult.put(s, (n+o*iter)/(iter+1));
			d += Math.abs(n-o) / o * 100;
			i++;
		}
		if(hasNew)	return 100;
		return d / i;
	}
	
	
}
