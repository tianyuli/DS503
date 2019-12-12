package hive;

import java.util.HashMap;
import java.util.HashSet;

public class Result {
	private HashSet<String> key = new HashSet<String> ();
	private HashMap<String , Double> sumResult;
	private HashMap<String , Double> countResult;
	private HashMap<String , Double> avgResult;
	private boolean SUM = false;
	private boolean COUNT = false;
	private boolean AVG = false;
	private int noField = 0;
	private int iter = 0;

	/**
	 * Constructor for result
	 * 
	 * @param sum		If the query contains sum function
	 * @param count		If the query contains count function
	 * @param avg		If the query contains avg function
	 */
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
		
		this.setNoField();
	}
	
	/**
	 * Set number of aggregation functions
	 */
	private void setNoField() {
		noField = 0;
		if(SUM) noField++;
		if(COUNT) noField++;
		if(AVG) noField++;
	}
	
	/**
	 * Get number of aggregation functions
	 */
	public int getNoField() {
		return noField;
	}
	
	/**
	 * Store the result set
	 * 
	 * @param result	Result of the hive query
	 */
	public void setResult(HashMap<String , Double[]> result) {
		for (String s: result.keySet()) {
			key.add(s);
			Double[] d = result.get(s);
			int i = 0;
			if(SUM) sumResult.put(s, d[i++]);
			if(COUNT) countResult.put(s, d[i++]);
			if(AVG) avgResult.put(s,d[i++]);
		}
	}
	
	/**
	 * Print out the result
	 * 
	 * @param percentage	Current sample percentage
	 */
	public void reportResult(int percentage) {
		for (String k: key) {
			System.out.print(k);
			if(SUM)	System.out.print("\t" +  sumResult.get(k)*100/percentage);	
			if(COUNT) System.out.print("\t" +  countResult.get(k)*100/percentage);
			if(AVG) System.out.print("\t" +  avgResult.get(k));
			System.out.println();
		}
	}
	
	/**
	 * Compare result of current iteration and previous results
	 * 
	 * @param result	Result of the hive query
	 * @return			The difference between results in percentage
	 */
	public double compareResult(HashMap<String , Double[]> result) {
		// Load data
		HashMap<String , Double> sum = new HashMap<String , Double> ();
		HashMap<String , Double> count = new HashMap<String , Double> ();
		HashMap<String , Double> avg = new HashMap<String , Double> ();
		iter ++;
		
		// Add result of current iteration to the results map
		for(String k: result.keySet()) {
			key.add(k);
			Double[] value = result.get(k);
			int i = 0;
			if(SUM)	sum.put(k, value[i++]);
			if(COUNT) count.put(k, value[i++]);
			if(AVG) avg.put(k, value[i++]);
		}
		
		// Compare
		double diff = 0;
		if(SUM)	diff += compareSum(sum);
		if(COUNT) diff += compareCount(count);
		if(AVG) diff += compareAVG(avg);
		
		System.out.print("Difference: ");
		System.out.println(diff / noField);
		
		return diff / noField;
	}
	
	/**
	 * Evaluate the difference between sums
	 * 
	 * @param result	Result of the hive query
	 * @return			The difference between sums
	 */
	private double compareSum(HashMap<String , Double> result) {
		double value = 0;
		int i = 0;
		boolean hasNew = false;
		
		for(String k: key) {
			Double oldValue = sumResult.get(k);
			Double newValue = result.get(k);
			
			// Handle the first iteration
			if(oldValue == null) {
				hasNew = true;
				sumResult.put(k, newValue);
			}
			
			// new result did not have this key
			if (!hasNew) {
				if(newValue == null) newValue = (double) 0; 
			}
		
			sumResult.put(k, newValue + oldValue);
			value += Math.abs(newValue * iter - oldValue) / oldValue * 100;
			i++;
		}
		
		// Handle the first iteration
		if(hasNew) {
			return 100;
		}
		
		return value / i;
	}

	/**
	 * Evaluate the difference between counts
	 * 
	 * @param result	Result of the hive query
	 * @return			The difference between counts
	 */
	private double compareCount(HashMap<String , Double> result) {
		double value = 0;
		int i = 0;
		boolean hasNew = false;
		
		for(String k: key) {
			Double oldValue = countResult.get(k);
			Double newValue = result.get(k);
			
			// Handle the first iteration
			if(oldValue == null) {
				hasNew = true;
				countResult.put(k, newValue);
			}
			
			if(!hasNew) {
				if(newValue == null) newValue = (double) 0;
			}
			
			countResult.put(k, newValue + oldValue);
			value += Math.abs(newValue * iter - oldValue) / oldValue * 100;
			i++;
		}
		
		// Handle the first iteration
		if(hasNew) {
			return 100;
		}
		
		return value / i;
	}

	/**
	 * Evaluate the difference between averages
	 * 
	 * @param result	Result of the hive query
	 * @return			The difference between averages
	 */
	private double compareAVG(HashMap<String , Double> result) {
		double value = 0;
		int i = 0;
		boolean hasNew = false;
		
		for(String k: key) {
			Double oldValue = avgResult.get(k);
			Double newValue = result.get(k);
			
			// Handle the first iteration
			if(oldValue == null) {
				hasNew = true;
				avgResult.put(k, newValue/iter);
			}
			
			if(!hasNew) {
				if(newValue == null) newValue = (double) 0;
			}
			
			avgResult.put(k, (newValue + oldValue * iter) / (iter + 1));
			value += Math.abs(newValue - oldValue) / oldValue * 100;
			i++;
		}
		
		// Handle the first iteration
		if(hasNew) {
			return 100;
		}
		
		return value / i;
	}	
}
