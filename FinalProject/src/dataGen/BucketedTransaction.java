package dataGen;

import java.util.Random;

public class BucketedTransaction {
	private int transID;
	private int custID;
	private String transTotal;
	private int transNumItems;
	private String day;
	private String month;
	private String dayOfWeek;
	private int bucket;
	
	/**
	 * Bucketed transaction constructor
	 * 
	 * @param transID			Transaction ID
	 * @param custID			Customer ID
	 * @param transTotal		Total transactions
	 * @param transNumItems		Number of transactions of this customer
	 * @param day				Day of month of the transaction
	 * @param month				Month of year of the transaction
	 * @param dayOfWeek			Day of week of the transaction
	 * @param bucket			Bucket of the transaction
	 */
	public BucketedTransaction(int transID, int custID, String transTotal,
			int transNumItems, String day, String month, String dayOfWeek) {
		this.transID = transID;
		this.custID = custID;
		this.transTotal = transTotal;
		this.transNumItems = transNumItems;
		this.day = day;
		this.month = month;
		this.dayOfWeek = dayOfWeek;
		
		Random r = new Random();
		bucket = r.nextInt(100);
	}
	
	public String write(){	
		return String.valueOf(transID) + "," + String.valueOf(custID) + ","
				+ String.valueOf(transTotal) + "," + String.valueOf(transNumItems)+ "," + 
				day + "," + month + "," + dayOfWeek + "," + bucket + "\n";
	}	
}
