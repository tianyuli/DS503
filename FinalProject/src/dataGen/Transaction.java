package dataGen;
import java.lang.String;

public class Transaction {
	private int transID;
	private int custID;
	private String transTotal;
	private int transNumItems;
	private String day;
	private String month;
	private String dayOfWeek;
	
	/**
	 * Regular transaction constructor
	 * 
	 * @param transID			Transaction ID
	 * @param custID			Customer ID
	 * @param transTotal		Total transactions
	 * @param transNumItems		Number of transactions of this customer
	 * @param day				Day of month of the transaction
	 * @param month				Month of year of the transaction
	 * @param dayOfWeek			Day of week of the transaction
	 */
	public Transaction(int transID, int custID, String transTotal,
			int transNumItems, String day, String month, String dayOfWeek) {
		this.transID = transID;
		this.custID = custID;
		this.transTotal = transTotal;
		this.transNumItems = transNumItems;
		this.day = day;
		this.month = month;
		this.dayOfWeek = dayOfWeek;
	}
	
	public String write(){	
		return String.valueOf(transID) + "," + String.valueOf(custID) + ","
				+ String.valueOf(transTotal) + "," + String.valueOf(transNumItems)+ "," + 
				day + "," + month + "," + dayOfWeek +"\n";
	}
}
