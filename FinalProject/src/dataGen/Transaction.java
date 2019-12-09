package dataGen;
import java.lang.String;

public class Transaction {
	private int TransID;
	private int CustID;
	private String TransTotal;
	private int TransNumItems;
	private String Day;
	private String Month;
	private String e;
	
	public Transaction(int transID, int custID, String transTotal,
			int transNumItems, String day, String month, String e1) {
		TransID = transID;
		CustID = custID;
		TransTotal = transTotal;
		TransNumItems = transNumItems;
		Day = day;
		Month = month;
		e = e1;
	}
	
	public String write(){	
		return String.valueOf(TransID) + "," + String.valueOf(CustID) + ","
				+ String.valueOf(TransTotal) + "," + String.valueOf(TransNumItems)+ "," + Day + "," + Month + "," + e+"\n";
	}
	
}
