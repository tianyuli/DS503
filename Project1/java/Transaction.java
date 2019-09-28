package Java;
import java.lang.String;

public class Transaction {
	private int TransID;
	private int CustID;
	private float TransTotal;
	private int TransNumItems;
	private String TransDesc;
	
	public Transaction(int transID, int custID, float transTotal,
			int transNumItems, String transDesc) {
		TransID = transID;
		CustID = custID;
		TransTotal = transTotal;
		TransNumItems = transNumItems;
		TransDesc = transDesc;
	}
	
	public String write(){	
		return String.valueOf(TransID) + "," + String.valueOf(CustID) + ","
				+ String.valueOf(TransTotal) + "," + String.valueOf(TransNumItems)+ "," + TransDesc+"\n";
	}
	
}
