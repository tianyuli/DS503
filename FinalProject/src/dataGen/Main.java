package dataGen;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;    
public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args){
		// Generate 5M lines of Transactions
		try{
			FileWriter csvWriter = new FileWriter("Transactions.csv");
		
			for(int index = 1; index <= 10000000; index++){
				int custID = (int)(Math.random() * 50000 + 1);
				float transTotal = (float) (Math.random() * 100);
				int transNumItems = (int)(Math.random() * 5 + 1 );
				
				int month = (int)(Math.random()*12 + 1);
				int day = 31;
				if(month == 2) {
					day = 28;
				}else if (has30(month)) {
					day = 30;
				}
				day = (int)(Math.random()*day + 1);
				Calendar calendar = new GregorianCalendar(2018, month, day);
				String e = calendar.getDisplayName( Calendar.DAY_OF_WEEK ,Calendar.LONG, Locale.getDefault());
				String m = calendar.getDisplayName( Calendar.MONTH ,Calendar.LONG, Locale.getDefault());
				Transaction t = new Transaction(index, custID, transTotal, transNumItems, String.valueOf(day), m, e);
				csvWriter.append(t.write());
			}
			csvWriter.flush();
			csvWriter.close();
			System.out.println("DONE");
		}catch (Exception e) {
		    e.printStackTrace();
		}
		
	}
	private static boolean has30(int month) {
		int[] m = new int[] {4,6,9,11};
		for (int i : m) {
			if(i == month)return true;
		}
		return false;
	}
}
