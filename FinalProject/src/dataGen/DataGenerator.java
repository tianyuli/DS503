package dataGen;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import hive.Method;

public class DataGenerator {
	final static int numberOfTransactions = 100000;
	final static Method implementMethod = Method.BUCKET;
	
	/**
	 * Generate transactions file
	 * 
	 * @param method	The approaching method
	 * @param size		The number of rows to be generated	 
	 */
	private static void generate(Method method, int size) {
		DecimalFormat format = new DecimalFormat("######0.00");
		
		try{
			FileWriter csvWriter = new FileWriter("Transactions.csv");
			
			for (int index = 1; index <= size; index++) {
				// Generate fields
				int custID = (int)(Math.random() * size + 1);
				String transTotal = format.format((float)(Math.random() * 100));
				int transNumItems = (int)(Math.random() * 5 + 1 );
				int month = (int)(Math.random()*12 + 1);
				
				int days = 31;
				if(month == 2) {
					days = 28;
				}
				else if (has30(month)) {
					days = 30;
				}
				int day = (int)(Math.random()*days + 1);
				
				Calendar calendar = new GregorianCalendar(2018, month, day);
				String dayOfWeek = calendar.getDisplayName( Calendar.DAY_OF_WEEK ,Calendar.LONG, Locale.getDefault());
				String monthOfYear = calendar.getDisplayName( Calendar.MONTH ,Calendar.LONG, Locale.getDefault());
				
				// Generate transactions based on the implementation method
				switch (method) {
				case BLOCK:
					Transaction transaction = new Transaction(index, custID, transTotal, transNumItems, String.valueOf(day),
							monthOfYear, dayOfWeek);
					csvWriter.append(transaction.write());
					
				case BUCKET:
					BucketedTransaction bucketedTransaction = new BucketedTransaction(index, custID, transTotal, transNumItems, String.valueOf(day),
							monthOfYear, dayOfWeek);
					csvWriter.append(bucketedTransaction.write());
				}	
			}
			
			csvWriter.flush();
			csvWriter.close();
			
			System.out.println("Data generation complete, " + size + " rows of " + format +
					"data have been generated.");
			
		} catch (IOException e) {
		    e.printStackTrace();
		}
	}

	/**
	 * Check if a month have 30 days
	 * 
	 * @param month
	 * @return
	 */
	private static boolean has30(int month) {
		int[] m = new int[] {4,6,9,11};
		
		for (int i : m) {
			if (i == month) {
				return true;
			}
		}
		
		return false;
	}
	
	public static void main(String[] args) {
		generate(implementMethod, numberOfTransactions);
	}
}
