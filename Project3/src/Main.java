
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args){
		// genterate 50,000 lines of Customers
		try{
			FileWriter csvWriter = new FileWriter("Customers.csv");
		
			for(int index = 1; index <= 50000; index++){
				String name = getRandomStr((int)(Math.random() * 10 + 10));
				int age = (int)(Math.random() * 60 + 10);
				String gender = (Math.random() * 2) > 1 ? "male" : "female";
				int countrycode = (int)(Math.random() * 10 + 1);
				float salary = (float) (Math.random() * (10000 - 100) + 100);
				
				Customer c = new Customer(index, name, age, gender, countrycode, salary);
				csvWriter.append(c.write());
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
		// Generate 5M lines of Transactions
		try{
			FileWriter csvWriter = new FileWriter("Transactions.csv");
		
			for(int index = 1; index <= 5000000; index++){
				int custID = (int)(Math.random() * 50000 + 1);
				float transTotal = (float) (Math.random() * (1000 - 10) + 10);
				int transNumItems = (int)(Math.random() * 10 + 1);
				String transDesc = getRandomStr((int)(Math.random() * 30 + 20));
				
				Transaction t = new Transaction(index, custID, transTotal, transNumItems, transDesc);
				csvWriter.append(t.write());
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
		
	}
	
	
	private static String getRandomStr(int len){
		String base = "abcdefghijklmnopqrstuvwxyz0123456789";
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < len; i++) {
			int number = random.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();   
	}

}
