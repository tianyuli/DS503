
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Main {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args){
		// Generate 5M lines of Transactions
		try{
			FileWriter csvWriter = new FileWriter("Transactions.csv");
			int custID = (int)(Math.random() * 50000 + 1);
			int times = (int)(Math.random() * 30 + 1);
			for(int index = 1; index <= 5000000; index++){
				if (times > 0) times --;
				else {
					custID = (int)(Math.random() * 50000 + 1);
					times = (int)(Math.random() * 30 + 1);
				}				
				
				float transTotal = (float) (Math.random() * (1000 - 10) + 10);
				int transNumItems = (int)(Math.random() * 10 + 1);
				String transDesc = getRandomStr((int)(Math.random() * 5 + 20));
				
				Transaction t = new Transaction(index, custID, transTotal, transNumItems, transDesc);
				csvWriter.append(t.write());
			}
			csvWriter.flush();
			csvWriter.close();
			
			csvWriter = new FileWriter("Points.csv");
			
			for(int index = 1; index <= 11000000; index++){
				int x = (int) (Math.random() * 10000 + 1);
				int y = (int) (Math.random() * 10000 + 1);
				Point p = new Point(x, y);
				csvWriter.append(p.write());
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
