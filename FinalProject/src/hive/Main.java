package hive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.stream.Stream;

public class Main {
	final static Method method = Method.BUCKET;
	final static String filePath = "input.txt";
	
	/**
	 * The main function
	 * 
	 * @param args			Arguments
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {		
		String input = readLineByLine(filePath);
		ParseInput ps = new ParseInput(input);
		
		System.out.println("Running query: " + ps.getSQL());
		System.out.println("With threshold: " + ps.getThreshold() + "%");
		System.out.println("And sample rate: " + ps.getAlpha() + "%");
		
		ProgressiveApproximate PA = new ProgressiveApproximate(ps.getSQL(), ps.getThreshold(), 
				ps.getAlpha(), method);
		PA.run();
	}
	
	/**
	 * Helper function to read the SQL query
	 * 
	 * @param filePath		Path of the SQL query file
	 * @return				The SQL query
	 */
	private static String readLineByLine(String filePath) {
	    StringBuilder contentBuilder = new StringBuilder();
	    
	    try (Stream<String> stream = Files.lines( Paths.get(filePath))){
	        stream.forEach(s -> contentBuilder.append(s).append("\n"));
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    
	    return contentBuilder.toString();
	}
}
