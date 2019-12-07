package hive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class ProgressiveApproximate {
	public static void main(String[] args) {
//        Path inputFile = new Path(args[0]);
		
		String input = readLineByLine("input.txt");
		ParseInput ps = new ParseInput(input);
		System.out.println(ps.getSQL());
		System.out.println(ps.getThreshold());
		System.out.println(ps.getAlpha());
		
	}
	private static String readLineByLine(String filePath) 
	{
	    StringBuilder contentBuilder = new StringBuilder();
	    try (Stream<String> stream = Files.lines( Paths.get(filePath))){
	        stream.forEach(s -> contentBuilder.append(s).append("\n"));
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	    }
	    return contentBuilder.toString();
	}
}
