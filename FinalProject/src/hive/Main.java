package hive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Main {
	public static void main(String[] args) {
//      Path inputFile = new Path(args[0]);
		
		String input = readLineByLine("input.txt");
		ParseInput ps = new ParseInput(input);
		ProgressiveApproximate PA = new ProgressiveApproximate(ps.getSQL(), ps.getThreshold(), ps.getAlpha());
		PA.run();
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
