package Problem4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author Tianyu Li
 * 
 * Assumptions: 
 * (1)The data is distributed into 100 blocks (1000 * 1000 each).
 * (2)The radius is smaller than 1000(the side length of each block). Or the use of MapReduce might be inefficient.
 * (3)The number of neighbors is smaller than 100000.
 */
public class OutlierDetection {
	private static int radius;
	private static int neighbors;
	
	private static class PointsMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String row[] = value.toString().split(",");
			int x = Integer.valueOf(row[0].trim());
			int y = Integer.valueOf(row[1].trim());
			
			// Distribute the point into blocks based on there location.
			int blockX = (x - 1) / 1000;
			int blockY = (y - 1) / 1000;
			
			context.write(new Text(blockX + ", " + blockY), new Text("inside" + ", " + x + ", " + y));
			
			// Check if this point may be used to detect outliers for points from other blocks.
			
			int leftBound = blockX * 1000;
			int rightBound = leftBound + 1000;
			int bottomBound = blockY * 1000;
			int topBound = bottomBound + 1000;
			
			if (blockX > 0 && x - leftBound <= radius) {
				
				// Left side
				context.write(new Text((blockX - 1) + ", " + blockY), new Text("outside" + ", " + x + ", " + y));
				
				// Bottomleft corner
				if (blockY > 0 & y - bottomBound <= radius) {
					context.write(new Text((blockX - 1) + ", " + (blockY - 1)), new Text("outside" + ", " + x + ", " + y));
				}
				
				// Topleft corner
				if (blockY < 9 & topBound - y <= radius) {
					context.write(new Text((blockX - 1) + ", " + (blockY + 1)), new Text("outside" + ", " + x + ", " + y));
				}
			}
			
			if (blockX < 9 && rightBound - x <= radius) {
				
				// Right side
				context.write(new Text((blockX + 1) + ", " + blockY), new Text("outside" + ", " + x + ", " + y));
				
				// Bottomright corner
				if (blockY > 0 & y - bottomBound <= radius) {
					context.write(new Text((blockX + 1) + ", " + (blockY - 1)), new Text("outside" + ", " + x + ", " + y));
				}
				
				// Topright corner
				if (blockY < 9 & topBound - y <= radius) {
					context.write(new Text((blockX + 1) + ", " + (blockY + 1)), new Text("outside" + ", " + x + ", " + y));
				}
			}
			
			// Bottom side
			if (blockY > 0 & y - bottomBound <= radius) {
				context.write(new Text(blockX + ", " + (blockY - 1)), new Text("outside" + ", " + x + ", " + y));
			}
			
			// Top side
			if (blockY < 9 & topBound - y <= radius) {
				context.write(new Text(blockX + ", " + (blockY + 1)), new Text("outside" + ", " + x + ", " + y));
			}
		}
	}
	
	private static class DetectReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> inPoints= new ArrayList<String>();
			List<String> outPoints= new ArrayList<String>();
			
			for (Text v: values) {
				String row[] = v.toString().split(",");
				if (row[0].equals("inside")) {
					inPoints.add(row[1].trim() + ", " + row[2].trim());
					outPoints.add(row[1].trim() + ", " + row[2].trim());
				}
				
				if (row[0].equals("outside")) {
					outPoints.add(row[1].trim() + ", " + row[2].trim());
				}
			}
			
			for (String in: inPoints) {
				int count = 0;
				
				for (String out: outPoints) {
					if (inRadius(in, out)) {
						count++;
					}
				}
				
				if (count > neighbors) {
					context.write(new Text(in), new Text("outlier"));
				}
			}
		}
	}
	
	private static boolean inRadius(String point1, String point2) {
		String p1[] = point1.split(",");
		String p2[] = point2.split(",");
		
		int x1 = Integer.valueOf(p1[0].trim());
		int y1 = Integer.valueOf(p1[1].trim());
		int x2 = Integer.valueOf(p2[0].trim());
		int y2 = Integer.valueOf(p2[1].trim());
		
		double distance = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
		
		if (distance <= radius) {
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new RuntimeException("Two arguments required: radius and number of neighbors");
		}
		
		radius = Integer.valueOf(args[0]);
		neighbors = Integer.valueOf(args[1]);
		
		Job job = Job.getInstance();		
		job.setJarByClass(OutlierDetection.class);
		job.setJobName("Outlier Detection");
		
		Path inputPath = new Path("input/Rectangle.csv");
		Path outputPath = new Path("output");		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(PointsMapper.class);
		job.setReducerClass(DetectReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
