package mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query5 {
	private static class TransactionMapper extends Mapper<Object, Text, Text, FloatWritable> {
		HashMap<Integer, String> cachedRows = new HashMap<Integer, String>();
		
		@Override
		protected void setup(Context context) throws IOException {
			URI[] cache = context.getCacheFiles();
			String line;
			
			if (cache != null && cache.length > 0) {
			    FileSystem fs = FileSystem.get(context.getConfiguration());
			    Path path = new Path(cache[0].toString());
			    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			    
			    while ((line = reader.readLine()) != null) {
			    	try {
			    		String row[] = line.split(",");
			    		String group = transAgeGenderToString(Integer.valueOf(row[2]), row[3]);
			    		cachedRows.put(Integer.valueOf(row[0]), group);
			    	} catch(Exception e) {
			    		System.out.println(e.toString());
			    	}
			    }
			}
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String row[] = value.toString().split(",");
			int custID = Integer.valueOf(row[1]);
			String group = cachedRows.get(custID);
			float transTotal = Float.valueOf(row[2]);
			
			context.write(new Text(group), new FloatWritable(transTotal));
		}
	}
	
	private static class AggregateReducer extends Reducer<Text, FloatWritable, Text, Text>{
	    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
	    	int numberOfCustomers = 0;
	        float minTransTotal = 1000;
	        float maxTransTotal = 10;
	        float totalTransTotal = 0;
	                                               
	        for (FloatWritable v : values) {
	        	numberOfCustomers++;
	        	totalTransTotal += v.get();
	        	
	        	if (v.get() < minTransTotal) {
	        		minTransTotal = v.get();
	        	}	
	        	
	        	if (v.get() > maxTransTotal) {
	        		maxTransTotal = v.get();
	        	}
	        }
	        
	        float avgTransTotal = totalTransTotal / numberOfCustomers;
	        
	        String ageGroup = transtringToAge(key.toString());
	        String gender = key.toString().substring(1);
	        context.write(new Text(ageGroup + ", " + gender), new Text(numberOfCustomers + ", " + minTransTotal + ", " + maxTransTotal +
	        		", " + avgTransTotal));
	    }
	}
	
	private static String transAgeGenderToString(int age, String gender) {
		String ageGroup = null;
		
		if (age >= 10 && age < 20) {
			ageGroup = "1";
		}
		else if (age >= 20 && age < 30) {
			ageGroup = "2";
		}
		else if (age >= 30 && age < 40) {
			ageGroup = "3";
		}
		else if (age >= 40 && age < 50) {
			ageGroup = "4";
		}
		else if (age >= 50 && age < 60) {
			ageGroup = "5";
		}
		else if (age >= 60 && age <= 70) {
			ageGroup = "6";
		}
		
		String result = ageGroup + gender;
		return result;
	}
	
	private static String transtringToAge(String input) {
		String result = null;
		
		if (input.charAt(0) == '1') {
			result = "10s";
		}
		else if (input.charAt(0) == '2') {
			result = "20s";
		}
		else if (input.charAt(0) == '3') {
			result = "30s";
		}
		else if (input.charAt(0) == '4') {
			result = "40s";
		}
		else if (input.charAt(0) == '5') {
			result = "50s";
		}
		else if (input.charAt(0) == '6') {
			result = "60s";
		}
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();		
		job.setJarByClass(Query5.class);
		job.setJobName("Query 5");
		
		Path cachePath = new Path("input/Customers.csv");
		job.addCacheFile(cachePath.toUri());
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(TransactionMapper.class);
		job.setReducerClass(AggregateReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
