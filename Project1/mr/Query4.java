package mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4 {
	private static class TransactionMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
		HashMap<Integer, Integer> cachedRows = new HashMap<Integer, Integer>();
		
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
			    		cachedRows.put(Integer.valueOf(row[0]), Integer.valueOf(row[4]));
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
			int countryCode = Integer.valueOf(cachedRows.get(custID).toString());
			float transTotal = Float.valueOf(row[2]);
			
			context.write(new IntWritable(countryCode), new FloatWritable(transTotal));
		}
	}
	
	private static class AggregateReducer extends Reducer<IntWritable, FloatWritable, IntWritable, Text>{
	    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
	    	int numberOfCustomers = 0;
	        float minTransTotal = 1000;
	        float maxTransTotal = 10;
	                                               
	        for (FloatWritable v : values) {
	        	numberOfCustomers++;
	        	
	        	if (v.get() < minTransTotal) {
	        		minTransTotal = v.get();
	        	}	
	        	
	        	if (v.get() > maxTransTotal) {
	        		maxTransTotal = v.get();
	        	}
	        }

	        context.write(key, new Text(numberOfCustomers + ", " + minTransTotal + ", " + maxTransTotal));
	    }
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();		
		job.setJarByClass(Query4.class);
		job.setJobName("Query 4");
		
		Path cachePath = new Path("input/Customers.csv");
		job.addCacheFile(cachePath.toUri());
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(TransactionMapper.class);
		job.setReducerClass(AggregateReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
