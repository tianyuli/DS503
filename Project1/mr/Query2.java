package mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2 {
	private static class CustomerMapper extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			String row[] = value.toString().split(",");
			int ID = Integer.valueOf(row[0]);
			
			context.write(new IntWritable(ID), new Text("Customer:" + row[1]));
		}
	}
	
	private static class TransactionMapper extends Mapper<Object, Text, IntWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			String row[] = value.toString().split(",");
			int custID = Integer.valueOf(row[1]);
			
			context.write(new IntWritable(custID), new Text("Transaction:" + row[2]));
		}
	}
	
	private static class TransactionCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int numTransactions = 0;
			float totalSum = 0;
			boolean hasTrans = false;
			
			for (Text v: values) {
				String[] splitted = v.toString().split(":", 2);
				
				if (splitted[0].equals("Customer")) {
					context.write(key, v);
				}
				
				else if (splitted[0].equals("Transaction")) {
					hasTrans = true;
					numTransactions++;
					totalSum += Float.valueOf(splitted[1]);
				}
				
				else throw new InterruptedException("Invalid Prefix");
			}
			
			if (hasTrans) {
				context.write(key, new Text("Combined:" + numTransactions + "," + totalSum));
			}
		}
	}
	
	private static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String customerName = null;
			int numTransactions = 0;
			float totalSum = 0;
			
			for (Text v: values) {
				String[] splitted = v.toString().split(":", 2);
				
				if (splitted[0].equals("Customer")) {
					customerName = splitted[1];
				}
				
				else if (splitted[0].equals("Combined")) {
					String[] innerSplitted = splitted[1].split(",", 2);
					numTransactions += Integer.valueOf(innerSplitted[0]);;
					totalSum += Float.valueOf(innerSplitted[1]);
				}
				
				else throw new InterruptedException("Invalid Prefix");
			}
			
			context.write(key, new Text(customerName + ", " + numTransactions + ", " + totalSum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();		
		job.setJarByClass(Query2.class);
		job.setJobName("Query 2");

		Path customerInputPath = new Path(args[0]);
		Path transactionInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);		
		MultipleInputs.addInputPath(job, customerInputPath, TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, transactionInputPath, TextInputFormat.class, TransactionMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setCombinerClass(TransactionCombiner.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
