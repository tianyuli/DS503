package Problem3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JsonProcess {
	private static class JsonMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String row[] = value.toString().split(",");
			Long flag = Long.valueOf(row[5]);
			Long elevation = Long.valueOf(row[8]);
			
			context.write(new LongWritable(flag), new LongWritable(elevation));
		}
	}
	
	private static class MaxMinReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long min = Long.MIN_VALUE;
			long max = Long.MAX_VALUE;
			
			for (LongWritable v: values) {
				if (v.get() > max) {
					max = v.get();
				}
				
				if (v.get() < min) {
					min = v.get();
				}
			}
			
			context.write(key, new Text("max value: " + max + ", min value: " + min));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();		
		job.setJarByClass(JsonProcess.class);
		job.setJobName("Json Process");
		
		Path inputPath = new Path("input/airfield.json");
		Path outputPath = new Path("output");		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(JsonMapper.class);
		job.setReducerClass(MaxMinReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);		
	}
}
