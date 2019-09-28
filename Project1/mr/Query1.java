package mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {
	private static class FilterMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			String row[] = value.toString().split(",");
			int age = Integer.valueOf(row[2].toString());
			
			if (age >= 20 && age <= 50) {
				context.write(value, new Text(""));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();		
		job.setJarByClass(Query1.class);
		job.setJobName("Query 1");

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(FilterMapper.class);
		job.setOutputKeyClass(Text.class);
	    
	    boolean status = job.waitForCompletion(true);
		System.exit(status ? 0 : 1);
	}
}
