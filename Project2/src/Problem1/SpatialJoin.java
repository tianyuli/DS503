package Problem1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpatialJoin {
	public static class SpatialMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
        }	
    }
	public static class SpatialReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			
	    }
	}
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length == 7) {
            conf.set("Window",args[3]+","+args[4]+","+args[5]+","+args[6]);
        } else {
            conf.set("Window","null");
        }
        Job job = Job.getInstance(conf, "SpatialJoin");
        job.setJarByClass(SpatialJoin.class);
        job.setMapperClass(SpatialMapper.class);
        job.setReducerClass(SpatialReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
