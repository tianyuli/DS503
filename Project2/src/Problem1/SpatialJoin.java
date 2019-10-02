package Problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpatialJoin {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length == 7) {
            conf.set("Window",args[3]+","+args[4]+","+args[5]+","+args[6]);
        } else {
            conf.set("Window","null");
        }
		Job job = Job.getInstance(conf, "SpatialJoin");		
		job.setJarByClass(SpatialJoin.class);
        
        Path pointsInputPath = new Path(args[0]);
		Path rectangleInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		
		MultipleInputs.addInputPath(job, pointsInputPath, TextInputFormat.class, PointsMapper.class);
		MultipleInputs.addInputPath(job, rectangleInputPath, TextInputFormat.class, RectangleMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
