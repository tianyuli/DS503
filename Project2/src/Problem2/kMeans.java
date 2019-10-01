package Problem2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import Problem1.Point;

public class kMeans {	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String input = args[0];
		String output = args[1];
		float close;
        if (args.length == 4) {
        	close = Float.valueOf(args[3]);
        } else {
        	close = (float) 0.1;
        }
		int k = Integer.valueOf(args[2]);

		List<String> clusters= new ArrayList<String>();
		String file_name = "part-r-00000";
		//first round
		//get random k points
		for(int index = 0; index < k; index++){
			int x = (int) (Math.random() * 10000);
			int y = (int) (Math.random() * 10000);
			Point p = new Point(x, y);
			clusters.add(p.write());
		}


		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("K",Integer.toString(k));

		//loop
		int i = 0;
		while(i < 6) {
			for (int j=0;j<clusters.size();j++){
				conf.set("C"+Integer.toString(j),clusters.get(j));
			}
			Path temp_out = new Path(output+"output_"+Integer.toString(i)+"/");
			Job job = Job.getInstance(conf, "kMeans");

			job.setNumReduceTasks(1);
			job.setJarByClass(kMeans.class);

			job.setMapperClass(kMeansMapper.class);
			job.setCombinerClass(kMeansCombiner.class);
			job.setReducerClass(kMeansReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, temp_out);
			job.waitForCompletion(true);
			
			//List<String> newCluster = getCentroids(output+"output_"+Integer.toString(i)+"/"+file_name);
//			if(checkClose(newCluster, clusters, close)) break;
			i = 6;
			
		}
	}
	
	private static boolean checkClose(List<String> newCluster, List<String> cluster, float close) {
		for(int j=0;j<newCluster.size();j++){
            String[] centroids = newCluster.get(j).split(",");
            double old_x = Double.valueOf(centroids[0]);
            double old_y = Double.valueOf(centroids[1]);
            double new_x = Double.valueOf(centroids[2]);
            double new_y = Double.valueOf(centroids[3]);
            double diff_x = Math.abs(old_x-new_x);
            double diff_y = Math.abs(old_y-new_y);
            if (diff_x > close || diff_y > close)	return false;
        }
		return true;

	}
}
