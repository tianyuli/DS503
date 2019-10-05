package Problem2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class kMeans {	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String input = args[0];
		String kfile = args[1];
		String output = args[2];
		int k = 100;
		if(args.length == 4) {
			k = Integer.valueOf(args[3]);
		}
		conf.set("K",Integer.toString(k));

		List<String> clusters= new ArrayList<String>();
		//get random k centroid
		File kCentroid = new File(kfile);
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(kCentroid));
		for(int i = 0; i < k; i++) {
			clusters.add(br.readLine());
		}

		//loop
		int i = 0;
		while(i < 6) {
//			System.out.println("--------------------------------The "+i + "th turn----------------------------");
			//add all centroid to the config
			for (int j = 0;j < clusters.size();j++){
//				System.out.print("Adding Centroids: " + clusters.get(j));
				conf.set("C"+Integer.toString(j),clusters.get(j));
			}
			String temp_out = output+Integer.toString(i);

			Job job = Job.getInstance(conf, "kMeans");
			job.setNumReduceTasks(1);					
			job.setJarByClass(kMeans.class);
			job.setJarByClass(kMeans.class);
			job.setMapperClass(kMeansMapper.class);
			job.setCombinerClass(kMeansCombiner.class);
			job.setReducerClass(kMeansReducer.class);

			FileInputFormat.addInputPath(job, new Path(input));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(temp_out));
			job.waitForCompletion(true);
			clusters.clear();
			clusters = checkClose(temp_out+"/part-r-00000", 1);
			if(clusters.size() != k) break;
			i++;

		}
	}

	@SuppressWarnings("resource")
	private static List<String> checkClose(String temp_out, int close) throws NumberFormatException, IOException {
//		System.out.println(temp_out);
		File result = new File(temp_out);
		BufferedReader br;
		List<String> newCen = new ArrayList<String>();
		br = new BufferedReader(new FileReader(result));
		String line;			
		boolean isclose = true;
		while( (line = br.readLine()) != null) {
			String[] centroids = line.split("\t");
			int old_x = Integer.valueOf(centroids[0].split(",")[0]);
			int old_y = Integer.valueOf(centroids[0].split(",")[1]);
			int new_x = Integer.valueOf(centroids[1].split(",")[0]);
			int	new_y = Integer.valueOf(centroids[1].split(",")[1]);
			if (Math.abs(new_y - old_y) > close || Math.abs(new_x - old_x) > close)	 isclose = false;
			newCen.add(centroids[1]);
		}
		if(isclose) {
			newCen.clear();
		}
//		System.out.println(newCen.size());
		return newCen;	
	}
}
