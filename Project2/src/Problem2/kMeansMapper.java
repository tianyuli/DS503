package Problem2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class kMeansMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Integer K = Integer.valueOf(conf.get("K"));
        
        String[] data = value.toString().split(",");
        int cur_x = Integer.valueOf(data[0]);
        int cur_y = Integer.valueOf(data[1]);
        
        List<Double> closest = new ArrayList<Double>();
        closest.add(Double.POSITIVE_INFINITY);//Distance
        closest.add((double)0);//index

        // read all the centroids from the setting
        // and compare to the closest one
        for (int i = 0;i < K;i++){
        	//get the ith centroid
            String[] temp = conf.get("C" + Integer.toString(i)).split(",");
            int x = Integer.valueOf(temp[0]);
            int y = Integer.valueOf(temp[1]);
            double dis = Math.sqrt(Math.pow(cur_x - x,2)+Math.pow(cur_y - y,2));

            if (dis < closest.get(0)) {
            	closest.set(0,dis);
            	closest.set(1,(double)i);//record the index of closest centroid
            }
        }
        //key is the centroid
        context.write(new Text(conf.get("C"+Integer.toString(closest.get(1).intValue()))),new Text(value));
	}
}
