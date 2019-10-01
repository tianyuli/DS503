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


        List<Double> ans = new ArrayList<Double>();
        ans.add(Double.POSITIVE_INFINITY);//Distance
        ans.add((double)0);//index

        // read all the centroids from the setting
        // and compare to get the close one
        for (int i=0;i<K;i++){
            String[] temp = conf.get("C"+Integer.toString(i)).split(",");
            int temp_x = Integer.valueOf(temp[0]);
            int temp_y = Integer.valueOf(temp[1]);
            double dis = Math.sqrt(Math.pow(cur_x - temp_x,2)+Math.pow(cur_y - temp_y,2));

            if (dis<ans.get(0)) {
                ans.set(0,dis);
                ans.set(1,(double)i);
            }
        }

        context.write(new Text(conf.get("C"+Integer.toString(ans.get(1).intValue()))),new Text(value));

	}
}
