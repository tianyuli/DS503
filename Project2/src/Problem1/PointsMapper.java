package Problem1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int block = Integer.valueOf(conf.get("Block"));
			String ws = conf.get("Window");
	        int w_x_1 = 0;
	        int w_x_2 = 10000;
	        int w_y_1 = 0;
	        int w_y_2 = 10000;
	        if (!(ws.equals("null"))){
	            String[] windows = ws.split(",");
	            w_x_1 = Integer.valueOf(windows[0]);
	            w_x_2 = Integer.valueOf(windows[2]);
	            w_y_1 = Integer.valueOf(windows[1]);
	            w_y_2 = Integer.valueOf(windows[3]);
	        }
			String[] point = value.toString().split(",");
			if(Integer.valueOf(point[0]) >= w_x_1 && Integer.valueOf(point[0]) <= w_x_2) {
				if(Integer.valueOf(point[1]) >= w_y_1 && Integer.valueOf(point[1]) <= w_y_2) {
					int id = (Integer.valueOf(point[0]) - w_x_1) / ((w_x_2 - w_x_1)/block);
//					System.out.println("Key: "+id+" ****** "+value);
					context.write(new Text(id+""), new Text("Point:" + value));
				}
			}
			
		}
}