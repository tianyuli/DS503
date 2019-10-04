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
	        int blockx = (w_x_2 - w_x_1) / 10;
	        int blocky = (w_y_2 - w_y_1) / 10;
			String[] point = value.toString().split(",");
			if(Integer.valueOf(point[0]) >= w_x_1 && Integer.valueOf(point[0]) <= w_x_2) {
				if(Integer.valueOf(point[1]) >= w_y_1 && Integer.valueOf(point[1]) <= w_y_2) {
					int x_0 = (Integer.valueOf(point[0]) - w_x_1) / blockx * blockx + w_x_1;
					int y_0 = (Integer.valueOf(point[1]) - w_y_1) / blocky * blocky + w_y_1;
					context.write(new Text(x_0+","+y_0+","+blockx+","+blocky), 
							new Text("Point:" + value));
				}
			}
			
		}
}