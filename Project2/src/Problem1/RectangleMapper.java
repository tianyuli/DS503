package Problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RectangleMapper extends Mapper<LongWritable, Text, Text, Text> {
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
		String[] data = value.toString().split(",");
		
		int x_1 = Integer.valueOf(data[0]);
        int x_2 = Integer.valueOf(data[0])+Integer.valueOf(data[3]);
        int y_1 = Integer.valueOf(data[1]);
        int y_2 = Integer.valueOf(data[1])+Integer.valueOf(data[2]);
		for (int i=(int)Math.floor(x_1/10);i<=(int)Math.floor(x_2/10);i++) {
            for (int j=(int)Math.floor(y_1/10);j<=(int)Math.floor(y_2/10);j++)
                if (x_1 >= w_x_1 && x_1 <= w_x_2 && y_1 >= w_y_1 && y_1 <= w_y_2) {
                    // down left rectangle inside the window
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                }else if (x_1 >= w_x_1 && x_1 <= w_x_2 && y_2 >= w_y_1 && y_2 <= w_y_2){
                    // up left rectangle inside the window
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                }else if (x_2 >= w_x_1 && x_2 <= w_x_2 && y_1 >= w_y_1 && y_1 <= w_y_2){
                    // down right rectangle inside the window
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                }else if (x_2 >= w_x_1 && x_2 <= w_x_2 && y_2 >= w_y_1 && y_2 <= w_y_2){
                    // down right rectangle inside the window
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                }else if (x_1 <= w_x_1 && x_2 >= w_x_2 && y_1 <= w_y_1 && y_2 >= w_y_2) {
                    // window inside the rectangle
                    context.write(new Text(Integer.toString(i)+"_"+Integer.toString(j)),new Text(value.toString()));
                }
        }
	}
}
