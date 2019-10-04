package Problem1;

import java.io.IOException;
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
		int blockx = (w_x_2 - w_x_1)/10;
		int blocky = (w_y_2 - w_y_1)/10;
		
		int x_1 = Integer.valueOf(data[0]);
        int w = Integer.valueOf(data[3]);
        int y_1 = Integer.valueOf(data[1]);
        int h = Integer.valueOf(data[2]);
		
        //check each block if it in or partially in the rectangle
        for(int x = w_x_1; x < w_x_2; x += blockx) {
        	for(int y = w_y_1; y < w_y_2; y += blocky) {
        		Rectangle rB = new Rectangle(x, y, blocky, blockx);
        		Rectangle r = new Rectangle(x_1, y_1, h, w);
        		if(isIn(rB,r))
        			context.write(new Text(x+","+y+","+blockx+","+blocky), new Text("Rectangle:"+value));
        	}
        }
	}
	private boolean isIn(Rectangle rb, Rectangle r) {
		if(r.getX() + r.getW() < rb.getX())		return false;
		if(r.getX() > rb.getX() + rb.getW())	return false;
		if(r.getY() > rb.getY() + rb.getH())	return false;
		if(r.getY() + r.getH() < rb.getY())		return false;
		return true;
	}
}
