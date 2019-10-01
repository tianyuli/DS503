package Problem2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class kMeansCombiner extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        double mean_x = 0;
        double mean_y = 0;
        double count = 0;
        for (Text str:value) {
            String[] data = str.toString().split(",");
            mean_x += Double.valueOf(data[0]);
            mean_y += Double.valueOf(data[1]);
            count += 1;
        }
        context.write(key, new Text(Double.toString(mean_x)+","+Double.toString(mean_y)+","+Double.toString(count)));
    }
}