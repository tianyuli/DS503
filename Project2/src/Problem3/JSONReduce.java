package Problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JSONReduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
		int min = Integer.MAX_VALUE;
		int max = 0;
		for(IntWritable v: value) {
			min = Math.min(min, v.get());
			max = Math.max(max, v.get());
		}
		context.write(key, new Text(min + " "+ max));
	}
}
