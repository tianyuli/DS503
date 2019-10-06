package Problem3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JSONMapper extends Mapper<Text, Text, Text, IntWritable> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		IntWritable elevation = new IntWritable(Integer.valueOf(line[8]));
		context.write(new Text(line[5]), elevation);		
	}
}
