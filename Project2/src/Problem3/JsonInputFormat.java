package Problem3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class JsonInputFormat extends TextInputFormat{

	public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, 
			Reporter reporter) throws IOException {
		JsonRecordReader reader = new JsonRecordReader((FileSplit) inputSplit, jobConf);
		return reader;
	}
}
