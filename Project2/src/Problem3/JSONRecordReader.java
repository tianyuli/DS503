package Problem3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class JSONRecordReader extends RecordReader<Text, Text> {
	private Text key;
	private Text value;
	private FSDataInputStream fsIn;
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split = (FileSplit)inputsplit;
		Configuration conf = context.getConfiguration();
		Path file = split.getPath();
		fsIn = file.getFileSystem(conf).open(file);
		fsIn.seek(0);		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		return false;
	}

}
