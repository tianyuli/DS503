package Problem3;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class JsonRecordReader extends RecordReader<LongWritable, Text> {
	private final byte[] start;
	private final byte[] end;
	private final long startLocation;
	private final long endLocation;

	private final FSDataInputStream inputStream;
	private final DataOutputBuffer buffer;
	private LongWritable key;
	private Text value;

	public JsonRecordReader(FileSplit split, JobConf jobConf) throws IOException {
		start = "{".getBytes();
		end = "},".getBytes();
		startLocation = split.getStart();
		endLocation = startLocation + split.getLength();

		buffer = new DataOutputBuffer();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(jobConf);
		inputStream = fs.open(path);
		inputStream.seek(startLocation);
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (inputStream.getPos() - startLocation) / (float) (endLocation - startLocation);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (inputStream.getPos() < endLocation) {
			if (readUntilMatch(start, false)) {
				try {
					buffer.write(start);
					if (readUntilMatch(end, true)) {
						key = new LongWritable(inputStream.getPos());
						String pairs = new String(buffer.getData());
						String data = "";
						
						for (String s: pairs.split(",")) {
							data.concat(s.split(":")[1]);
						}
						
						value = new Text(data);
						return true;
					}
				} finally {
					buffer.reset();
				}
			}
		}
		return false;
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = inputStream.read();
			
			if (b == -1)
				return false;
			
			if (withinBlock)
				buffer.write(b);
			
			if (b == match[i]) {
				i++;
				if (i >= match.length)
					return true;
			} else
				i = 0;
			
			if (!withinBlock && i == 0 && inputStream.getPos() >= endLocation)
				return false;
		}
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {

	}
}
