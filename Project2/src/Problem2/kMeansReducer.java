package Problem2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class kMeansReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        int x = 0;
        int y = 0;
        int count = 0;
        for (Text str:value) {
            String[] data = str.toString().split(",");
            x += Integer.valueOf(data[0]);
            y += Integer.valueOf(data[1]);
            count += Integer.valueOf(data[2]);;
        }
        //key is the previous centroid, value if the new centroid
        context.write(key,new Text(String.valueOf(x/count)+","+String.valueOf(y/count)));
    }
}
