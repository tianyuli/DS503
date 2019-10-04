package Problem1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<List<Integer>> points = new ArrayList<>();
		List<List<Integer>> rectangles  = new ArrayList<>();
	
		for(Text v: values) {
			String[] splitted = v.toString().split(":", 2);
			if("Point".equals(splitted[0])) {
				ArrayList<Integer> point = new ArrayList<Integer>();
				String[] data = splitted[1].split(",");
				point.add(Integer.valueOf(data[0]));//x0
				point.add(Integer.valueOf(data[1]));//y0
				points.add(point);				
			}else {
				ArrayList<Integer> rect = new ArrayList<Integer>();
				String[] data = splitted[1].split(",");
				rect.add(Integer.valueOf(data[0]));//x0
				rect.add(Integer.valueOf(data[1]));//y0
				rect.add(Integer.valueOf(data[2]));//h
				rect.add(Integer.valueOf(data[3]));//w
				rectangles.add(rect);				
			}
		}
		
		for(List<Integer> p: points) {
			for(List<Integer> r: rectangles) {
				if(p.get(0) >= r.get(0) && p.get(0) <= r.get(0)+r.get(3)) {//x0<=x<=x0+w
					if(p.get(1) >= r.get(1) && p.get(1) <= r.get(2)+r.get(1)) {//y0<=y<=y0+h
						StringBuffer k = new StringBuffer();
						for(int i: r) {
							k.append(i+", ");
						}
						context.write(new Text(k.toString()), new Text("("+p.get(0)+","+p.get(1)+")"));
					}
				
				}
			}
		}
	}	
}
