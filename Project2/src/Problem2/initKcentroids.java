package Problem2;

import java.io.FileWriter;

import Problem1.Point;

public class initKcentroids {
	public static void main(String[] args) {
		try{
			FileWriter csvWriter = new FileWriter("kCntroids.csv");
			for(int index = 0; index < 100; index++){
				int x = (int) (Math.random() * 10000);
				int y = (int) (Math.random() * 10000);
				Point p = new Point(x, y);
				csvWriter.append(p.write());
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
	}
}
