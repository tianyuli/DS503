package Problem1;
import java.io.FileWriter;
import java.io.IOException;

public class Generator {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args){
		// Generate Points
		try{
			FileWriter csvWriter = new FileWriter("Points.csv");
		
			for(int index = 1; index <= 6000000; index++){
				float x = (float) (Math.random() * 10000);
				float y = (float) (Math.random() * 10000);
				Point p = new Point(x, y);
				csvWriter.append(p.write());
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
		// Generate Rectangles
		try{
			FileWriter csvWriter = new FileWriter("Rectangle.csv");
		
			for(int index = 1; index <= 3000000; ){
				float x = (float) (Math.random() * 10000);
				float y = (float) (Math.random() * 10000);
				float h = (float) (Math.random() * 10000);
				float w = (float) (Math.random() * 10000);
				if((x+w) > 10000 || (y+h) > 10000)
					continue;
				Rectangle r = new Rectangle(x,y,h,w);
				csvWriter.append(r.write());
				index++;
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
		
	}
}
