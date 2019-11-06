package src;
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
		
			for(int index = 1; index <= 11000000; index++){
				int x = (int) (Math.random() * 10000 + 1);
				int y = (int) (Math.random() * 10000 + 1);
				Point p = new Point(x, y);
				csvWriter.append(p.write());
			}
			csvWriter.flush();
			csvWriter.close();
		}catch (Exception e) {
		    e.printStackTrace();
		}
		// Generate Rectangles
		/*
		 * try{ FileWriter csvWriter = new FileWriter("Rectangle.csv");
		 * 
		 * for(int index = 1; index <= 70000000; ){ int x = (int) (Math.random() * 10000
		 * + 1); int y = (int) (Math.random() * 10000 + 1); int h = (int) (Math.random()
		 * * 10 + 1); int w = (int) (Math.random() * 10 + 1); if((x+w) > 10000 || (y+h)
		 * > 10000) continue; Rectangle r = new Rectangle(x,y,h,w);
		 * csvWriter.append(r.write()); index++; } csvWriter.flush(); csvWriter.close();
		 * }catch (Exception e) { e.printStackTrace(); } try{ FileWriter csvWriter = new
		 * FileWriter("kCentroids.csv"); for(int index = 0; index < 100; index++){ int x
		 * = (int) (Math.random() * 10000); int y = (int) (Math.random() * 10000); Point
		 * p = new Point(x, y); csvWriter.append(p.write()); } csvWriter.flush();
		 * csvWriter.close(); }catch (Exception e) { e.printStackTrace(); }
		 */
		
	}
}
