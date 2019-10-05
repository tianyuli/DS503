package DataGeneration;

public class Point {
	private int x;
	private int y;
	
	public Point(int x, int y) {
		this.x = x;
		this.y = y;
	}
	
	public String write() {
		return String.valueOf(x)+","+String.valueOf(y)+"\n";  
	}
}
