package Problem1;

public class Point {
	private float x;
	private float y;
	
	public Point(float x, float y) {
		this.x = x;
		this.y = y;
	}
	
	public String write() {
		return String.valueOf(x)+", "+String.valueOf(y);  
	}
}
