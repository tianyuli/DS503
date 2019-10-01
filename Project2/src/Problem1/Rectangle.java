package Problem1;

public class Rectangle {
	private int x;
	private int y;
	private int h;
	private int w;
	
	public Rectangle(int x, int y, int h, int w) {
		this.x = x;
		this.y = y;
		this.h = h;
		this.w = w;				
	}
	public String write() {
		return String.valueOf(x)+", "+String.valueOf(y) + ", "+String.valueOf(h)+", "+String.valueOf(w);  
	}
}
