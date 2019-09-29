package Problem1;

public class Rectangle {
	private float x;
	private float y;
	private float h;
	private float w;
	
	public Rectangle(float x, float y, float h, float w) {
		this.x = x;
		this.y = y;
		this.h = h;
		this.w = w;				
	}
	public String write() {
		return String.valueOf(x)+", "+String.valueOf(y) + ", "+String.valueOf(h)+", "+String.valueOf(w);  
	}
}
