package src;

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
		return String.valueOf(x)+","+String.valueOf(y) + ","+String.valueOf(h)+","+String.valueOf(w)+"\n";  
	}
	public int getX() {
		return this.x;
	}
	public int getY() {
		return this.y;
	}
	public int getH() {
		return this.h;
	}
	public int getW() {
		return this.w;
	}
}
