package hive;

public class ParseInput {
	private String SQL;
	private double Threshold = 0.05;
	private int Alpha = 5;
	
	public ParseInput(String sql) {
		int thre = sql.length() - 1;
		int alp = sql.length() - 1;
		if(sql.contains("Threshold")){
			thre = sql.indexOf("Threshold");
			
		}
		if(sql.contains("Alpha")) {
			alp = sql.indexOf("Alpha");
		}
		this.SQL = sql.substring(0, thre-1);
		if(SQL.charAt(SQL.length() - 1) == ';'){
			SQL = SQL.substring(0, SQL.length() - 1);
		}
		// if thre == sql.length() - 1 mean use default threshold and default alpha
		if(thre != sql.length() - 1) {
			this.Threshold = Double.valueOf(sql.substring(thre + 9, alp));
			// if alpha == sql.length() - 1 mean use default alpha
			if(alp != sql.length() - 1) {
				this.Alpha = (int) (double) Double.valueOf(sql.substring(alp + 5,sql.length()-2));
			}
		}		 
	}
	
	public double getThreshold() {
		return this.Threshold;
	}
	public int getAlpha() {
		return this.Alpha;
	}
	public String getSQL() {
		return this.SQL;
	}
}