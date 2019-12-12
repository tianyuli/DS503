package hive;

public class ParseInput {
	private String SQL;
	private double threshold = 5;
	private int alpha = 5;
	
	/**
	 * Parse the input SQL query into hive SQL query, threshold and sample rate.
	 * 
	 * @param sql		The input SQL query
	 */
	public ParseInput(String sql) {
		int thresholdIndex = sql.length() - 1;
		int alphaIndex = sql.length() - 1;
		if(sql.contains("threshold")){
			thresholdIndex = sql.indexOf("threshold");
			
		}
		if(sql.contains("alpha")) {
			alphaIndex = sql.indexOf("alpha");
		}
		this.SQL = sql.substring(0, thresholdIndex-1);
		if(SQL.charAt(SQL.length() - 1) == ';'){
			SQL = SQL.substring(0, SQL.length() - 1);
		}
		// if thresholdIndex == sql.length() - 1 mean use default threshold and default alpha
		if(thresholdIndex != sql.length() - 1) {
			this.threshold = Double.valueOf(sql.substring(thresholdIndex + 9, alphaIndex));
			// if alpha == sql.length() - 1 mean use default alpha
			if(alphaIndex != sql.length() - 1) {
				this.alpha = (int) (double) Double.valueOf(sql.substring(alphaIndex + 5,sql.length()-2));
			}
		}		 
	}
	
	public double getThreshold() {
		return this.threshold;
	}
	public int getAlpha() {
		return this.alpha;
	}
	public String getSQL() {
		return this.SQL;
	}
}
