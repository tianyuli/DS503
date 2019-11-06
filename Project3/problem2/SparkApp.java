package problem2;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class SparkApp {
	final static String inputPath = "hdfs://localhost:9000/user/tl/input/Points.csv";
	final static String outputPath = "hdfs://localhost:9000/user/tl/output";
	
	private static class GroupPoints implements PairFunction<String, Integer, Integer>{
		/**
		 * 
		 */
		private static final long serialVersionUID = -6982118909862136716L;

		@Override
		public Tuple2<Integer, Integer> call(String points) throws Exception {
			int x = Integer.valueOf(points.split(",")[0]);
			int y = Integer.valueOf(points.split(",")[1]);
			
			int row = (int) Math.ceil(x / 20);
			int column = (int) Math.ceil(y / 20);
			
			Integer cellID = row + 500 * column;
			return new Tuple2<Integer, Integer>(cellID, 1);
		}
	}
	
	private static class Sum implements Function2<Integer, Integer, Integer>{

		/**
		 * 
		 */
		private static final long serialVersionUID = -356258263181599219L;

		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	}
	
	private static class ReGroup implements PairFlatMapFunction<Tuple2<Integer, Integer>, Integer, String>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 5959903167399375416L;

		@Override
		public Iterator<Tuple2<Integer, String>> call(Tuple2<Integer, Integer> input) throws Exception {
			List<Tuple2<Integer, String>> output = new ArrayList<Tuple2<Integer, String>>();
			
			// Self
			output.add(new Tuple2<Integer, String>(input._1, new String(input._1 + ":" + input._2)));
			
			// Left
			if (input._1 % 500 != 1) {
				output.add(new Tuple2<Integer, String>(input._1 - 1, new String(input._1 + ":" + input._2)));
			}
			
			// Right
			if (input._1 % 500 != 0) {
				output.add(new Tuple2<Integer, String>(input._1 + 1, new String(input._1 + ":" + input._2)));
			}
			
			// Top
			if (input._1 > 500) {
				output.add(new Tuple2<Integer, String>(input._1 - 500, new String(input._1 + ":" + input._2)));
				
				if (input._1 % 500 != 1) {
					output.add(new Tuple2<Integer, String>(input._1 - 501, new String(input._1 + ":" + input._2)));
				}
				
				if (input._1 % 500 != 0) {
					output.add(new Tuple2<Integer, String>(input._1 - 499, new String(input._1 + ":" + input._2)));
				}
			}
			
			// Bottom
			if (input._1 <= 249500) {
				output.add(new Tuple2<Integer, String>(input._1 + 500, new String(input._1 + ":" + input._2)));
				
				if (input._1 % 500 != 1) {
					output.add(new Tuple2<Integer, String>(input._1 + 499, new String(input._1 + ":" + input._2)));
				}
				
				if (input._1 % 500 != 0) {
					output.add(new Tuple2<Integer, String>(input._1 + 501, new String(input._1 + ":" + input._2)));
				}
			}
			
			return output.iterator();
		}
	}
	
	private static class CalculateRDI implements PairFunction<Tuple2<Integer, Iterable<String>>, Integer, Double> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 4708534303470489214L;

		@Override
		public Tuple2<Integer, Double> call(Tuple2<Integer, Iterable<String>> input) throws Exception {
			int self = 0;
			int count = 0;
			int sum = 0;
			
			for (String v: input._2) {
				int cell = Integer.valueOf(v.split(":")[0]);
				int cellCount = Integer.valueOf(v.split(":")[1]);
				
				if (cell == input._1) {
					self = cellCount;
				}
				
				else {
					count += 1;
					sum += cellCount;
				}
			}
			
			double RDI = 1;
			
			if (count != 0) {
				RDI = (double)self / ((double)sum / (double)count);
			}
			
			return new Tuple2<Integer, Double>(input._1, RDI);
		}
	}
	
	private static class SwitchKeyValue implements PairFunction<Tuple2<Integer, Double> , Double, Integer> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1139567795117947214L;

		@Override
		public Tuple2<Double, Integer> call(Tuple2<Integer, Double> input) throws Exception {
			return new Tuple2<Double, Integer>(input._2, input._1);
		}
	}
	
	private static class ReportNeighbors implements Function<Tuple2<Integer, Iterable<String>>, Boolean> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 7264653921012029029L;
		List<Integer> cells;
		
		ReportNeighbors(List<Integer> cells) {
			this.cells = cells;
		}

		@Override
		public Boolean call(Tuple2<Integer, Iterable<String>> input) throws Exception {
			if (this.cells.contains(input._1)) {
				return true;
			}
			return false;
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Problem 2
		SparkSession spark = SparkSession.builder().appName("GridCells").getOrCreate();
		
		JavaRDD<String> points = spark.read().textFile(inputPath).javaRDD();
		
		JavaPairRDD<Integer, Integer> cells = points.mapToPair(new GroupPoints());
		
		JavaPairRDD<Integer, Integer> cellsCount = cells.reduceByKey(new Sum());
		
		JavaPairRDD<Integer, Iterable<String>> regroupedCell = cellsCount.flatMapToPair(new ReGroup()).groupByKey();
		
		JavaPairRDD<Integer, Double> cellWithRDI = regroupedCell.mapToPair(new CalculateRDI());
		
		JavaPairRDD<Double, Integer> switchedRDI = cellWithRDI.mapToPair(new SwitchKeyValue());
		
		List<Tuple2<Double, Integer>> sortedRDI = switchedRDI.sortByKey(false).take(50);
		
		List<Integer> topCells = new ArrayList<Integer>();
		for (Tuple2<Double, Integer> v: sortedRDI) {
			topCells.add(v._2);
		}
		
		System.out.println(topCells);
		
		
		//Problem 3
		JavaPairRDD<Integer, Iterable<String>> neighbors = regroupedCell.filter(new ReportNeighbors(topCells));
		neighbors.saveAsTextFile(outputPath);
	}
}
