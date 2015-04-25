package de.hpi.uni_potsdam.de.nodeHistogramJava;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class NodeHistogramm {
	
	static String SPLITCHARACTER = "\t";
//	static String outputFollower = "output/follower";
//	static String outputFollowing = "output/following";
//	static String outputAll = "output/all";
	
	public static void main(String[] args) {
		final String input = args[0];
		final String outputFollower = args[1];
		final String outputFollowing = args[2];
		final String outputCombined = args[3];
		SparkConf conf = new SparkConf().setAppName("de.hpi.uni_potsdam.de.nodeHistogramJava.NodeHistogram");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//delete dirs
		File outputFollowerFile = new File(outputFollower);
		File outputFollowingFile = new File(outputFollowing);
		File outputAllFile = new File(outputCombined);
		try {
			FileUtils.deleteDirectory(outputFollowerFile);
			FileUtils.deleteDirectory(outputFollowingFile);
			FileUtils.deleteDirectory(outputAllFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//follower
		JavaPairRDD<String, Integer> followerCount =  calculateCounts(sc, input, 0);
		followerCount.saveAsTextFile(outputFollower);
		
		//following
		JavaPairRDD<String, Integer> followingCount = calculateCounts(sc, input, 1);
		followerCount.saveAsTextFile(outputFollowing); 
		
		//all
		JavaPairRDD<String, Integer> follow = followingCount.union(followerCount);
		JavaPairRDD<String, Integer> count = sum(follow);
		count.saveAsTextFile(outputCombined); 
	}

	public static JavaPairRDD<String, Integer> calculateCounts(JavaSparkContext sc, final String inputFile, final int type){
		JavaRDD<String> lines = sc.textFile(inputFile);

		JavaPairRDD<String, Integer> followerPairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 2035592311483698209L;

		public Tuple2<String, Integer> call(String s) { 
			  	return new Tuple2<String, Integer>(s.split(SPLITCHARACTER)[type], 1); 
			  }
		});
		
		JavaPairRDD<String, Integer> followerCount = sum(followerPairs);
		
		return followerCount;
	}
	
	public static JavaPairRDD<String, Integer> sum (JavaPairRDD<String, Integer> followPairs){
		return followPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 9072894169891059255L;

			public Integer call(Integer a, Integer b) { return a + b; }
			});
	}
}
