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
	static String outputFollower = "output/follower";
	static String outputFollowing = "output/following";
	static String outputAll = "output/all";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("de.hpi.uni_potsdam.de.nodeHistogramJava.NodeHistogram");
		JavaSparkContext sc = new JavaSparkContext(conf);
		File outputFollowerFile = new File(outputFollower);
		File outputFollowingFile = new File(outputFollowing);
		File outputAllFile = new File(outputAll);
		try {
			FileUtils.deleteDirectory(outputFollowerFile);
			FileUtils.deleteDirectory(outputFollowingFile);
			FileUtils.deleteDirectory(outputAllFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//follower
		JavaPairRDD<String, Integer> followerCount =  calculateCounts(sc, 0);
		followerCount.saveAsTextFile(outputFollower);
		
		//following
		JavaPairRDD<String, Integer> followingCount = calculateCounts(sc, 1);
		followerCount.saveAsTextFile(outputFollowing); 
		
		//all
		JavaPairRDD<String, Integer> follow = followingCount.union(followerCount);
		JavaPairRDD<String, Integer> count = sum(follow);
		count.saveAsTextFile(outputAll); 
	}

	public static JavaPairRDD<String, Integer> calculateCounts(JavaSparkContext sc, final int type){
		JavaRDD<String> lines = sc.textFile("../smallTwitter.txt");

		JavaPairRDD<String, Integer> followerPairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
		  public Tuple2<String, Integer> call(String s) { 
			  	return new Tuple2<String, Integer>(s.split(SPLITCHARACTER)[type], 1); 
			  }
		});
		
		JavaPairRDD<String, Integer> followerCount = sum(followerPairs);
		
		return followerCount;
	}
	
	public static JavaPairRDD<String, Integer> sum (JavaPairRDD<String, Integer> followPairs){
		return followPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			  public Integer call(Integer a, Integer b) { return a + b; }
			});
	}
	


	
	
	
}
