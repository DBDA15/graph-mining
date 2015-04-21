package de.hpi.uni_potsdam.de.nodeHistogramJava;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import scala.Tuple2;





public class NodeHistogramm {
	
	static String SPLITCHARACTER = "\t";
	static String outputFollower = "output/follower";
	static String outputFollowing = "output/following";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("de.hpi.uni_potsdam.de.nodeHistogramJava.NodeHistogram");
		JavaSparkContext sc = new JavaSparkContext(conf);
		File outputFollowerFile = new File(outputFollower);
		File outputFollowingFile = new File(outputFollowing);
		try {
			FileUtils.deleteDirectory(outputFollowerFile);
			FileUtils.deleteDirectory(outputFollowingFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		spark(sc, 0, outputFollower);
		spark(sc, 1, outputFollowing);
	}

	public static void spark(JavaSparkContext sc, final int type, String outputLocation){

		JavaRDD<String> lines = sc.textFile("../smallTwitter.txt");

		JavaPairRDD<String, Integer> followerPairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
		  public Tuple2<String, Integer> call(String s) { 
			  	return new Tuple2<String, Integer>(s.split(SPLITCHARACTER)[type], 1); 
			  }
		});
		
		JavaPairRDD<String, Integer> followerCount = followerPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		followerCount.saveAsTextFile(outputLocation); 
	}
	
}
