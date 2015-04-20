package de.hpi.dbda.nodehistogram
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main {
   def main(args: Array[String]) {
      println("Hello, world!")
      
     val conf = new SparkConf().setAppName("nodehistogram")
     val sparkContext = new SparkContext(conf)
     
     val distFile = sparkContext.textFile("../testinput.txt")
     distFile.map(s => s.length).reduce((a,b) => a + b)
     distFile.foreach { println}
    }
}