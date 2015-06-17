package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool

/**
 * Created by rice on 08.06.15.
 */
object GraphMiningFlink {

  def main(args: Array[String]) {

    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    var seperator = "\t"
    var k = 10

    if (args.length > 3) {
      seperator = args(3)
    }
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // val parameter = ParameterTool.fromArgs(args);

    val rawGraph =  env.readTextFile(inputFile)
    val dataset = Truss.convertGraph(rawGraph, seperator)


    if (mode.equals("triangle"))
      Truss.getTriangles(dataset)

    if (mode.equals("truss"))
      Truss.calculateTruss(2, dataset)

//    if(mode.equals("maxtruss"))
//      MaximalTruss.maximumTruss(Truss.convertGraph(context.textFile(inputPath, 10), seperator), context, outputPath,args(4))

    // execute program.
    env.execute("Flink Scala Graph Mining")

  }

}
