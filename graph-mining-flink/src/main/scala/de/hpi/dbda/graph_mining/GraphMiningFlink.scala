package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import java.io.File

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

    val rawGraph = env.readTextFile(inputPath)
    val dataset = Truss.addDegrees(Truss.convertGraph(rawGraph, seperator))


    if (mode.equals("triangle")) {
      val triangles = Truss.getTriangles(dataset, 1)

      val output = outputPath + "/triangle"

      deleteFolder(output)

      triangles.writeAsCsv(output,  "\n", " ")
    }

    if (mode.equals("truss")) {
      val truss = Truss.calculateTruss(2, dataset)

      val output = outputPath + "/truss"

      deleteFolder(output)

      truss.writeAsCsv(output, "\n", " ")

      truss.print()
    }

//    if(mode.equals("maxtruss"))
//      MaximalTruss.maximumTruss(Truss.convertGraph(context.textFile(inputPath, 10), seperator), context, outputPath,args(4))

    // execute program.
    env.execute("Flink Scala Graph Mining")

  }

  def deleteFolder(folderPath:String) {
    val folder = new File(folderPath)
    if (folder.isDirectory()) {
      for (c <- folder.listFiles())
        deleteFolder(c.getAbsolutePath)
    }
    folder.delete()
  }
}
