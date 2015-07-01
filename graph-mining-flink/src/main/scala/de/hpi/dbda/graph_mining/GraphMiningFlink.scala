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

    val startTime = java.lang.System.currentTimeMillis()

    val rawGraph = env.readTextFile(inputPath)

    val rawGraphTime = java.lang.System.currentTimeMillis() - startTime

    val dataset = Truss.addDegrees(Truss.convertGraph(rawGraph, seperator))

    val addDegreesTime = java.lang.System.currentTimeMillis() - startTime - rawGraphTime

    if (mode.equals("triangle")) {
      val triangles = Truss.getTriangles(dataset)

      val output = outputPath + "/triangle"

      deleteFolder(output)

      triangles.writeAsCsv(output,  "\n", " ")
    }

    if (mode.equals("truss")) {
      val truss = Truss.calculateTruss(4, dataset)

      val output = outputPath + "/truss"

      deleteFolder(output)

      truss.writeAsCsv(output, "\n", " ")

      truss.print()
    }

    var maxTrussesTime = 0.toLong
    var writeOutputTime = 0.toLong

    if(mode.equals("maxtruss")) {
      val trusses = MaximalTruss.maxTruss(dataset, args(4))

      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/truss"
      deleteFolder(output)

      trusses.writeAsText(output)

      writeOutputTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime
    }

    // execute program.
   // println(env.getExecutionPlan())
    env.execute("Flink Scala Graph Mining")

    val fullTime = java.lang.System.currentTimeMillis() - startTime
    val temp = 1+1
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
