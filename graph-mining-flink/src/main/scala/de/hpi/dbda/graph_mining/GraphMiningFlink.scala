package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import java.io.File
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.log4j.{Level, Logger}

import scalax.file.FileSystem

/**
 * Created by rice on 08.06.15.
 */
object GraphMiningFlink {

  def main(args: Array[String]) {
//
//    val level = Level.WARN
//    Logger.getLogger("org").setLevel(level)
//    Logger.getLogger("akka").setLevel(level)


    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    var seperator = "\t"
    var k = 10

    if (args.length > 3) {
      seperator = args(3)
    }

//    val host = "tenemhead2"
//    val port = 6123
//    val jars = "target/graph-mining-flink-1.0-SNAPSHOT.jar"
//    val parallelism = 10
//
//    val env = ExecutionEnvironment.createRemoteEnvironment(host, port, parallelism, jars);

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // val parameter = ParameterTool.fromArgs(args);

    val startTime = java.lang.System.currentTimeMillis()

    val rawGraph = env.readTextFile(inputPath)

    val rawGraphTime = java.lang.System.currentTimeMillis() - startTime

    var dataset = Truss.addDegrees(Truss.convertGraph(rawGraph, seperator))

    val addDegreesTime = java.lang.System.currentTimeMillis() - startTime - rawGraphTime

    dataset.writeAsText("D:/_uni/_Master3/DBDA/graph-mining/output/writeTest", WriteMode.OVERWRITE)

    dataset = Truss.convertDegreedGraph(env.readTextFile("D:/_uni/_Master3/DBDA/graph-mining/output/writeTest"), "\t")

    var maxTrussesTime = 0.toLong
    var writeOutputTime = 0.toLong

    if (mode.equals("triangle")) {
      val triangles = Truss.getTriangles(dataset, k)

      val output = outputPath + "/triangle"

    //  deleteFolder(output)

      triangles.writeAsCsv(output,  "\n", " ",  WriteMode.OVERWRITE)
    }

    if (mode.equals("truss")) {
      val truss = Truss.calculateTruss(args(4).toInt, dataset)

      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/truss"

//      deleteFolder(output)
//
      truss.writeAsCsv(output, "\n", " ", WriteMode.OVERWRITE)
    }


    if(mode.equals("maxtruss")) {
      val trusses = MaximalTruss.maxTruss(dataset, args(4))
//      val trusses = MaximalTruss.maxTruss1(dataset, args(4), env)

      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/maxtruss"
    //  deleteFolder(output)

      trusses.writeAsText(output, WriteMode.OVERWRITE)

      writeOutputTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime
    }

    // execute program.
//    println(env.getExecutionPlan())
    env.execute("Flink Scala Graph Mining")

    val endTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime
    val diffTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime

//    val fullTime = java.lang.System.currentTimeMillis() - startTime
//    val temp = 1+1
    println("############## overall used time = " + diffTime + " #######################")
    println("############## add Degrees time = " + addDegreesTime + " #########################")
    println("############## raw Graph reading time = " + rawGraphTime + " ######################")

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
