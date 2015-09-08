package de.hpi.dbda.graph_mining

import java.io.{File, PrintWriter}

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import java.io.File
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.log4j.{Level, Logger}

object GraphMiningFlink {

  def main(args: Array[String]) {

    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val separator = args(3)
    var k = 10

    val env = ExecutionEnvironment.getExecutionEnvironment


    val startTime = java.lang.System.currentTimeMillis()

    val rawGraph = env.readTextFile(inputPath)
    val rawGraphTime = java.lang.System.currentTimeMillis() - startTime

    val dataset = Truss.addDegrees(Truss.convertGraph(rawGraph, separator))
    val addDegreesTime = java.lang.System.currentTimeMillis() - startTime - rawGraphTime

    var maxTrussesTime = 0.toLong
    var writeOutputTime = 0.toLong

    if (mode.equals("triangle")) {
      val triangles = Truss.getTriangles(dataset, k)

      val output = outputPath + "/triangle"

      triangles.writeAsCsv(output,  "\n", " ",  WriteMode.OVERWRITE)
    }

    if (mode.equals("truss")) {
      val truss = Truss.calculateTruss(args(4).toInt, dataset)

      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/truss"

      truss.writeAsCsv(output, "\n", " ", WriteMode.OVERWRITE)
    }


    if(mode.equals("maxtruss")) {
      val trusses = MaximalTruss.maxTruss(dataset, args(4))
      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/maxtruss"

      trusses.writeAsText(output, WriteMode.OVERWRITE)

      writeOutputTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime
    }

    if(mode.equals("maxtrusswriting")) {
      val trusses = MaximalTruss.maxTrussWithWriting(dataset, args(4), env)

      maxTrussesTime = java.lang.System.currentTimeMillis() - addDegreesTime - startTime - rawGraphTime

      val output = outputPath + "/maxtruss"

      trusses.writeAsText(output, WriteMode.OVERWRITE)

      writeOutputTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime
    }

    env.execute("Flink Scala Graph Mining")

    val diffTime = java.lang.System.currentTimeMillis() - maxTrussesTime - addDegreesTime - startTime - rawGraphTime

    println("############## overall time = " + diffTime + " #######################")
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
