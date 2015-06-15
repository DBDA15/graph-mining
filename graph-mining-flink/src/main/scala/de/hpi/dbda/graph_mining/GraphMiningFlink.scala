package de.hpi.dbda.graph_mining

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Created by rice on 08.06.15.
 */
object GraphMiningFlink {

  def main(args: Array[String]) {
    val inputFile:String = "../trussMini.txt"

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

   val rawGraph =  env.readTextFile(inputFile)
   val dataset = Truss.convertGraph(rawGraph, "\t")

    Truss.getTriangles(dataset)


    Truss.calculateTruss(2, dataset)

//dataset.print()

    // execute program.
    env.execute("Flink Scala Graph Mining")
  }

}
