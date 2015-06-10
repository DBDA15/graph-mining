package de.hpi.dbda.graph_mining

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Created by rice on 08.06.15.
 */
object GraphMiningFlink {

  def main(args: Array[String]) {
    val inputFile:String = ""

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.readFile('csv', inputFile)

    // execute program
    env.execute("Flink Scala Graph Mining")
  }

}
