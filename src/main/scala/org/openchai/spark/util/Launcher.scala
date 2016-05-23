package org.openchai.spark.util

import org.apache.spark.launcher.SparkLauncher

object Launcher {

  def main(args: Array[String]) {
    launch("org.openchai.spark.p2p.P2pRDDTest", Seq("1", "2", "3"))
  }

  def apply(mainClass: String, appArgs: Seq[String]) = launch(mainClass, appArgs)

  def launch(mainClass: String, appArgs: Seq[String]) = {
    println(s"Launching $mainClass with args ${appArgs.mkString(" ")} ..")
    import collection.JavaConverters._
    val env = Map("SPARK_PRINT_LAUNCH_COMMAND" -> "1").asJava
    val master = appArgs(0)
    val spark = new SparkLauncher(env)
      .addSparkArg("--verbose")
      .setSparkHome("/shared/sparkmaven")
      .setAppResource("/git/ocrdd/libs/spark_p2prdd-tests-1.0.0.jar")
      .setAppResource("/git/ocrdd/libs/spark_p2prdd-1.0.0.jar")
      .setMainClass(mainClass)
      .setMaster(master)
    if (!appArgs.isEmpty) {
      spark.addAppArgs(appArgs: _*)
    }
    val s = spark.launch()
    println("launched.. and waiting..")
    s.waitFor()
    println("crapped out")

  }
}
