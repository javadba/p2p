package org.openchai.spark.util

import org.apache.spark.launcher.SparkLauncher

object Launcher {

  def main(args: Array[String]) {
    launch("org.openchai.spark.p2p.P2pRDDTest",Seq("1","2","3"))
  }
  def apply(mainClass: String, appArgs: Seq[String]) = launch(mainClass, appArgs)

  def launch(mainClass: String, appArgs: Seq[String]) = {
    println(s"Launching $mainClass with args ${appArgs.mkString(" ")} ..")
    val spark = new SparkLauncher()
      .setSparkHome("/shared/sparkmaven")
      .setAppResource("/shared/wfdemo/target/scala-2.11/keywordsservlet_2.11-0.1.0-SNAPSHOT.jar")
      //    .setMainClass("com.astralync.demo.spark.RegexFilters")
      .setMainClass(mainClass)
      .setMaster("mellryn.local")
    //      .addSparkArg()
    if (!appArgs.isEmpty) {
      spark.addAppArgs(appArgs: _*)
    }
    spark.launch()
    println("launched.. and waiting..")
//    spark.waitFor()  // This method disappeared!!
    Thread.currentThread.join
    println("crapped out")

  }
}
