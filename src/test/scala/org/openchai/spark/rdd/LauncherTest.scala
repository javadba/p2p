package org.openchai.spark.rdd

import org.apache.spark.launcher.SparkLauncher

object LauncherTest extends App {
  val spark = new SparkLauncher()
    .setSparkHome("/shared/sparkmaven")
    .setAppResource("/shared/wfdemo/target/scala-2.11/keywordsservlet_2.11-0.1.0-SNAPSHOT.jar")
//    .setMainClass("com.astralync.demo.spark.RegexFilters")
    .setMainClass("org.openchai.spark.p2p.P2pRDDTest")
    .setMaster("mellryn.local")
    .launch()
  println("launched.. and waiting..")
  spark.waitFor()
  println("crapped out")
}