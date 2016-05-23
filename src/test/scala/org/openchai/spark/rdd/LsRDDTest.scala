package org.openchai.spark.rdd

import java.io.File

import org.apache.spark.SparkContext
import org.openchai.spark.util.TcpUtils

class LsRDDTest

object LsRDDTest {
  def rdd(sc: SparkContext, master: String) = {
    val dir = "/data/lsrdd"
    val dirs = new File(dir).listFiles.filter(f => f.isDirectory && !f.getName.startsWith("."))
    val rpaths = dirs.map(d => s"${TcpUtils.getLocalHostname}:${RackPath.hostToRack(TcpUtils.getLocalHostname)}:$d")
    val lsrdd = new LsSourceRDD[String,String,String,String](sc, rpaths)
    lsrdd
  }

  // TODO: remove hardcoding of paths..
  val SparkHome = "/shared/sparkmaven"
  def rddTest(master: String, appArgs: Array[String]) = {
    val env = Map("SPARK_HOME" -> SparkHome,"spark.driver.memory"->"4g")

    val jars = Seq("/git/ocrdd/libs/spark_p2prdd-1.0.0-tests.jar",
      "/git/ocrdd/libs/spark_p2prdd-1.0.0.jar")
    val sc = new SparkContext(master, "LsRddTest",SparkHome, jars,env)
    val lsrdd = rdd(sc, master)
    val lines = lsrdd.map(_._2.split("\n"))
    val twenty = lines.take(20).map(_.mkString(","))
    println(s"lines are ${twenty.zipWithIndex.mkString("\n")}")
    println(s"LsRDD count=${lsrdd.count}")
  }

  def main(args: Array[String]) {
      val master = args(0)
      val appArgs = if (args.length > 1) args.slice(1, args.length) else new Array[String](0)
      println(s"Calling rddTest with master=$master and appArgs=${appArgs.mkString(" ")}")
      rddTest(master, appArgs)
  }

}

