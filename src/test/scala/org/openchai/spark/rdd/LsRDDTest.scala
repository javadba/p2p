/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openchai.spark.rdd

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openchai.spark.util.{FileUtils, TcpUtils}

class LsRDDTest

object LsRDDTest {
  def sourceRdd(sc: SparkContext, master: String, dir: String) = {
    val dirs = new File(dir).listFiles.filter(f => f.isDirectory && !f.getName.startsWith("."))
    val rpaths = dirs.map(d => s"${TcpUtils.getLocalHostname}:${RackPath.hostToRack(TcpUtils.getLocalHostname)}:$d")
    val lsrdd = new LsSourceRDD[String,String,String,String](sc, rpaths)
    lsrdd
  }

  // TODO: remove hardcoding of paths..
  val SparkHome = "/shared/sparkmaven"
  def rddSourceTest(master: String, appArgs: Array[String]) = {
    val env = Map("SPARK_HOME" -> SparkHome,"spark.driver.memory"->"4g")

    val jars = Seq("/git/ocrdd/libs/spark_p2prdd-1.0.0-tests.jar",
      "/git/ocrdd/libs/spark_p2prdd-1.0.0.jar")
    val sc = new SparkContext(master, "LsRddTest",SparkHome, jars,env)
    val lsrdd = sourceRdd(sc, master,"/data/lsrdd")
    val lines = lsrdd.map(_._2.split("\n"))
    val twenty = lines.take(20).map(_.mkString(","))
    println(s"lines are ${twenty.zipWithIndex.mkString("\n")}")
    println(s"LsRDD count=${lsrdd.count}")
  }

  def sinkRdd(sc: SparkContext, master: String, parent: RDD[(String,String)], dir: String) = {
    FileUtils.mkdirs(dir)
    val dirs = (0 until 5).map(d => s"$dir/sink$d")
    val rpaths = dirs.map(d => s"${TcpUtils.getLocalHostname}:${RackPath.hostToRack(TcpUtils.getLocalHostname)}:$d")
    val lsrdd = new LsSinkRDD[String,String,String,String](rpaths, parent)
    lsrdd
  }

  def rddSinkTest(master: String, appArgs: Array[String]) = {
    val env = Map("SPARK_HOME" -> SparkHome,"spark.driver.memory"->"4g")

    val jars = Seq("/git/ocrdd/libs/spark_p2prdd-1.0.0-tests.jar",
      "/git/ocrdd/libs/spark_p2prdd-1.0.0.jar")
    val sc = new SparkContext(master, "LsRddTest",SparkHome, jars,env)

    val lsrdd = sourceRdd(sc, master,"/data/lsrdd")
    val lines = lsrdd.map(_._2.split("\n"))

    val outdir = "/data/rddout"
    val sinkrdd = sinkRdd(sc, master, lsrdd,outdir)
    sinkrdd.saveAsTextFile(sinkrdd.rackPaths.map(_.fullPath).mkString(","))

    val sourcerdd2 = sourceRdd(sc, master,outdir)
    val srclines = sourcerdd2.map(_._2.split("\n"))
    println(s"LsRdd2 count=${srclines.count}")
  }
  
  def main(args: Array[String]) {
      val master = args(0)
      val appArgs = if (args.length > 1) args.slice(1, args.length) else new Array[String](0)
      println(s"Calling rddTest with master=$master and appArgs=${appArgs.mkString(" ")}")
//      rddSourceTest(master, appArgs)
      rddSinkTest(master, appArgs)
  }

}

