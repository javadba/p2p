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
