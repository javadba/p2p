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

import org.apache.spark.SparkContext
import org.openchai.spark.p2p.{TcpConnectionParams, TcpServer}
import org.openchai.spark.util.TcpUtils

object P2pRDDTest {
  def main(args: Array[String]) = {
    val master = args(0)
    val server = if (master.startsWith("spark")) {
      master.substring(master.lastIndexOf("/") + 1, master.lastIndexOf(":"))
    } else if (master.startsWith("local")) {
      TcpUtils.getLocalHostname
    } else {
      throw new IllegalArgumentException(s"Unable to parse the server from the master $master")
    }

    val sc = new SparkContext(master,"P2pRDDTest")
    val lsrdd = LsRDDTest.sourceRdd(sc, master, "/data/lsrdd")
    val params = TcpConnectionParams(server, TcpServer.TestPort)
    val p2pRdd = new P2pRDD/*[LabeledArr]*/(sc, lsrdd, params)
    val cnt = p2pRdd.count

  }
}
