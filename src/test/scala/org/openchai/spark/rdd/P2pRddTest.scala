package org.openchai.spark.rdd

import org.apache.spark.SparkContext
import org.openchai.spark.p2p.{TcpServer, TcpConnectionParams, P2pConnectionParams}
import org.openchai.spark.rdd.LsRDD.LabeledArr

object P2pRddTest {
  def main(args: Array[String]) = {
    val master = args(0)
    val sc = new SparkContext(master,"P2pRDDTest")
    val lsrdd = LsRDDTest.rdd(sc, master)
    val params = TcpConnectionParams(master, TcpServer.TestPort)
    val p2pRdd = new P2pRDD/*[LabeledArr]*/(sc, lsrdd, params)
  }
}