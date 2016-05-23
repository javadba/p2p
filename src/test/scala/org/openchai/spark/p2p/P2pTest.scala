package org.openchai.spark.p2p

/**
 * TcpCommTest
 *
 */
object P2pTest {

  val weightsMergePolicy: String = "best"
  val TestPort = TcpServer.TestPort

  def main(args: Array[String]) {
    TcpServer.startServer(TestPort)
    TcpClient.main(Array("" + TestPort))
    Thread.currentThread.join
  }
}
