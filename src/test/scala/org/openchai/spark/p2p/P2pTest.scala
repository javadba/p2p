package org.openchai.spark.p2p

/**
 * TcpCommTest
 *
 */
object P2pTest {

  val weightsMergePolicy: String = "best"
  val TestPort = 8989

  def main(args: Array[String]) {
    val host = TcpUtils.getLocalHostname
    val server = new TcpServer(host, TestPort, new UpdaterServerIF(weightsMergePolicy))
    new Thread() {
      override def run() {
        server.run
      }
    }.start
    TcpClient.main(Array("" + TestPort))
//    val w = serviceIf.run(ModelParams(new DefaultModel(), new DefaultHyperParams()),3)
    Thread.currentThread.join
  }
}
