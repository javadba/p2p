package org.openchai.spark.p2p

/**
 * TcpCommTest
 *
 */
object P2pTest {
  def main(args: Array[String]) {
    val host = TcpUtils.getLocalHostname
    val port = 8989
    val server = new TcpServer(host, port, new UpdaterServerIF)
    new Thread() {
      override def run() {
        server.run
      }
    }.start
    import UpdaterIF._
    val serviceIf = new UpdaterIF
    val client = TcpClient(TcpConnectionParams("localhost", port), serviceIf)
    val w = serviceIf.run(TrainingParams(new DefaultModel(), new DefaultHyperParams()),3)
    Thread.currentThread.join
  }
}
