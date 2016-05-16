package org.openchai.spark.p2p

/**
 * TcpCommTest
 *
 */
object TcpCommTest {
  class TestUpdaterIF extends UpdaterIF {

  }
  def main(args: Array[String]) {
    val host = TcpUtils.getLocalHostname
    val port = 8989
    val server = new TcpServer(host, port, )
  }
}
