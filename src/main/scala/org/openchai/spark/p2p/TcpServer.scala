package org.openchai.spark.p2p

import java.io.{BufferedOutputStream, BufferedInputStream}
import java.net._
import java.util.concurrent.Executors
import TcpCommon._
import org.openchai.spark.util.{TcpUtils, Logger}
import Logger._
import collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object TcpServer {
  val BufSize = (Math.pow(2, 20) - 1).toInt
  // 1Meg
  val weightsMergePolicy: String = "best"
  val TestPort = 8989
  var serverThread: Thread = _

  def startServer(port: Int) {
    val host = TcpUtils.getLocalHostname
    val server = new TcpServer(host, port, new UpdaterServerIF(weightsMergePolicy))
    serverThread = new Thread() {
      override def run() {
        server.run
      }
    }
    serverThread.start
  }

  def main(args: Array[String]) {
    startServer(TestPort)
    //    val w = serviceIf.run(ModelParams(new DefaultModel(), new DefaultHyperParams()),3)
    Thread.currentThread.join
  }
}

case class TcpServer(host: String, port: Int, serviceIf: ServerIF) extends P2pServer with P2pBinding {

  import TcpServer._

  private var serverSocket: ServerSocket = _
  private var stopRequested: Boolean = _
  val threads = mutable.ArrayBuffer[Thread]()

  def CheckPort = false
  def checkPort(port: Int) = {
    import scala.concurrent.duration._
    val socketTimeout = 200
    val duration: Duration = 10 seconds
//    if (CheckPort) {
//      val result =
//        Future {
//          Try {
//            val socket = new java.net.Socket()
//            socket.connect(new InetSocketAddress("localhost", port), socketTimeout)
//            socket.close()
//            port
//          } toOption
//        }
//      Try {
//        Await.result(result, duration)
//      }.toOption.getOrElse(Nil)
//    }
  }

  override def start(): Boolean = {
    serverSocket = new ServerSocket()
//    checkPort(port) match {
//      case m: Exception => error(s"Server already running on port $port"); false
      /* case _ => */ serverSocket.bind(new InetSocketAddress(host, port)); true
//    }
  }

  def service(socket: Socket): Thread = {
    val sockaddr = socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress]
    info(s"Received connection request from ${sockaddr.getHostName}@${sockaddr.getAddress.getHostAddress} on socket ${socket.getPort}")
    val t = new Thread() {
      override def run() = {
        val is = new BufferedInputStream(socket.getInputStream)
        val os = new BufferedOutputStream(socket.getOutputStream)
        val buf = new Array[Byte](BufSize)
        do {
          debug("Listening for messages..")
          //          while (is.available() <= 0) {
          //            Thread.sleep(100)
          //          }
          is.read(buf)
          val req = deserialize(buf).asInstanceOf[P2pReq[_]]
          debug(s"Message received: ${req.toString}")
          val resp = serviceIf.service(req)
          debug(s"Sending response:  ${resp.toString}")
          val ser = serialize(resp)
          os.write(ser)
          os.flush
        } while (!reconnectEveryRequest)
        Thread.sleep(5000)
      }
    }
    t
  }

  override def run(): Unit = {
    if (serverSocket == null) {
      start
    }
    while (!stopRequested) {
      val t = service(serverSocket.accept())
      t.start
      threads += t
    }
  }

  override def stop(): Boolean = ???
}
