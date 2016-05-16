package org.openchai.spark.p2p

import java.io.{BufferedOutputStream, BufferedInputStream}
import java.net.{Socket, InetSocketAddress, SocketAddress, ServerSocket}
import java.util.concurrent.Executors
import TcpCommon._
import org.openchai.spark.util.Logger
import Logger._
import collection.mutable

object TcpServer {
  val BufSize = (Math.pow(2, 20) - 1).toInt // 1Meg
}

case class TcpServer(host: String, port: Int, serviceIf: P2pIF) extends P2pServer {

  import TcpServer._

  private var serverSocket: ServerSocket = _
  private var stopRequested: Boolean = _
  val threads = mutable.ArrayBuffer[Thread]()

  override def start(): Boolean = {
    serverSocket = new ServerSocket()
    serverSocket.bind(new InetSocketAddress(host, port))
    true
  }

  def service(socket: Socket): Thread = {
    val sockaddr = socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress]
    info(s"Received connection request from ${sockaddr.getHostName}@${sockaddr.getAddress.getHostAddress}")
    val t = new Thread() {
      override def run() = {
        val is = new BufferedInputStream(socket.getInputStream)
        val os = new BufferedOutputStream(socket.getOutputStream)
        val buf = new Array[Byte](BufSize)
        while (true) {
          debug("Listening for messages..")
          is.read(buf)
          val obj = deserialize(buf)
          debug(s"Message received: ${obj.toString}")
          val resp = serviceIf.service(obj)
          debug(s"Sending response:  ${resp.toString}")
          val ser = serialize(resp)
          os.write(ser)
        }
      }
    }
    t
  }

  override def run(): Unit = {
    while (!stopRequested) {
      val t = service(serverSocket.accept())
      t.start
      threads += t
    }
  }

  override def stop(): Boolean = ???
}