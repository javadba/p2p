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
package org.openchai.spark.p2p

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net._

import org.openchai.spark.p2p.TcpCommon._
import org.openchai.spark.util.Logger._
import org.openchai.spark.util.{Logger, TcpUtils}

import scala.collection.mutable

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
