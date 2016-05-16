package org.openchai.spark.p2p

import TcpCommon._

case class TcpConnectionParams(server: String, port: Int) extends P2pConnectionParams

case class TcpClient(connParams: TcpConnectionParams, serviceIf: ServiceIF)
  extends P2pRpc with P2pClient {

  import TcpClient._
  import org.openchai.spark.util.Logger._

  import java.io._
  import java.net._

  import reflect.runtime.universe._

  private var sock: Socket = _
  private var os: OutputStream = _
  private var is: InputStream = _

  override def isConnected: Boolean = is != null && os != null

  override def connect(connParam: P2pConnectionParams): Boolean = {
    val tconn = connParams.asInstanceOf[TcpConnectionParams]
    sock = new Socket(tconn.server, tconn.port)
    os = sock.getOutputStream
    is = sock.getInputStream
    bind(this, serviceIf)
    is != null && os != null
  }

  override def request[U /*<: Serializable */ : TypeTag , V /*<: Serializable */ :  TypeTag](req: P2pReq[U]): P2pResp[V] = {
    // TODO: determine how to properly size the bos
    val buf = new Array[Byte](2 ^ 18)
    val serreq = serialize(req)
    os.write(serreq)
    val nread = is.read(buf)
    info(s"request: received $nread bytes")
    val o = deserialize(buf)
    val out = o.asInstanceOf[P2pResp[V]]
    out
  }
}

object TcpClient {
  def main(args: Array[String]) {
    val serviceIf = new UpdaterIF
    val client = TcpClient(TcpConnectionParams("localhost", 7088), serviceIf)
    val w = serviceIf.getWeights

  }
}
// show
