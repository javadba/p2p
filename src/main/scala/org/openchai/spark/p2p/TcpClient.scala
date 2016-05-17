package org.openchai.spark.p2p

import TcpCommon._
import org.openchai.spark.p2p.UpdaterIF.TrainingParams

case class TcpConnectionParams(server: String, port: Int) extends P2pConnectionParams

case class TcpClient(connParams: TcpConnectionParams, serviceIf: ServiceIF)
  extends P2pRpc with P2pBinding {

  import TcpClient._
  import org.openchai.spark.util.Logger._

  import java.io._
  import java.net._

  import reflect.runtime.universe._

  private var sock: Socket = _
  private var os: OutputStream = _
  private var is: InputStream = _

  {
      connect(connParams)
//    bind(this, serviceIf)
  }
  override def isConnected: Boolean = is != null && os != null

  override def connect(connParam: P2pConnectionParams): Boolean = {
    savedConnParam = connParam
    val tconn = connParams.asInstanceOf[TcpConnectionParams]
    sock = new Socket(tconn.server, tconn.port)
    os = sock.getOutputStream
    is = sock.getInputStream
    bind(this, serviceIf)
    is != null && os != null
  }

  private var savedConnParam: P2pConnectionParams = _

  override def request[U /*<: Serializable */ : TypeTag , V /*<: Serializable */ :  TypeTag](req: P2pReq[U]): P2pResp[V] = {
    // TODO: determine how to properly size the bos
    if (!isConnected) {
      connect(savedConnParam)
    }
    val buf = new Array[Byte](Math.pow(2,20).toInt)
    val serreq = serialize(req)
    os.write(serreq)
    val nread = is.read(buf)
    info(s"request: received $nread bytes")
    val o = deserialize(buf)
    val out = o.asInstanceOf[P2pResp[V]]
    if (reconnectEveryRequest) {
      sock.close
      sock = null
      os = null
      is = null
    }
    out
  }
}

object TcpClient {
  def main(args: Array[String]) {
    import UpdaterIF._
    val serviceIf = new UpdaterIF
    val client = TcpClient(TcpConnectionParams("localhost", 7088), serviceIf)
    val w = serviceIf.run(TrainingParams(new DefaultModel(), new DefaultHyperParams()),3)
  }
}
// show
