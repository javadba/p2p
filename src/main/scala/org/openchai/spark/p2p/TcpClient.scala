package org.openchai.spark.p2p

import TcpCommon._

case class TcpConnectionParams(server: String, port: Int) extends P2pConnectionParams

case class TcpClient(connParams: TcpConnectionParams, serviceIf: ServiceIF)
  extends P2pRpc with P2pBinding {

  import org.openchai.spark.util.Logger._

  import java.io._
  import java.net._

  import reflect.runtime.universe._

  private var sock: Socket = _
  private var os: OutputStream = _
  private var is: InputStream = _

  {
      connect(connParams)
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

  override def request[U : TypeTag , V :  TypeTag](req: P2pReq[U]): P2pResp[V] = {
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
  val TestPort = 8989
  def main(args: Array[String]) {
    import UpdaterIF._
    val port = if (args.length >= 1) args(0) else TestPort
    val serviceIf = new UpdaterIF
    val client = TcpClient(TcpConnectionParams("localhost", TestPort), serviceIf)
    val w = serviceIf.run(ModelParams(new DefaultModel(), new DefaultHyperParams()),TestData.mdata(10,100), 3)
  }
}
