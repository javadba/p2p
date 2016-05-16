package org.openchai.spark

package object p2p {

  trait P2pConnectionParams

  trait ServiceIF[T] {

    import reflect.runtime.universe.TypeTag
    def request[U: TypeTag, V: TypeTag](req: P2pReq[U]): P2pResp[V] = {
      getRpc.request(req)
    }
    var rpc: P2pRpc = _
    protected var optRpc: Option[P2pRpc] = None

    protected def getRpc() = optRpc match {
      case None => throw new IllegalStateException("RPC mechanism has not been set")
      case _ => optRpc.get
    }

    val clientName = TcpUtils.getLocalHostname
  }

  sealed trait P2pMessage[T] {
    def value(): T
  }

  trait P2pReq[T] extends P2pMessage[T]

  trait P2pResp[T] extends P2pMessage[T]
}
