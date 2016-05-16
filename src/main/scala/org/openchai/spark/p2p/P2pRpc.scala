package org.openchai.spark.p2p


trait P2pRpc {

  import reflect.runtime.universe.TypeTag

  def connect(connParam: P2pConnectionParams): P2pRpc
  def isConnected: Boolean

  def request[U: TypeTag, V: TypeTag](req: P2pReq[U]): P2pResp[V]

}
