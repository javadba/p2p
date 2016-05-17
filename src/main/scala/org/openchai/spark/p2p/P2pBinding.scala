package org.openchai.spark.p2p

trait P2pBinding {

  def bind(rpc: P2pRpc, serviceIf: ServiceIF) = {
    assert(rpc.isConnected)
    serviceIf.optRpc = Some(rpc)
  }

  val reconnectEveryRequest: Boolean = false
}
