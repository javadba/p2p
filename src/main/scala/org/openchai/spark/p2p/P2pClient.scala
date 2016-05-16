package org.openchai.spark.p2p

trait P2pClient {

  def bind(rpc: P2pRpc, serviceIf: ServiceIF) = {
    assert(rpc.isConnected)
    serviceIf.rpc = rpc
  }

}
