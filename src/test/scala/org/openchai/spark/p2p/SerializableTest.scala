package org.openchai.spark.p2p

import reflect.runtime.universe._

abstract class MySerializable[T <: Serializable : TypeTag] {


  def serialize(t: T)

  def request[U <: Serializable : TypeTag, V <: Serializable : TypeTag](req: P2pReq[U]): P2pResp[V]

}

class Serz extends Serializable

class Ser extends MySerializable[Serz] {

  import reflect.runtime.universe._
  import reflect.runtime._

  override def serialize(t: Serz): Unit = {}

  //
  override def request[U <: Serializable : TypeTag, V <: Serializable : TypeTag](req: P2pReq[U]): P2pResp[V] = {
    new P2pResp[V] {
      override def value() = (new Serz).asInstanceOf[V]
    }
  }
}
