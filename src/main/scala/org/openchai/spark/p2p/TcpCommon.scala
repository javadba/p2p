package org.openchai.spark.p2p

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

object TcpCommon {
  def serialize(a: Any): Array[Byte] = {
    // TODO: determine how to properly size the bos
    val bos = new ByteArrayOutputStream(2 ^ 18)
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(a)
    bos.toByteArray
  }

  def deserialize(a: Array[Byte]): Any = {
    import java.io._
    // TODO: determine how to properly size the bos
    val bis = new ByteArrayInputStream(a)
    val ois = new ObjectInputStream(bis)
    ois.readObject
  }

}
