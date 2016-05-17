package org.openchai.spark.p2p

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
 * TcpCommon
 *
 */
object TcpCommon {
  def serialize /*[T <: Serializable : TypeTag]*/ (a: Any): Array[Byte] = {
    // TODO: determine how to properly size the bos
    val bos = new ByteArrayOutputStream(2 ^ 18)
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(a)
//    bos.close
    bos.toByteArray
  }

  def deserialize /*[T <: Serializable : TypeTag]*/ (a: Array[Byte]): Any = {
    import java.io._
    // TODO: determine how to properly size the bos
//    val buf = new Array[Byte](2 ^ 18)
    val bis = new ByteArrayInputStream(a)
    val ois = new ObjectInputStream(bis)
//    bis.close
    ois.readObject
    //    val out = ois.asInstanceOf[T]
    //    out
  }

}
