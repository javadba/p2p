/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openchai.spark

import org.openchai.spark.util.TcpUtils

package object p2p {

  trait P2pConnectionParams

  trait ServiceIF {

    import reflect.runtime.universe.TypeTag
    def request[U: TypeTag, V: TypeTag](req: P2pReq[U]): P2pResp[V] = {
      getRpc.request(req)
    }
    protected[p2p] var optRpc: Option[P2pRpc] = None

    protected def getRpc() = optRpc match {
      case None => throw new IllegalStateException("RPC mechanism has not been set")
      case _ => optRpc.get
    }

    val clientName = TcpUtils.getLocalHostname
  }

  trait ServerIF {
    def service(req: P2pReq[_]): P2pResp[_]
  }

  sealed trait P2pMessage[T] {
    def value(): T
  }

  trait P2pReq[T] extends P2pMessage[T]

  trait P2pResp[T] extends P2pMessage[T]

  trait ArrayData[V] {
    def tag: String
    def dims: Seq[Int]
    def toArray: V
  }

  type DArray = Array[Double]
  case class MData(override val tag: String, override val dims: Seq[Int], override val toArray: DArray) extends ArrayData[DArray]
  type AnyData = MData
  case class TData(label: Double, data: Vector[Double])
}
