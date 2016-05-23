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
