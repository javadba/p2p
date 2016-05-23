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
