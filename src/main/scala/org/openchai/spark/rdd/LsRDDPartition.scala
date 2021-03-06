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
package org.openchai.spark.rdd

import org.apache.spark.{Partitioner, Partition}

case class LsRDDPartition[T](override val index: Int, rackPath: RackPath) extends Partition

class LsRDDPartitioner[K,V](parts: Seq[LsRDDPartition[V]]) extends Partitioner {

  import collection.mutable

  override def numPartitions: Int = parts.length

  val sorted = parts.sortBy(_.rackPath.fullPath).zipWithIndex
  val sortedMap = sorted.foldLeft(mutable.HashMap[String, Int]()) { case (h, (p, x)) =>
    h(p.rackPath.fullPath) = x
    h
  }

  override def getPartition(key: Any): Int = {
    Math.abs(key.hashCode) % sortedMap.size
//    sortedMap(key.asInstanceOf[String])
  }
}

