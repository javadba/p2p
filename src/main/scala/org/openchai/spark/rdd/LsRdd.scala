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

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner}


trait LsRDD[K, V] extends RDD[(K, V)] with Serializable {

  def paths(): Seq[String]

  def rackPaths = paths().map { p => RackPath(p) }.sortBy { p => p.fullPath }

  type KV[A, B] = (A, B)
  type KVO = KV[K, V]

  def rackPathsToPartitions[T] = rackPaths.zipWithIndex.map { case (r, x) =>
    LsRDDPartition[T](x, r)
  }.toArray // .asInstanceOf[Array[Partition]]

  protected[rdd] var parts: Array[Partition] = getPartitions

  override protected def getPartitions: Array[Partition] =
    rackPathsToPartitions[V].asInstanceOf[Array[Partition]]

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val lsPartition = split.asInstanceOf[LsRDDPartition[Double]]
    Seq(lsPartition.rackPath.host)
  }

}

object LsRDD {

  import reflect.runtime.universe._

  val Delim = '\t'

  @inline def tagToClass[W: TypeTag] = typeTag[W].mirror.runtimeClass(typeOf[W]).newInstance.asInstanceOf[W]

  case class LabeledArr(label: String, value: Double, data: Array[Double])

  def converter[T: TypeTag, U: TypeTag](outClass: Class[U], oin: T): U = {
    if (!oin.isInstanceOf[String]) {
      throw new UnsupportedOperationException(s"Only RDD[String] presently supported as input to LsRDD  (actual type=${oin.getClass.getName}. Check back later.")
    }
    val in = oin.asInstanceOf[String]

    val string = classOf[String]
    val double = classOf[Double]
    val darr = classOf[Array[Double]]
    val labeledarr = classOf[LabeledArr]

    val out = outClass match {
      case x if x == string => in
      case x if x == double => in
      case x if x == darr => in.split(Delim).map(_.toDouble)
      case x if x == labeledarr => {
        val headTail = in.split(Delim).splitAt(1)
        (headTail._1.head, headTail._2.map(_.toDouble).splitAt(1))
      }
      case _ => throw new IllegalArgumentException(s"Type $outClass not supported")
    }
    out.asInstanceOf[U]
  }

}

