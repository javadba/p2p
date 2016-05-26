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

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.openchai.spark.util.FileUtils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class LsSourceRDD[K1: /*Ordering: */ClassTag, V1: ClassTag, K2: /*Ordering: */ClassTag, V2: ClassTag](@transient sc: SparkContext, var paths: Seq[String])(implicit evk1: TypeTag[K1], evv1: TypeTag[V1], evk2: TypeTag[K2], evv2: TypeTag[V2])
  extends RDD[(K2, V2)](sc, Nil) with LsRDD[K2, V2] {

//  override val keyOrdering = new Ordering[String]() {
//    override def compare(x: String, y: String): Int = x.compareTo(y)
//  }
  override def compute(split: Partition, context: TaskContext): Iterator[(K2, V2)] = {
    val part = split.asInstanceOf[LsRDDPartition[Int]]
    val d = readFromRackPath(part.rackPath)
    //    val conv = converter(newType.newInstance, ev.newInstance) _
    d.split("\n").map(l => (part.rackPath.fullPath/*.substring(0,part.rackPath.fullPath.lastIndexOf("/"))*/, l)).iterator.asInstanceOf[Iterator[(K2, V2)]]
  }

  def readFromRackPath(rp: RackPath) = {
    val host = rp.host
    val sourceHost = host match {
      case h if InetAddress.getLocalHost.getHostName.equals(host) => h
//      case h if (h in rackpaths) => RackPath.hostToRack(host)
      case h => throw new UnsupportedOperationException(s"scp not implemented yet (requested host=$h)")
    }
    // TODO: st up scp read from the host

    val d = FileUtils.readPath(rp.path)
    d
  }

}
