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

import java.io.File

import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.{OrderedRDDFunctions, RDD}
import org.openchai.spark.util.{Logger, FileUtils}

import reflect.runtime.universe._
import scala.reflect.ClassTag

case class LsSinkRDD[K1: ClassTag, V1: ClassTag, K2: ClassTag, V2: ClassTag](@transient sc: SparkContext, var paths: Seq[String],
  parent: RDD[(K1, V1)])(implicit evk1: TypeTag[K1], evv1: TypeTag[V1], evk2: TypeTag[K2], evv2: TypeTag[V2])
  extends RDD[(K2, V2)](parent) with LsRDD[K2, V2] {

  import LsRDD._

  import Logger._
  paths.foreach( p => FileUtils.mkdirs(p))
  override /* lazy */ val partitioner /*: LsRDDPartitioner[K2,V2]*/ = Some(new LsRDDPartitioner[K2,V2](getPartitions.asInstanceOf[Array[LsRDDPartition[V2]]]))

  override def saveAsTextFile(rackPathsSeparatedByCommas: String): Unit = {
    saveToRackPathAsTextFile(rackPathsSeparatedByCommas.split(","))
  }

  def toRecord(tup: (String, DArray)) = {
    s"${tup._1}$Delim${tup._2}"
//    s"${tup._1}$Delim${tup._2.asInstanceOf[DArray].mkString(""+Delim)}}"
  }

  def saveToRackPathAsTextFile(paths: Seq[String]) = {
    import SparkContext._
    import sc._
//    val partedRdd = parent/*.asInstanceOf[OrderedRDDFunctions[K1,V1,(K1,V1)]]*/.repartitionAndSortWithinPartitions(partitioner.get)
    val partedRdd = parent.partitionBy(partitioner.get)
    val cnt = partedRdd.count
    println(s"parted count = $cnt")
//    val convertepd = partedRdd.map { case (k, v) => (k.asInstanceOf[K2], v.asInstanceOf[V2]) }
    val mapped = partedRdd.mapPartitionsWithIndex ({ case (ix, iter) =>
      val dat = iter.toList
      saveToRackPath(paths(ix), dat.asInstanceOf[Seq[(String, DArray)]].map(toRecord).mkString("\n"))
      dat.toIterator
    }, true)
    val cnt2 = mapped.count
    println(s"mapped count = $cnt2")

  }

  override def compute(split: Partition, context: TaskContext): Iterator[KVO] = {
    throw new IllegalStateException("SourceRDD should never be computed")
  }

  def saveToRackPath(path: RackPath, data: String): Unit = {
    info(s"Saving to rackPath $path")
    // TODO: support remote scp
    var f = new File(path.path)
    val dir = if (!f.isDirectory) {
      f.getParentFile
    } else {
      f
    }
    if (!dir.exists) {
      dir.mkdirs
    }
    FileUtils.write(path.path, data)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val lsPartition = split.asInstanceOf[LsRDDPartition[Double]]
    Seq(lsPartition.rackPath.host)
  }

}
