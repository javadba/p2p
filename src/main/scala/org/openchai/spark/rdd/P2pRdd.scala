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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.openchai.spark.p2p.UpdaterIF.{DefaultHyperParams, DefaultModel, ModelParams}
import org.openchai.spark.p2p._

import scala.reflect.ClassTag

class P2pRDD[KVO:ClassTag,T:ClassTag](sc: SparkContext, parent: RDD[KVO], p2pParams: P2pConnectionParams)
  extends RDD[T](parent) {

  TcpServer.startServer(TcpServer.TestPort)
  var testingSize : Int = 1000
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val updaterIF =  new UpdaterIF
    val p2pClient = new TcpClient(p2pParams.asInstanceOf[TcpConnectionParams], updaterIF)
    val dat = parent.compute(split, context)
    val converted = dat.map { case (path, idAndData) =>
      (path.asInstanceOf[String], idAndData.asInstanceOf[String].split(LsRDD.Delim).tail.map(_.toDouble))
    }.toList
    val grouped = converted.groupBy(_._1)

    import collection.mutable
    // TODO: Throw warning or exception if size mismatch
    val bigarr = converted.map(_._2).foldLeft(mutable.ArrayBuffer[Double]()) { case (buf, darr) =>
      buf ++ mutable.ArrayBuffer[Double](darr.asInstanceOf[Array[Double]]:_*)
    }.toArray.slice(0,testingSize)
    val iter = updaterIF.run(ModelParams(new DefaultModel(), new DefaultHyperParams()),
      MData("somepath",Seq(2,5),bigarr),3)
    iter.asInstanceOf[Iterator[T]]
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}

object P2pRDD {
  case class LabeledVector(v: Vector, label: Double)
}
