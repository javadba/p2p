package org.openchai.spark.rdd

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkContext, TaskContext, Partition}
import org.apache.spark.rdd.RDD
import org.openchai.spark.p2p.UpdaterIF.{DefaultHyperParams, DefaultModel, ModelParams}
import org.openchai.spark.p2p._

import reflect.runtime.universe._
import scala.reflect.ClassTag

class P2pRDD[T /*<: Serializable */: ClassTag](sc: SparkContext, parent: RDD[T], p2pParams: P2pConnectionParams)
  extends RDD[T](parent) {
  def asyncConvergence(tuple: (Vector, Double)) = ???

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val updaterIF =  new UpdaterIF
    val p2pClient = new TcpClient(p2pParams.asInstanceOf[TcpConnectionParams], updaterIF)
    val dat = parent.compute(split, context).toList
    val iter = updaterIF.run(ModelParams(new DefaultModel(), new DefaultHyperParams()),
      dat.toArray.asInstanceOf[AnyData],3)
    iter.asInstanceOf[Iterator[T]]
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}

object P2pRDD {
  case class LabeledVector(v: Vector, label: Double)
}
