package org.openchai.spark.rdd

import java.io.File
import java.net.InetAddress

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{OrderedRDDFunctions, PairRDDFunctions, RDD}
import org.apache.spark.{Partitioner, Partition, SparkContext, TaskContext}
import org.openchai.spark.util.FileUtils

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class LsRDDPartition[T](override val index: Int, rackPath: RackPath) extends Partition

trait LsRDD[K, V] extends Serializable {

  import LsRDD._
  import reflect.runtime.universe._

  def paths(): Seq[String]

  def rackPaths = paths().map { p => RackPath(p) }.sortBy { p => p.fullPath }

  type KV[A, B] = (A, B)
  type KVO = KV[K, V]

  def rackPathsToPartitions[T] = rackPaths.zipWithIndex.map{ case (r,x) =>
    LsRDDPartition[T](x,r)}.toArray // .asInstanceOf[Array[Partition]]

  protected[rdd] var parts: Array[Partition] = _
}

case class LsSinkRDD[K1: TypeTag, V1: TypeTag, K2: TypeTag, V2: TypeTag](@transient sc: SparkContext, var paths: Seq[String],
  parent: RDD[(K1, V1)])(implicit evk1: TypeTag[K1], evv1: TypeTag[V1], evk2: TypeTag[K2], evv2: TypeTag[V2])
  extends RDD[(K2, V2)](parent) with LsRDD[K2, V2] {

  import LsRDD._
  import reflect.runtime.universe._

  //  implicit val ttype = ev.tpe

  //  val tClass = /* ev.getClass */ typeTag[T].getClass

  override val partitioner /*: LsRDDPartitioner[K2,V2]*/ = Some(new LsRDDPartitioner[K2,V2](parts.asInstanceOf[Seq[LsRDDPartition[V2]]]))

  override def saveAsTextFile(rackPathsSeparatedByCommas: String): Unit = {
    saveToRackPathAsTextFile(rackPathsSeparatedByCommas.split(","))
  }

  def toRecord(tup: (String, DArray)) = {
    s"${tup._1}$Delim${tup._2.asInstanceOf[DArray].mkString(""+Delim)}}"
  }

  def saveToRackPathAsTextFile(paths: Seq[String]) = {
    //    val partRdd = parent.partitionBy(partitioner).repartitionAndSortWithinPartitions(
    val partedRdd = parent.asInstanceOf[OrderedRDDFunctions[K1,V1,(K1,V1)]].repartitionAndSortWithinPartitions(partitioner.get)
    parts = partedRdd.partitions
    //    val conv = converter[String, U](outClass.getClass.asInstanceOf[Class[U]], _: String)
    //      d.asInstanceOf[String] /*.split("\n")*/ .map(conv)
    //    }.toList.mkString("\n")
    val converted = partedRdd.map { case (k, v) => (k.asInstanceOf[K2], v.asInstanceOf[V2]) }
    converted.mapPartitionsWithIndex { case (ix, iter) =>
      val dat = iter.toList
      saveToRackPath(paths(ix), dat.asInstanceOf[Seq[(String, DArray)]].map(toRecord).mkString("\n"))
      dat.toIterator
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[KVO] = {

    //    val outClass = typeTag[U].mirror.runtimeClass(typeOf[U]).asInstanceOf[U]
    //    //      val conv = converter[T,U](outClass.getClass.asInstanceOf[Class[U]], _: T)
    //    val conv = converter[String, U](outClass.getClass.asInstanceOf[Class[U]], _: String)
    //    val converted = parent.compute(split, context).map { case d =>
    //      d.asInstanceOf[String] /*.split("\n")*/ .map(conv)
    //    }.toList.mkString("\n")
//    saveToRackPathAsTextFile(paths)
    null
  }

  //  def saveToRackPath(path: RackPath, data: Array[Byte]): Unit = {
  def saveToRackPath(path: RackPath, data: String): Unit = {
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
    tools.nsc.io.File(path.path).writeAll(data)
  }

  override protected def getPartitions: Array[Partition] = parts

  //  override protected def getPartitions: Array[Partition] = {
  //    paths.zipWithIndex.map{ case(p,x) => LsPartition(x,p) }.toArray
  //  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val lsPartition = split.asInstanceOf[LsRDDPartition[Double]]
    Seq(lsPartition.rackPath.host)
  }

  //  override protected def getPreferredLocations(split: Partition): Seq[String] = {
  //    val mypart = split.asInstanceOf[LsPartition[Int]]
  //    Seq(mypart.host)

}

case class LsSourceRDD[K1: TypeTag, V1: TypeTag, K2: TypeTag, V2: TypeTag](@transient sc: SparkContext, var paths: Seq[String])(implicit evk1: TypeTag[K1], evv1: TypeTag[V1], evk2: TypeTag[K2], evv2: TypeTag[V2])
  extends RDD[(K2, V2)](sc, Nil) with LsRDD[K2, V2] {

  import LsRDD._
  import RackPath._
  import reflect.runtime.universe._

  override def compute(split: Partition, context: TaskContext): Iterator[(K2, V2)] = {
    val part = split.asInstanceOf[LsRDDPartition[Int]]
    val d = readFromRackPath(part.rackPath)
    //    val conv = converter(newType.newInstance, ev.newInstance) _
    d.split("\n").map(l => (part.rackPath.host, l)).iterator.asInstanceOf[Iterator[(K2, V2)]]
  }

  def readFromRackPath(rp: RackPath) = {
    val host = rp.host
    val sourceHost = host match {
      case h if InetAddress.getLocalHost.getHostName.equals(host) => h
      case _ => RackPath.hostToRack(host)
    }
    // TODO: st up scp read from the host
    val d = FileUtils.readPath(rp.path)
    d
  }

  override protected def getPartitions: Array[Partition] =
    rackPathsToPartitions[V2].asInstanceOf[Array[Partition]]

  //  override protected def getPartitions: Array[Partition] = {
  //    paths.zipWithIndex.map{ case(p,x) => LsPartition(x,p) }.toArray
  //  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val lsPartition = split.asInstanceOf[LsRDDPartition[Double]]
    Seq(lsPartition.rackPath.host)
  }

  //  override protected def getPreferredLocations(split: Partition): Seq[String] = {
  //    val mypart = split.asInstanceOf[LsPartition[Int]]
  //    Seq(mypart.host)

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

    //    val outClass = typeTag[U].mirror.runtimeClass(typeOf[U])

    val out = outClass match {
      //    val out = tClass match {
      case x if x==string => in
      case x if x==double => in
      case x if x==darr => in.split(Delim).map(_.toDouble)
      case x if x == labeledarr => {
        val headTail = in.split(Delim).splitAt(1)
        (headTail._1.head, headTail._2.map(_.toDouble).splitAt(1))
      }
      case _ => throw new IllegalArgumentException(s"Type $outClass not supported")
    }
    out.asInstanceOf[U]
  }

}

class LsRDDPartitioner[K,V](parts: Seq[LsRDDPartition[V]]) extends Partitioner {

  import collection.mutable

  //rackPaths: Seq[RackPath]
  override def numPartitions: Int = parts.length

  val sorted = parts.sortBy(_.rackPath.fullPath).zipWithIndex
  val sortedMap = sorted.foldLeft(mutable.HashMap[String, Int]()) { case (h, (p, x)) =>
    h(p.rackPath.fullPath) = x
    h
  }

  override def getPartition(key: Any): Int = {
    sortedMap(key.asInstanceOf[String])
  }
}