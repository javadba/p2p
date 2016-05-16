package org.openchai.spark.rdd

import java.net.InetAddress
import java.util.Scanner

import org.apache.spark.{SparkContext, TaskContext, Partition}
import org.apache.spark.rdd.RDD

case class RackPath(host: String, rack: String, path: String) {
  val fullPath = RackPath.toFullPath(host,rack,path)
}

object RackPathTester {
  def hostToRack(host: String) = {
    s"rack${hostToPartitionIndex(host)}"
  }

  def hostToPartitionIndex(host: String) = {
    host match {
      case h if h <= "d" => 1
      case h if h <= "l" => 2
      case _ => 3
    }
  }

  def hostsInRack(hostnames: Seq[String], rack: String) = {

  }
}

object RackPath {
  def toFullPath(host: String, rack: String, path: String) = s"$host:$rack:$path"

  def apply(fullPath: String) =
    new RackPath(
      fullPath.substring(0, fullPath.indexOf('.')),
      fullPath.substring(fullPath.indexOf('.') + 1, fullPath.lastIndexOf('.')),
      fullPath.substring(fullPath.lastIndexOf('.')))

  def hostToRack(host: String) = RackPathTester.hostToRack(host)
}

case class LsPartition[T /*<: Numeric[T]*/](override val index: Int,rackPath: RackPath) extends Partition

import reflect.ClassTag
class LsRdd[T: ClassTag /*<: Numeric[T]*/ /*: TypeTag*/](sc: SparkContext, parent: RDD[T], paths: Seq[String] )
  extends RDD[T](parent) {

  val rackPaths = paths.map { p => RackPath(p) }.sortBy{ p => p.fullPath }
  val lsPartitions = rackPaths.zipWithIndex.map { case (rp, x) => LsPartition[Double](x,rp) }.toArray

  def readFile(fpath: String) = {
    val content = new Scanner(fpath).useDelimiter("\\Z").next()
    content
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val part = split.asInstanceOf[LsPartition[Int]]
    val d = readFromRackPath(part.rackPath)
    d.split("\n").iterator.asInstanceOf[Iterator[T]]
  }

  override protected def getPartitions: Array[Partition] = lsPartitions.asInstanceOf[Array[Partition]]

  def readFromRackPath(rp: RackPath) = {
    val host = rp.host
    val sourceHost = host match {
      case h if InetAddress.getLocalHost.getHostName.equals(host) => h
      case _ => RackPath.hostToRack(host)
    }
    val d = readFile(rp.path)
    d
  }

  //  override protected def getPartitions: Array[Partition] = {
  //    paths.zipWithIndex.map{ case(p,x) => LsPartition(x,p) }.toArray
  //  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val lsPartition = split.asInstanceOf[LsPartition[Double]]
    Seq(lsPartition.rackPath.host)
  }

  //  override protected def getPreferredLocations(split: Partition): Seq[String] = {
  //    val mypart = split.asInstanceOf[LsPartition[Int]]
  //    Seq(mypart.host)


}


