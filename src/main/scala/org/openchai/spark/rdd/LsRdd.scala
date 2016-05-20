package org.openchai.spark.rdd

import java.io.{FileOutputStream, File}
import java.net.InetAddress
import java.util.Scanner

import org.apache.spark.{SparkContext, TaskContext, Partition}
import org.apache.spark.rdd.RDD
import org.openchai.spark.p2p.TcpServer
import org.openchai.spark.util.{TcpUtils, Launcher}

case class RackPath(host: String, rack: String, path: String) {
  val fullPath = RackPath.toFullPath(host, rack, path)
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

case class LsRDDPartition[T](override val index: Int, rackPath: RackPath) extends Partition

import reflect.runtime.universe.TypeTag
import reflect.ClassTag

class LsRDD[T: ClassTag](sc: SparkContext, parent: RDD[T], var paths: Seq[String])(implicit ev: TypeTag[T])
  extends RDD[T](parent) {

  implicit val ttype = ev.tpe

  import reflect.runtime.universe._

  val tClass = typeTag[T].getClass

  def rackPaths() = paths.map { p => RackPath(p) }.sortBy { p => p.fullPath }

  def readFile(fpath: String) = {
    val content = new Scanner(fpath).useDelimiter("\\Z").next()
    content
  }

  def converter(in: String): T = {
    val string = classOf[String]
    val double = classOf[Double]
    val darr = classOf[Array[Double]]
    val labeledarr = classOf[(Double, Array[Double])]

    tClass match {
      case string => in.asInstanceOf[T]
      case double => in.toDouble.asInstanceOf[T]
      case darr => in.split(" ").map(_.toDouble).toArray.asInstanceOf[T]
      case labeledarr => {
        val headTail = in.split(" ").map(_.toDouble).splitAt(1)
        (headTail._1.head, headTail._2).asInstanceOf[T]
      }
      case _ => throw new IllegalArgumentException(s"Type $tClass not supported")
    }
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

  override def saveAsTextFile(rackPathsSeparatedByCommas: String): Unit = {
    val rpaths = rackPathsSeparatedByCommas.split(",")
    paths = rpaths
    // TODO:
    //   Ensure dataset has (K,v)
    //   use a custom Partitioner
    //   invoke new ShuffledRDD with the custom partitioner
//    this.repartition(paths.length)
//    this.mapPartitions { iter =>
//      val data = iter.toList
//      saveToRackPath(
//    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val part = split.asInstanceOf[LsRDDPartition[Int]]
    val d = readFromRackPath(part.rackPath)
    d.split("\n").map(converter).iterator
  }

  override protected def getPartitions: Array[Partition] =
    rackPaths.zipWithIndex.map { case (rp, x) => LsRDDPartition[T](x, rp) }.toArray.asInstanceOf[Array[Partition]]

  def readFromRackPath(rp: RackPath) = {
    val host = rp.host
    val sourceHost = host match {
      case h if InetAddress.getLocalHost.getHostName.equals(host) => h
      case _ => RackPath.hostToRack(host)
    }
    // TODO: st up scp read from the host
    val d = readFile(rp.path)
    d
  }

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
  def rddTest(master: String, appArgs: Array[String]) = {
    val dir = "/data/lsrdd"
    val dirs = new File(dir).listFiles.filter(f => f.isDirectory && !f.getName.startsWith("."))
    val rpaths = dirs.map(d => s"${TcpUtils.getLocalHostname}:${RackPath.hostToRack(TcpUtils.getLocalHostname)}:$d")
      val sc = new SparkContext(master, "LsRddTest")
    val lsrdd = new LsRDD(sc, null, rpaths)
    lsrdd.count
  }
  def main(args: Array[String]) {
    if (args(0) == "launched") {
      val master = args(1)
      val appArgs = if (args.length > 2) args.slice(2, args.length) else new Array[String](0)
      println(s"Calling rddTest with master=$master and appArgs=${appArgs.mkString(" ")}")
      rddTest(master, appArgs)

    } else {
     // ::
      // first arg is "launcher" and should be removed
      Launcher(classOf[LsRDD[_]].getName, "launched" +: args.toSeq)
    }

  }

}


