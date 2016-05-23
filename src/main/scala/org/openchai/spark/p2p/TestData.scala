package org.openchai.spark.p2p

import java.util.Random

object TestData {
  import UpdaterIF._
  def randArr(size: Int) = Array.tabulate(size) {
    _ * new Random().nextDouble
  }

  def epochResult(nrows: Int, ncols: Int) = {
    val len = nrows * ncols
    EpochResult(Weights(Array(nrows, ncols), randArr(len)), /* MData(Seq(nrows, ncols), */ randArr(len) /*)*/,
      new Random().nextDouble, new Random().nextDouble)
  }

  def mdata(nrows: Int, ncols: Int) = MData(Seq(nrows, ncols), randArr(nrows * ncols))
}
