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
package org.openchai.spark.p2p

import java.util.Random

object TestData {
  import UpdaterIF._
  def randArr(size: Int) = Array.tabulate(size) {
    _ * new Random().nextDouble
  }

  def parabolicArr(size: Int) = {
    // y = 1.0 + (0.1 + 0.9*rand)x + (0.01 + 0.1rand)x^2
    val samplesPerPoint = 10
    val npoints = size / samplesPerPoint
    val rand = new Random
    def f(x: Double) = 1.0 + (0.1+0.9*rand.nextFloat)*x + (0.01 + 0.09*rand.nextFloat)*x*x
    Array.tabulate(npoints) { case n =>
      val x = n % samplesPerPoint
      (x, f(x))
    }
  }
  def epochResult(nrows: Int, ncols: Int) = {
    val len = nrows * ncols
    EpochResult(Weights(Array(nrows, ncols), randArr(len)), parabolicArr(len).map(_._2),
      new Random().nextDouble, new Random().nextDouble)
  }

  def mdata(nrows: Int, ncols: Int) = MData("someTag", Seq(nrows, ncols), randArr(nrows * ncols))
}
