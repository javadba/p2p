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

  def epochResult(nrows: Int, ncols: Int) = {
    val len = nrows * ncols
    EpochResult(Weights(Array(nrows, ncols), randArr(len)), /* MData(Seq(nrows, ncols), */ randArr(len) /*)*/,
      new Random().nextDouble, new Random().nextDouble)
  }

  def mdata(nrows: Int, ncols: Int) = MData(Seq(nrows, ncols), randArr(nrows * ncols))
}
