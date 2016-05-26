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
import org.openchai.spark.util.Logger
import Logger._
import breeze.linalg.{DenseVector => BDV}

object UpdaterServerIF {
  val WeightsMergePolicies = Seq("average", "best")
}

class UpdaterServerIF(weightsMergePolicy: String) extends ServerIF {

  import UpdaterIF._

  import collection.mutable

  var loops = 0
  val MaxLoops = 20

  var curWeightsAndAccuracy: (DArray, Double) = (null,-1.0)
  override def service(req: P2pReq[_]): P2pResp[_] = {
    val allResults = new mutable.ArrayBuffer[EpochResult]()
    req match {
      case o: KeepGoingReq => {
        KeepGoingResp(o.value < MaxLoops)
      }
      case o: GetModelParamsReq => {
        GetModelParamsResp(ModelParams(DefaultModel(), DefaultHyperParams(),
          Some(Weights(Array(4, 4), Array.tabulate(16) {
            _ * new Random().nextDouble
          }))))
      }
      case o: SendEpochResultReq => {
        val epochResult = o.value
        allResults += epochResult
        curWeightsAndAccuracy = {
          if (weightsMergePolicy == "best") {
            if (epochResult.accuracy > curWeightsAndAccuracy._2) {
              info(s"Found best: accuracy = ${epochResult.accuracy}")
              (epochResult.W.d, epochResult.accuracy)
            } else {
              debug("Sorry we're worse .. skipping..")
              curWeightsAndAccuracy
            }
          } else {
            val sum = allResults.map(x => new BDV[Double](x.W.d))
              .foldLeft(new BDV[Double](Array.fill(allResults.head.W.d.length)(0.0))) { case (sum, bdv) => sum + bdv }
            val avg = sum :/ allResults.length.toDouble
            (avg.toArray, allResults.map(_.accuracy).sum / allResults.length)
          }
        }
        SendEpochResultResp(ModelParams(DefaultModel(), DefaultHyperParams(), Some(Weights(epochResult.W.dims, curWeightsAndAccuracy._1))
        ))
      }
      case _ => throw new IllegalArgumentException(s"Unknown service type ${req.getClass.getName}")
    }
  }
}

