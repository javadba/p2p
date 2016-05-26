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

import org.apache.spark.mllib.linalg.{Vector => SparkVector, SparseVector, DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.MLProxy._
import org.openchai.spark.p2p
import org.openchai.spark.rdd.{LsRDD, P2pRDD}

object MllibImports {

  import org.apache.spark.mllib.optimization._

  /**
   * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   */

  import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
  import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

  import LsRDD._
  class AsyncConvergenceCostFun(
    data: P2pRDD[(String,String),MData],
    gradient: Gradient,
    updater: Updater,
    regParam: Double,
    numExamples: Long) extends DiffFunction[BDV[Double]] {

    /**
     * Creates a vector instance from a breeze vector.
     */
    private[spark] def fromBreeze(breezeVector: BV[Double]): SparkVector = {
      breezeVector match {
        case v: BDV[Double] =>
          if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
            new DenseVector(v.data)
          } else {
            new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
          }
        case v: BSV[Double] =>
          if (v.index.length == v.used) {
            new SparseVector(v.length, v.index, v.data)
          } else {
            new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
          }
        case v: BV[_] =>
          sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
      }
    }

    override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val w = fromBreeze(weights)
      val n = w.size
      val bcW = data.context.broadcast(w)
      val localGradient = gradient

      import org.apache.spark.ml.classification.RandomForestClassificationModel

      val (gradientSum, lossSum) = (new DenseVector(List(1.0).toArray),2.0) // data.asyncConvergence((Vectors.zeros(n), 0.0))(
//        seqOp = (c, v) => (c, v) match {
//          case ((grad, loss), (label, features)) =>
//            val l = localGradient.compute(
//              features, label, bcW.value, grad)
//            (grad, loss + l)
//        },
//        combOp = (c1, c2) => (c1, c2) match {
//          case ((grad1, loss1), (grad2, loss2)) =>
//            axpy(1.0, grad2, grad1)
//            (grad1, loss1 + loss2)
//        }
//    )

      /**
       * regVal is sum of weight squares if it's L2 updater;
       * for other updater, the same logic is followed.
       */
      val regVal = updater.compute(w, Vectors.zeros(n), 0, 1, regParam)._2

      val loss = lossSum / numExamples + regVal
      /**
       * It will return the gradient part of regularization using updater.
       *
       * Given the input parameters, the updater basically does the following,
       *
       * w' = w - thisIterStepSize * (gradient + regGradient(w))
       * Note that regGradient is function of w
       *
       * If we set gradient = 0, thisIterStepSize = 1, then
       *
       * regGradient(w) = w - w'
       *
       * TODO: We need to clean it up by separating the logic of regularization out
       * from updater to regularizer.
       */
      // The following gradientTotal is actually the regularization part of gradient.
      // Will add the gradientSum computed from the data with weights in the next step.
      val gradientTotal = w.copy
      axpy(-1.0, updater.compute(w, Vectors.zeros(n), 1, 1, regParam)._1, gradientTotal)

      // gradientTotal = gradientSum / numExamples + gradientTotal
      axpy(1.0 / numExamples, gradientSum, gradientTotal)

      (loss, toBreeze(gradientTotal).asInstanceOf[BDV[Double]])
    }
  }

  def toBreeze(values: SparkVector): BV[Double] =
    values match {
      case values: SparseVector => new BSV[Double](values.indices, values.toArray, values.size)
      case values: DenseVector => new BDV[Double](values.toArray)
    }

}

