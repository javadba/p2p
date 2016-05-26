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

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.proximal.QpGenerator
import breeze.optimize.{DiffFunction, ProjectedQuasiNewton}
import org.openchai.spark.util.Logger._

import scala.util.Random

/**
 * Weights Updater Interface used for Driver to Worker piecewise sgd updates
 *
 */
class UpdaterIF extends ServiceIF {

  import UpdaterIF._

  def keepGoing(nLoops: Int): KeepGoingResp = {
    val resp = getRpc().request(KeepGoingReq(nLoops))
    resp.asInstanceOf[KeepGoingResp]
  }

  def getModelParams(clientName: String): GetModelParamsResp = {
    val resp = getRpc().request(GetModelParamsReq(clientName))
    resp.asInstanceOf[GetModelParamsResp]
  }

  def sendEpochResult(er: EpochResult): SendEpochResultResp = {
    val resp = getRpc().request(SendEpochResultReq(er))
    resp.asInstanceOf[SendEpochResultResp]
  }

  def trainEpoch(modelParams: ModelParams, data: AnyData): EpochResult = {
    val model = modelParams.model
    model.update(modelParams.params, modelParams.weights)
    val eresult = model.compute(data)
    eresult
  }

  def initWeights(dims: Seq[Int]) = Weights(Array(dims(0), dims(1)), Array.tabulate(dims(0) * dims(1)) {
    _ * new Random().nextDouble
  })

  def run(training: ModelParams, data: MData, maxLoops: Int) = {
    var n = 0
    var trainParams = getModelParams(clientName).value
    if (trainParams.optW.isEmpty) {
      trainParams.optW = Some(initWeights(Seq(3, 4)))
    }
    while (keepGoing(n).value) {
      debug(s"About to train with ModelParams=${trainParams.toString}")
      val er = trainEpoch(trainParams, data)
      trainParams = sendEpochResult(er).value
      n += 1
    }
    info(s"We were told to *not* keep going for n=$n")
  }

}

object UpdaterIF {
  type Num = Double

  case class Weights(dims: Seq[Int], d: Array[Num]) {
    def toString(d: Array[Num], dims: Seq[Int]) = {
      val PrintLen = 100
      def round(d: Double, p: Int) = (d * Math.pow(10, p)).toLong / Math.pow(10, p)
      val prdat = if (d.length <= PrintLen) d else {
        val smalld = new Array[Double](PrintLen)
        System.arraycopy(d, 0, smalld, 0, PrintLen)
        smalld
      }
      if (dims.length != 2) {
        s"Weights: dims=${dims.mkString("[", ",", "]")} data=${prdat.mkString("[", "", "]")}"
      } else {
        prdat.indices.foldLeft(s" -- Weights (${dims(0)}x${dims(1)}) --\n") { case (s, ix) =>
          s + (if (ix % dims(1) == 0) "\n" else "") + "\t" + round(d(ix), 2)
        }
      }
    }

    override def toString() = toString(d, dims)
   }

  abstract class Model[T <: MData] {
    def compute(data: T): EpochResult = ???

    def update(params: HyperParams, weights: Weights): Model[T] = ???

  }

  case class DefaultModel() extends Model[MData] {

    import breeze.linalg._

    val UseLbfgs = true

    val errors = Array.tabulate(20){ d => Math.max(15 - d*0.97,0.001) }
    var iteration = 0
    override def compute(data: MData): EpochResult = {

      val dv = DenseVector(data.toArray)
      implicit class MathOps(x: Double) {
        def :^(y: Double) = Math.pow(x, y)
      }
      val (err, state) = if (UseLbfgs) {
        import breeze.optimize.LBFGS
        def computeObjective(h: DenseMatrix[Double], q: DenseVector[Double], x: DenseVector[Double]): Double = {
          val res = (x.t * h * x) * 0.5 + q.dot(x)
          res
        }

        case class Cost(H: DenseMatrix[Double],
          q: DenseVector[Double]) extends DiffFunction[DenseVector[Double]] {
          def calculate(x: DenseVector[Double]) = {
            (computeObjective(H, q, x), H * x + q)
          }
        }

        def getCost(H: DenseMatrix[Double], q: DenseVector[Double]) =  Cost(H, q)

        def optimizeWithLBFGS(init: DenseVector[Double],
          H: DenseMatrix[Double],
          q: DenseVector[Double]) = {
          val lbfgs = new LBFGS[DenseVector[Double]](-1, 7)
          val lstate = lbfgs.minimizeAndReturnState(getCost(H,q), init)
          lstate
        }

        val problemSize = 1000
        val nequalities = 30

        println(s"Generating randomized QPs with rank ${problemSize} equalities ${nequalities}")
        val (aeq, b, bl, bu, q, h) = QpGenerator(problemSize, nequalities)

        println(s"Test QuadraticMinimizer, CG , BFGS and OWLQN with $problemSize variables and $nequalities equality constraints")

        val ostate = optimizeWithLBFGS(dv,h,q)
        val cost = getCost(h,q)
        val error = errors(iteration) // new Random.computeObjective(cost.H, cost.q,
        iteration += 1
        (error,ostate)

      } else {
        val optimizer = new ProjectedQuasiNewton(tolerance = 1.0E-5)
        val f = new DiffFunction[DenseVector[Double]] {
          def calculate(x: DenseVector[Double]) = {
            (norm((x - 3.0) :^ 2.0, 1), (x * 2.0) - 6.0)
          }
        }
        val state = optimizer.minimizeAndReturnState(f, dv)
        val error = errors(iteration) // new Random.computeObjective(cost.H, cost.q,
        iteration += 1
        (error, state)
      }
      EpochResult(Weights(data.dims, state.x.toArray), state.fVals.toArray, err, Math.max(0.01, 1 - err/15.0))
    }

    override def update(params: HyperParams, weights: Weights): Model[MData] = {
      this
    }
  }

  abstract class HyperParams

  case class DefaultHyperParams() extends HyperParams

  case class ModelParams(model: Model[MData], params: HyperParams, var optW: Option[Weights] = None) {
    def weights = {
      optW.getOrElse(throw new IllegalStateException("Weights not set"))
    }
  }

  case class EpochResult(W: Weights, errors: DArray, totalError: Double, accuracy: Double)

  case class GetModelParamsReq(val clientName: String) extends P2pReq[String] {
    override def value() = clientName // retain the clientName parameter so we know what the value signifies
  }

  case class GetModelParamsResp(override val value: ModelParams) extends P2pResp[ModelParams]

  case class SendEpochResultReq(override val value: EpochResult) extends P2pReq[EpochResult]

  case class SendEpochResultResp(override val value: ModelParams) extends P2pResp[ModelParams]

  case class KeepGoingReq(nloops: Int) extends P2pReq[Int] {
    override def value() = nloops
  }

  case class KeepGoingResp(override val value: Boolean) extends P2pResp[Boolean]

}

