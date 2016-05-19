package org.openchai.spark.p2p

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.proximal.QpGenerator
import breeze.optimize.{DiffFunction, ProjectedQuasiNewton}
import org.apache.spark.ml.MLProxy._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.rdd.RDD
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

  def run(training: ModelParams, data: AnyData, maxLoops: Int) = {
    var n = 0
    var trainParams = getModelParams(clientName).value
    if (trainParams.optW.isEmpty) {
      trainParams.optW = Some(initWeights(Seq(3, 4)))
    }
    while (keepGoing(n).value) {
      debug(s"About to train with ModelParams=${trainParams.toString}")
      val er = trainEpoch(trainParams, data)
      trainParams = sendEpochResult(er).value
    }
    new Iterator[Double]() {
      override def hasNext: Boolean = false

      override def next(): Num = ???
    }

  }

}

trait ServerIF {
  def service(req: P2pReq[_]): P2pResp[_]
}

object UpdaterServerIF {
  val WeightsMergePolicies = Seq("average", "best")
}

class UpdaterServerIF(weightsMergePolicy: String) extends ServerIF {

  import UpdaterIF._
  import collection.mutable

  var loops = 0
  val MaxLoops = 4

  override def service(req: P2pReq[_]): P2pResp[_] = {
    var curWeights: DArray = null
    var curAccuracy = -1.0
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
        var (curAccuracy1, curWeights1) = {
          if (weightsMergePolicy == "best") {
            if (epochResult.accuracy > curAccuracy) {
              (epochResult.accuracy, Some(epochResult))
            } else {
              (curAccuracy, curWeights)
            }
          } else {
            val sum = allResults.map(x => new BDV[Double](x.W.d))
              .foldLeft(new BDV[Double](Array.fill(allResults.head.W.d.length)(0.0))) { case (sum, bdv) => sum + bdv }
            val avg = sum :/ allResults.length.toDouble
            (allResults.map(_.accuracy).sum / allResults.length, avg)
          }
        }
        SendEpochResultResp(ModelParams(DefaultModel(), DefaultHyperParams(), Some(Weights(epochResult.W.dims, curWeights))
          //          Some(Weights(Array(4, 3), Array.tabulate(12) {
          //            _ * new Random().nextDouble }))
        ))
      }
      case _ => throw new IllegalArgumentException(s"Unknown service type ${req.getClass.getName}")
    }
  }
}

object UpdaterIF {
  type Num = Double

  case class Weights(dims: Seq[Int], d: Array[Num]) {
    def toString(d: Array[Num], dims: Seq[Int]) = {
      def round(d: Double, p: Int) = (d * Math.pow(10, p)).toLong / Math.pow(10, p)
      if (dims.length != 2) {
        s"Weights: dims=${dims.mkString("[", ",", "]")} data=${d.mkString("[", "", "]")}"
      } else {
        (0 until d.length).foldLeft(s" -- Weights (${dims(0)}x${dims(1)}) --\n") { case (s, ix) =>
          s + (if (ix % dims(1) == 0) "\n" else "") + "\t" + round(d(ix), 2)
        }
      }
    }

    override def toString() = s"Weights: dims=${toString(d, dims)}"
  }


  abstract class Model[T <: AnyData] {
    def compute(data: T): EpochResult = ???

    //    def train(data: T): Model[T] = ???
    //
    def update(params: HyperParams, weights: Weights): Model[T] = ???

  }


  case class DefaultModel() extends Model[MData] {

    import breeze.linalg._
    import breeze.numerics._

    val UseLbfgs = true

    override def compute(data: MData): EpochResult = {
      //      epochResult(data.dims(0), data.dims(1))

      val dv = DenseVector(data.toArray)
      implicit class MathOps(x: Double) {
        def :^(y: Double) = Math.pow(x, y)
      }
      val state = if (UseLbfgs) {
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

        def optimizeWithLBFGS(init: DenseVector[Double],
          H: DenseMatrix[Double],
          q: DenseVector[Double]) = {
          val lbfgs = new LBFGS[DenseVector[Double]](-1, 7)
          val lstate = lbfgs.minimizeAndReturnState(Cost(H, q), init)
          lstate
        }

        val problemSize = 1000
        val nequalities = 30

        println(s"Generating randomized QPs with rank ${problemSize} equalities ${nequalities}")
        val (aeq, b, bl, bu, q, h) = QpGenerator(problemSize, nequalities)

        println(s"Test QuadraticMinimizer, CG , BFGS and OWLQN with $problemSize variables and $nequalities equality constraints")


        optimizeWithLBFGS(dv,h,q)

      } else {
        val optimizer = new ProjectedQuasiNewton(tolerance = 1.0E-5)
        val f = new DiffFunction[DenseVector[Double]] {
          def calculate(x: DenseVector[Double]) = {
            (norm((x - 3.0) :^ 2.0, 1), (x * 2.0) - 6.0)
          }
        }
        val state = optimizer.minimizeAndReturnState(f, dv)
        state
      }
      EpochResult(Weights(data.dims, state.x.toArray), state.fVals.toArray, state.value, state.value)
      //      null
    }

    override def update(params: HyperParams, weights: Weights): Model[MData] = {
      this
    }
  }

  abstract class HyperParams

  case class DefaultHyperParams() extends HyperParams

  import reflect.runtime.universe.TypeTag

  case class ModelParams /*[T <: AnyData : TypeTag]*/ (model: Model[MData], params: HyperParams, var optW: Option[Weights] = None) {
    def weights = {
      optW.getOrElse(throw new IllegalStateException("Weights not set"))
    }
  }

  case class EpochResult(W: Weights, errors: DArray, totalError: Double, accuracy: Double)

  case class GetModelParamsReq(val clientName: String) extends P2pReq[String] {
    override def value() = clientName // placeholder
  }

  case class GetModelParamsResp(override val value: ModelParams) extends P2pResp[ModelParams]

  case class SendEpochResultReq(override val value: EpochResult) extends P2pReq[EpochResult]

  case class SendEpochResultResp(override val value: ModelParams) extends P2pResp[ModelParams]

  case class KeepGoingReq(nloops: Int) extends P2pReq[Int] {
    override def value() = nloops
  }

  case class KeepGoingResp(override val value: Boolean) extends P2pResp[Boolean]

}

