package org.openchai.spark.p2p

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

  def getTrainingParams(clientName: String): GetTrainingParamsResp = {
    val resp = getRpc().request(GetTrainingParamsReq(clientName))
    resp.asInstanceOf[GetTrainingParamsResp]
  }

  def sendEpochResult(er: EpochResult): SendEpochResultResp = {
    val resp = getRpc().request(SendEpochResultReq(er))
    resp.asInstanceOf[SendEpochResultResp]
  }


  def trainEpoch(training: TrainingParams): EpochResult =
    EpochResult(Weights(Array(2, 5), Array.tabulate(10) {
      _ * new Random().nextDouble
    }),
      new Random().nextDouble, new Random().nextDouble)

  def initWeights() = Weights(Array(3, 4), Array.tabulate(12) {
    _ * new Random().nextDouble
  })

  def run(training: TrainingParams, maxLoops: Int) = {
    var n = 0
    var trainParams = getTrainingParams(clientName).value
    if (trainParams.optW.isEmpty) {
      trainParams.optW = Some(initWeights())
    }
    while (keepGoing(n).value) {
      debug(s"About to train with trainingparams=${trainParams.toString}")
      val er = trainEpoch(trainParams)
      trainParams = sendEpochResult(er).value
    }

  }

}

trait ServerIF {
  def service(req: P2pReq[_]): P2pResp[_]
}

class UpdaterServerIF extends ServerIF {

  import UpdaterIF._

  var loops = 0
  val MaxLoops = 4

  override def service(req: P2pReq[_]): P2pResp[_] = req match {
    case o: KeepGoingReq => {
      KeepGoingResp(o.value < MaxLoops)
    }
    case o: GetTrainingParamsReq => {
      GetTrainingParamsResp(TrainingParams(DefaultModel(), DefaultHyperParams(),
        Some(Weights(Array(4, 4), Array.tabulate(16) {
          _ * new Random().nextDouble
        }))))
    }
    case o: SendEpochResultReq => {
      SendEpochResultResp(TrainingParams(DefaultModel(), DefaultHyperParams(),
        Some(Weights(Array(4, 3), Array.tabulate(12) {
          _ * new Random().nextDouble
        }))))
    }
    case _ => throw new IllegalArgumentException(s"Unknown service type ${req.getClass.getName}")
  }
}

object UpdaterIF {
  type Num = Double

  case class Weights(dim: Array[Int], d: Array[Num]) {
    def toString(d: Array[Num], dims: Seq[Int]) = {
      def round(d: Double, p: Int) = (d * Math.pow(10,p)).toLong / Math.pow(10,p)
      if (dims.length != 2) {
        s"Weights: dims=${dims.mkString("[", ",", "]")} data=${d.mkString("[", "", "]")}"
      } else {
        (0 until d.length).foldLeft(s" -- Weights (${dims(0)}x${dims(1)}) --\n") { case (s, ix) =>
          s + (if (ix % dims(1) == 0) "\n" else "") + "\t" + round(d(ix), 2)
        }
      }
    }
    override def toString() = s"Weights: dims=${toString(d,dim)}"
  }

  abstract class Model

  case class DefaultModel() extends Model

  abstract class HyperParams

  case class DefaultHyperParams() extends HyperParams

  case class TrainingParams(model: Model, params: HyperParams, var optW: Option[Weights] = None) {
    def W = {
      optW.getOrElse(throw new IllegalStateException("Weights not set"))
    }
  }

  case class EpochResult(W: Weights, error: Double, accuracy: Double)

  case class GetTrainingParamsReq(val clientName: String) extends P2pReq[String] {
    override def value() = clientName // placeholder
  }

  case class GetTrainingParamsResp(override val value: TrainingParams) extends P2pResp[TrainingParams]

  case class SendEpochResultReq(override val value: EpochResult) extends P2pReq[EpochResult]

  case class SendEpochResultResp(override val value: TrainingParams) extends P2pResp[TrainingParams]

  case class KeepGoingReq(nloops: Int) extends P2pReq[Int] {
    override def value() = nloops
  }

  case class KeepGoingResp(override val value: Boolean) extends P2pResp[Boolean]

}

