package org.openchai.spark.p2p

/**
 *  Weights Updater Interface used for Driver to Worker piecewise sgd updates
 *
 */
class UpdaterIF extends ServiceIF {
//  import reflect.runtime.universe._

  type Num = Double
  type Weights = Array[Array[Num]]

  abstract class Model

  trait HyperParams

  case class TrainingParams(model: Model, params: HyperParams, var optW: Option[Weights] = None) {
    def W = { optW.getOrElse(throw new IllegalStateException("Weights not set")) }
  }

  case class EpochResult(W: Weights, error: Double, accuracy: Double)

  case class GetTrainingParamsReq(clientName: String) extends P2pReq[Unit] {
    override def value() = Unit // placeholder
  }

  case class GetTrainingParamsResp(override val value: TrainingParams) extends P2pResp[TrainingParams]

  case class SendEpochResultReq(override val value: EpochResult) extends P2pReq[EpochResult]

  case class SendEpochResultResp(override val value: EpochResult) extends P2pResp[EpochResult]

  case class KeepGoingReq(nloops: Int) extends P2pReq[Int] {
    override def value() = nloops
  }

  case class KeepGoingResp(override val value: Boolean) extends P2pResp[Boolean]

//  var nLoops = 0
  def keepGoing(nLoops: Int): Boolean = {
    val resp = rpc.request(KeepGoingReq(nLoops))
//    nLoops += 1
    resp.value
  }

  def getTrainingParams(clientName: String): TrainingParams = {
    val resp = rpc.request(GetTrainingParamsReq(clientName))
    resp.value
  }

  def sendEpochResult(er: EpochResult): TrainingParams = {
    val resp = rpc.request(SendEpochResultReq(er))
    resp.value
  }


  def trainEpoch(training: TrainingParams): EpochResult = ???

  def initWeights() = ???
  
  def run(training: TrainingParams, maxLoops: Int) = {
    var n = 0
    var params = getTrainingParams(clientName)
    if (params.optW.isEmpty) {
      params.optW = Some(initWeights())
    }
    while (keepGoing(n)) {
      val er = trainEpoch(params)
      params = sendEpochResult(er)
    }

  }

}
