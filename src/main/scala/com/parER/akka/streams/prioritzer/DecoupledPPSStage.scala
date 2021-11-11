package com.parER.akka.streams.prioritzer

import akka.stream.stage.{GraphStageLogic, InHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.datastructure.{BaseComparison, Comparison}

class DecoupledPPSStage(name: String, kMax: Int = 10, updateFactor: Int = 1 ) extends PPSStage(name, kMax, 1) {

  override val in = Inlet[List[BaseComparison]]("DecoupledPPSStage.in")
  override val out = Outlet[List[BaseComparison]]("DecoupledPPSStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new PPSGraphStageLogic(kMax, updateFactor, shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          update(grab(in))
          pull(in)
        }
        override def onUpstreamFinish(): Unit = {
          println("========== FILLING FINISHED =============")
          emitRemaining()
          completeStage()
        }
      })
    }
}
