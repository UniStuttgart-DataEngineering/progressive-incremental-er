package com.parER.akka.streams.prioritzer

import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.datastructure.{BaseComparison, Comparison}

class PPSStage(name: String, kMax: Int = 10, updateFactor: Int = 1 ) extends GraphStage[FlowShape[List[BaseComparison], List[BaseComparison]]] {
  val in = Inlet[List[BaseComparison]]("PPSStage.in")
  val out = Outlet[List[BaseComparison]]("PPSStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new PPSGraphStageLogic(kMax, updateFactor, shape)
}
