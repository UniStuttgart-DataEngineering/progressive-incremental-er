package com.parER.akka.streams.prioritzer

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.datastructure.{BaseComparison, Comparison}

class NoStage(name: String, opt: Long = 0) extends GraphStage[FlowShape[List[BaseComparison], List[BaseComparison]]] {
  val in = Inlet[List[BaseComparison]]("NoStage.in")
  val out = Outlet[List[BaseComparison]]("NoStage.out")

  override val shape = FlowShape.of(in, out)

  //println(s"KMax: ${opt.toInt}")

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          push(out, grab(in))
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}
