package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.parER.core.collecting.ProgressiveCollector
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation

import scala.concurrent.{Future, Promise}

class ProgressiveCollectorSinkWithoutMatValue(t0: Long, t1: Long, dp: AbstractDuplicatePropagation, print: Boolean = true) extends GraphStage[SinkShape[List[BaseComparison]]] {
  val in: Inlet[List[BaseComparison]] = Inlet("ProgressiveCollectorSinkWithoutMatValue")
  override val shape: SinkShape[List[BaseComparison]] = SinkShape(in)


  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val proColl = new ProgressiveCollector(t0, t1, dp, print)

      // This requests one element at the Sink startup.
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val comparisons = grab(in)
          proColl.execute(comparisons)
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          proColl.printLast()
          completeStage()
        }

      })
    }
}
