package com.parER.akka.streams

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.concurrent.{Future, Promise}

class CollectorSink(threshold: Float = 0.5f) extends GraphStageWithMaterializedValue[SinkShape[List[BaseComparison]], Future[(Long, List[BaseComparison])]] {
  val in: Inlet[List[BaseComparison]] = Inlet("CollectorSink")
  override val shape: SinkShape[List[BaseComparison]] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val p: Promise[(Long, List[BaseComparison])] = Promise()
    val logic = new GraphStageLogic(shape) {

      val buffer = List.newBuilder[BaseComparison]
      var counter = 0L;

      // This requests one element at the Sink startup.
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val comparisons = grab(in)
          counter += comparisons.size
          buffer ++= comparisons.filter(c => c.sim >= threshold)
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          val result = (counter, buffer.result())
          p.trySuccess(result)
          completeStage()
        }
      })
    }
    (logic, p.future)
  }
}
