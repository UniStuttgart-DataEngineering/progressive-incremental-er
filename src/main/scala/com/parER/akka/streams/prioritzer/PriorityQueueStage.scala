package com.parER.akka.streams.prioritzer

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.core.prioritizing.Prioritizer
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable

class PriorityQueueStage(name: String, opt: Long = 0) extends GraphStage[FlowShape[List[BaseComparison], List[BaseComparison]]] {
  val in = Inlet[List[BaseComparison]]("PriorityStage.in")
  val out = Outlet[List[BaseComparison]]("PriorityStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {


      val prioritizer = Prioritizer.apply(name, opt)
      val buffer = mutable.Queue[BaseComparison]()
      var downstreamWaiting = false
      val kMax = 10

      def bufferFull = buffer.size == kMax

      override def preStart(): Unit = {
        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        pull(in)
      }

      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          prioritizer.execute(grab(in))
          if (downstreamWaiting) {
            downstreamWaiting = false
            buffer.enqueueAll(prioritizer.get(kMax-buffer.size))
            push(out, buffer.dequeueAll(e => true).toList)
          }
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          println("HEY ??")
          while (prioritizer.hasComparisons()) {
            emit(out, prioritizer.get(kMax))
          }
          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (!prioritizer.hasComparisons()) {
            downstreamWaiting = true
          } else {
            buffer.enqueueAll(prioritizer.get(kMax-buffer.size))
            push(out, buffer.dequeueAll(e => true).toList)
          }
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        }
      })
    }
}
