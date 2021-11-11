package com.parER.akka.streams.utils

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable

class UnboundedBuffer[A] extends GraphStage[FlowShape[A, A]] {
  val in = Inlet[A]("UnboundedBuffer.in")
  val out = Outlet[A]("UnboundedBuffer.out")

  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val buffer = mutable.Queue[A]()
      def bufferFull = buffer.size == 1e8 //buffer.size == 2
      var downstreamWaiting = false

      override def preStart(): Unit = {
        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        pull(in)
      }

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            buffer.enqueue(elem)
            if (downstreamWaiting) {
              downstreamWaiting = false
              val bufferedElem = buffer.dequeue()
              push(out, bufferedElem)
            }
            if (!bufferFull) {
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.nonEmpty) {
              // emit the rest if possible
              emitMultiple(out, buffer.iterator)
            }
            completeStage()
          }
        })

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (buffer.isEmpty) {
              downstreamWaiting = true
            } else {
              val elem = buffer.dequeue()
              push(out, elem)
            }
            if (!bufferFull && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        })
    }

}














