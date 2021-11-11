package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{BlockTuple, Message, Update}
import com.parER.core.blocking.Blocking
import org.scify.jedai.textmodels.TokenNGrams

class ProcessingTokenBlockerStage(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {
  val in = Inlet[(Int, TokenNGrams)]("ProcessingTokenBlockerStage.in")
  val out = Outlet[Message]("ProcessingTokenBlockerStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val tokenBlocker = Blocking.apply(name, size1, size2, ro, ff)
      tokenBlocker.setModelStoring(false)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (i, p) = grab(in)
          val tuple = tokenBlocker.process(i, p)
          if (noUpdate) {
            push(out, BlockTuple(tuple._1, tuple._2, tuple._3))
          }
          else {
            if (tuple._3.size > 0) {
              val msg = List(Update(i, p), BlockTuple(tuple._1, tuple._2, tuple._3))
              emitMultiple[Message](out, msg)
            }
            else {
              push(out, Update(i,p))
            }
          }
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}