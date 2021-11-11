package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.core.blocking.Blocking
import org.scify.jedai.textmodels.TokenNGrams

class TokenBlockerStage(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {
  val in = Inlet[(Int, TokenNGrams)]("TokenBlockerStage.in")
  val out = Outlet[Message]("TokenBlockerStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val tokenBlocker = Blocking.apply(name, size1, size2, ro, ff)
      tokenBlocker.setModelStoring(false)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (i, p) = grab(in)
          val comparisons = tokenBlocker.execute(i, p)
          if (comparisons.size > 0) {
            val msg = List(new Update(i, p), new Comparisons(comparisons))
            emitMultiple[Message](out, msg)
          } else {
            push(out, new Update(i,p))
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