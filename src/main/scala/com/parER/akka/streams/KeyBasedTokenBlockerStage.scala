package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.AggregatorMessage
import com.parER.core.blocking.Blocking
import org.scify.jedai.textmodels.TokenNGrams

class KeyBasedTokenBlockerStage(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f) extends GraphStage[FlowShape[(Int, TokenNGrams, Int, List[String]), AggregatorMessage]] {
  val in = Inlet[(Int, TokenNGrams, Int, List[String])]("KeyBasedTokenBlockerStage.in")
  val out = Outlet[AggregatorMessage]("KeyBasedTokenBlockerStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val tokenBlocker = Blocking.apply(name, size1, size2, ro, ff) //
      tokenBlocker.setModelStoring(false)

      //val f = (i: Int, p: TokenNGrams, k: List[String]) => tokenBlocker.execute(i, p, k)
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (i, p, id, k) = grab(in)
          val tuple = tokenBlocker.process(i,p,k)
          push(out, new AggregatorMessage(i.toString + p.getDatasetId.toString, tuple._1, tuple._2, tuple._3))
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}