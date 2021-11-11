package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.core.blocking.StoreModel
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

class StoreModelStage(size1: Int = 16, size2: Int = 16) extends GraphStage[FlowShape[Message, List[BaseComparison]]] {
  val in = Inlet[Message]("StoreModelStage.in")
  val out = Outlet[List[BaseComparison]]("StoreModelStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val storeModel = new StoreModel(size1, size2)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val m = grab(in)
          m match {
            case Comparisons(comparisons) => solveComparisons(comparisons)
            case Update(id, model) => solveUpdate(id, model)
            case _ => println("Che cazzo mi significa?")
          }
        }

        def solveComparisons(comparisons: List[BaseComparison]): Unit = {
          val comps = storeModel.betterSolveComparisons(comparisons)
          if (comps.size > 0)
            push(out, comps)
          else
            pull(in)
        }

        def solveUpdate(id: Int, model: TokenNGrams): Unit = {
          storeModel.solveUpdate(id, model)
          pull(in)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

}