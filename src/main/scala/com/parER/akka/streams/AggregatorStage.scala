package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{AggregatorMessage, Comparisons, Message, Update}
import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class AggregatorStage(nBlockers: Int, ff: Float) extends GraphStage[FlowShape[AggregatorMessage, Message]] {
  val in = Inlet[AggregatorMessage]("AggregatorStage.in")
  val out = Outlet[Message]("AggregatorStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      var hm = new mutable.HashMap[String, ListBuffer[AggregatorMessage]]() // state
      val ccer = Config.ccer

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val msg = grab(in)
          val id = msg.id
          if (nBlockers > 1) {
            if (hm.contains(id)) {
              hm(id) += msg
              if (hm(id).size == nBlockers) {
                emitMessage(id, msg.e1, msg.e1Model)
              } else {
                pull(in)
              }
            } else {
              var lb = ListBuffer[AggregatorMessage](msg)
              hm.update(id, lb)
              pull(in)
            }
          } else {
            hm.update(msg.id, ListBuffer(msg))
            emitMessage(msg.id, msg.e1, msg.e1Model)
          }
        }

        override def onUpstreamFinish(): Unit = {
          println("UPSTREAMFINISHED " + hm.keys.size)
          completeStage()
        }

        def emitMessage(id: String, idx: Int, model: TokenNGrams) = {
          val messages = hm(id).result()
          if (messages.size != nBlockers) {
            println("ERROR")
            assert(false)
          }
          val blocks = getBlocks(messages)
          val comparisons = generateComparisons(idx, model, blocks)
          hm.remove(id)
          if (comparisons.size > 0) {
            val msg = List(new Update(idx, model), new Comparisons(comparisons))
            emitMultiple[Message](out, msg)
          } else {
            push(out, new Update(idx,model))
          }
        }

        def getBlocks(messages: Iterable[AggregatorMessage]) = {
          var blocks = new ListBuffer[List[Int]]()
          for (msg <- messages)
            blocks ++= msg.blocks
          if (blocks.size > 0){
            val minSize = blocks.foldLeft(blocks(0).size){ (min, e) => math.min(min, e.size) }
            blocks.filterInPlace(b => (b.size+1)*ff < minSize+1)
          }
          blocks
        }

        def generateComparisons(idx: Int, textModel: TokenNGrams, blocks: ListBuffer[List[Int]]) = {
          val comparisons = List.newBuilder[BaseComparison]
          (textModel.getDatasetId, ccer) match {
            case (_, false) | (1, true) => for (block <- blocks; i <- block) comparisons.addOne(new Comparison(i, null, idx, textModel))
            case(0, true) => for (block <- blocks; i <- block) comparisons.addOne(new Comparison(idx, textModel, i, null))
          }
          comparisons.result()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

}