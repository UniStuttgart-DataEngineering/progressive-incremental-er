package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{BlockTupleSeq, Comparisons, Message, Update, UpdateSeq}
import com.parER.core.blocking.CompGeneration
import com.parER.core.compcleaning.{GlobalHSCompCleaner, ScalableBFCompCleaner}
import com.parER.core.prioritizing.{BlockSizePrioritizer, BlockSizePrioritizer2, Prioritizing}
import com.parER.core.ranking.Ranking
import com.parER.datastructure.BaseComparison
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable.ListBuffer

/**
 *
 * This stage simply applies I-PBS
 *
 * @param rankMethod
 * @param progressiveMethod
 * @param size1
 * @param size2
 * @param ro
 * @param ff
 * @param noUpdate
 */

class IncrementalProgressiveBlockScheduling(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

  val in = Inlet[Seq[(Int, TokenNGrams)]]("IncrementalProgressiveBlockScheduling.in")
  val out = Outlet[Message]("IncrementalProgressiveBlockScheduling.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // For time measurements
      var tTotal = 0L
      var tBlocking = 0L
      var tCompGen = 0L
      var tBlockFetch = 0L

      val blockScheduling = new BlockSizePrioritizer2(size1, size2, ro, ff)
      val compGeneration = new CompGeneration
//      val rankingCompCleaner = Ranking.apply(rankMethod)
//      val compRanker = Prioritizing.apply(progressiveMethod)
//      var count = 0
//      var originalComparisons = 0
//      var prunedComparisons = 0

      var cmpList = ListBuffer[BaseComparison]()
      val globalCCFunc = new ScalableBFCompCleaner(0.1f)
      //val globalCCFunc = new GlobalHSCompCleaner


      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Operate on increments.
          val items = grab(in)
          var TOT0, t0 = System.currentTimeMillis()

          // Update messages
          val updates = Seq.newBuilder[Update]
          for ((i, p) <- items) {
            updates += Update(i, p)
          }

          // Block Scheduling approach
          if (items.size > 0) {
            blockScheduling.incrementalProcess(items)
          }
          tBlocking += (System.currentTimeMillis() - t0)

          if (cmpList.isEmpty) {
            t0 = System.currentTimeMillis()
            val (tok, b1, b2) = blockScheduling.getMinimum()
            tBlockFetch += (System.currentTimeMillis() - t0)

            t0 = System.currentTimeMillis()
//            val comparisons = compGeneration.blockComparisons(0, b1._1, (tok, b1._2)) ++
//              compGeneration.blockComparisons(1, b2._1, (tok, b2._2))
            val comparisons = compGeneration.blockComparisonsEmpty(0, b1._1, (tok, b1._2)) ++
              compGeneration.blockComparisonsEmpty(1, b2._1, (tok, b2._2))

            cmpList.addAll(globalCCFunc.execute(comparisons))
            //cmpList.addAll(compGeneration.blockComparisons(0, b1._1, (tok, b1._2)))
            //cmpList.addAll(compGeneration.blockComparisons(1, b2._1, (tok, b2._2)))
            tCompGen += (System.currentTimeMillis() - t0)
          }

          // Push the comparisons, should be O(1)
          val comps = if (cmpList.size > 0) {
            val cmp = cmpList.head
            cmpList.remove(0)
            List(cmp)
          } else
            List()

          // Total time
          tTotal += (System.currentTimeMillis() - TOT0)

          if (noUpdate) {
            emit(out, Comparisons(comps))
          }
          else {
            if (comps != null) {
              val msg = List(UpdateSeq(updates.result()), Comparisons(comps))
              emitMultiple[Message](out, msg)
            }
            else {
              push(out, UpdateSeq(updates.result()))
            }
          }
        }

        override def onUpstreamFinish(): Unit = {

          println("Upstream finished")
          println("tBlocking (s) = " + tBlocking/1000)
          println("tCompGen (s) = " + tCompGen/1000)
          println("tCompPrioFetch (s) = " + tBlockFetch/1000)
          println("OVERHEAD (s) = " + (tTotal/1000) )

          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}