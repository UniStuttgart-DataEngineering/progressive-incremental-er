package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner, StoreModel}
import com.parER.core.compcleaning.{ComparisonCleaning, GlobalHSCompCleaner}
import com.parER.core.prioritizing.{Prioritizing, TreeMapPrioritizer}
import com.parER.core.ranking.{Ranking, WNP2JSRanker}
import com.parER.datastructure.{BlockRanker, ProgressiveComparison, ValueEntityRanker}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 *
 * This stage simply applies comparison centric approach.
 *
 * @param rankMethod
 * @param progressiveMethod
 * @param size1
 * @param size2
 * @param ro
 * @param ff
 * @param noUpdate
 */

class AbstractProgressiveComparisonCentricStage(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {

  val in = Inlet[(Int, TokenNGrams)]("AbstractProgressiveComparisonCentricStage.in")
  val out = Outlet[Message]("AbstractProgressiveComparisonCentricStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // For time measurements
      var tTotal = 0L
      var tBlocking = 0L
      var tBlGhost = 0L
      var tCompGen = 0L
      var tCoCl = 0L
      var tModel = 0L
      var tRank = 0L
      var tGetBest = 0L
      var tGlobalCoCl = 0L

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)
      val blockGhoster = new BlockGhosting(Config.filteringRatio)
      val compGeneration = new CompGeneration

      val rankingCompCleaner = Ranking.apply(rankMethod)

      // TODO only comparison-centric progressiveMethod available
      // TODO Variants: PriorityQueue, TreeMap of hashsets, Adaptive TreeMap
      val compRanker = Prioritizing.apply(progressiveMethod)

      var count = 0

      var originalComparisons = 0
      var prunedComparisons = 0

      tokenBlocker.setModelStoring(false)

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Usual token blocking + block pruning + block ghosting + I-WNP + generation
          val (i, p) = grab(in)

          var TOT0 = System.currentTimeMillis()

          var t0 = System.currentTimeMillis()
          val tuple = tokenBlocker.progressiveProcess(i, p)
          tBlocking += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          val bIds = blockGhoster.progressiveProcess(tuple._1, tuple._2, tuple._3)
          tBlGhost += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          var comps = compGeneration.progressiveComparisons(bIds._1, bIds._2, bIds._3)
          tCompGen += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          comps = rankingCompCleaner.execute(comps)
          tCoCl += (System.currentTimeMillis() - t0)

          if (comps.size > 0) {

            t0 = System.currentTimeMillis()
            originalComparisons += comps.size

            // TODO remove this, check correctness of comparisons
            comps.foreach(x => {
                if (x.sim - 1.0f > 0.0f) {
                  println(s"Error: comparison with sim: ${x.sim}")
                  assert(false)
                }
            })

            compRanker.update(comps)
            tRank += (System.currentTimeMillis() - t0)

            // Select best comparisons
            t0 = System.currentTimeMillis()
            comps = compRanker.getBestComparisons()
            prunedComparisons += comps.size
            tGetBest += (System.currentTimeMillis() - t0)

          } else if (!compRanker.isEmpty()) {
            t0 = System.currentTimeMillis()
            comps = compRanker.getBestComparisons()
            prunedComparisons += comps.size
            tGetBest += (System.currentTimeMillis() - t0)
          }

          t0 = System.currentTimeMillis()
          // TODO GlobalCoCl?
          tGlobalCoCl += (System.currentTimeMillis() - t0)
          tTotal += (System.currentTimeMillis() - TOT0)

          // Push the comparisons
          if (noUpdate) {
            emit(out, Comparisons( comps ))
          }
          else {
            if (comps != null && comps.size > 0) {
              val msg = List(Update(i, p), Comparisons(comps))
              emitMultiple[Message](out, msg)
            }
            else {
              emit(out, Update(i,p))
            }
          }
        }

        override def onUpstreamFinish(): Unit = {

          println("Upstream finished")
          println("Original comparisons: "+originalComparisons)
          println("Pruned comparisons:"+prunedComparisons)

          println("tBlocking (ms) = " + tBlocking)
          println("tBlGhost (ms) = " + tBlGhost)
          println("tCompGen (ms) = " + tCompGen)
          println("tCoCl (ms) = " + tCoCl)
          println("tModel (ms) = " + tModel)
          println("tRank (ms) = " + tRank)
          println("tGetBest (ms) = " + tGetBest)
          println("tGlobalCoCl (ms) = " + tGlobalCoCl)
          println("OVERHEAD-sum (ms) = " + (tBlocking+tBlGhost+tCompGen+tCoCl+tRank+tGetBest+tGlobalCoCl+tModel))
          println("OVERHEAD (ms) = " + (tTotal) )

//          println("====== other statistics ===== ")
//          println("avg size = " + compRanker.getAvg())
//          println("max size = " + compRanker.getMax())
//          println("count ones = " + compRanker.getOnes)
//          println("count greater than ones = " + compRanker.getGreaterThanOne)
//          println("count " + compRanker.totSumSizesCount)
//
//          println("count comparisons = " + compRanker.getNComparisons())
//          println("count avg comparisons = " + compRanker.getAvgNComparisons())

          while ( !compRanker.isEmpty()) {
            var comps = compRanker.getBestComparisons()
            emit(out, Comparisons(comps))
          }

          println("Best comparisons terminated")

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