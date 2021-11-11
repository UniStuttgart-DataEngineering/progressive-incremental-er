package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, CompGeneration, StoreModel, TokenBlockerRefiner}
import com.parER.core.compcleaning.GlobalHSCompCleaner
import com.parER.core.prioritizing.TreeMapPrioritizer
import com.parER.core.ranking.WNP2Ranker
import com.parER.datastructure.{BlockRanker, ProgressiveComparison, ValueEntityRanker}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 *
 * This stage simply applies comparison centric approach.
 *
 * @param name
 * @param size1
 * @param size2
 * @param ro
 * @param ff
 * @param noUpdate
 */

class ProgressiveComparisonCentricStageV2(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {
  val in = Inlet[(Int, TokenNGrams)]("ProgressiveComparisonCentricStage.in")
  val out = Outlet[Message]("ProgressiveComparisonCentricStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // For time measurements
      var tBlocking = 0L
      var tBlGhost = 0L
      var tCompGen = 0L
      var tCoCl = 0L
      var tModel = 0L
      var tRank = 0L
      var tGetBest = 0L
      var tGlobalCoCl = 0L

      val storeModel = new StoreModel
      val tokenBlocker = new TokenBlockerRefiner(size1, size2, ro, ff)
      val blockGhoster = new BlockGhosting(Config.filteringRatio)
      val compGeneration = new CompGeneration
      val compCleaner = new WNP2Ranker//new WNP2CompCleaner//ComparisonCleaning.apply(Config.ccMethod)
      val globalCompCleaner = new GlobalHSCompCleaner

      val entityKeysMaps = List ( new mutable.HashMap[Int, TokenNGrams](), new mutable.HashMap[Int, TokenNGrams]() )

      val ranker = new ValueEntityRanker
      val blockRanker = new BlockRanker
      val compRanker = new TreeMapPrioritizer
      var count = 0

      var originalComparisons = 0
      var prunedComparisons = 0

      tokenBlocker.setModelStoring(false)

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Usual token blocking + block pruning + block ghosting + I-WNP + generation
          val (i, p) = grab(in)

          var t0 = System.currentTimeMillis()
          val tuple = tokenBlocker.process(i, p)
          tBlocking += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          val bIds = blockGhoster.process(tuple._1, tuple._2, tuple._3)
          tBlGhost += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          var comps = compGeneration.generateComparisons(bIds._1, bIds._2, bIds._3)
          tCompGen += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          comps = compCleaner.execute(comps)
          tCoCl += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          storeModel.solveUpdate(i, p)
          comps = storeModel.solveComparisons(comps)
          tModel += (System.currentTimeMillis() - t0)

//          // Update the model
//          t0 = System.currentTimeMillis()
//          entityKeysMaps(p.getDatasetId).update(i, p)
//          tModel += (System.currentTimeMillis() - t0)

          if (comps.size > 0) {

            t0 = System.currentTimeMillis()
            originalComparisons += comps.size

            // Rank comparisons
            comps.foreach(x => {
                if (x.sim - 1.0f > 0.01f) {
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
          //if (comps != null)
          //  comps = globalCompCleaner.execute(comps)
          tGlobalCoCl += (System.currentTimeMillis() - t0)

          //println("Comparisons2 = " + comps.size)

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

          while (!compRanker.isEmpty()) {
            var comps = compRanker.getBestComparisons()
            comps = globalCompCleaner.execute(comps)
            emit(out, Comparisons(comps))
          }

          println("Best comparisons terminated")

          while (!ranker.isEmpty(0) && !ranker.isEmpty(1)) {

            ranker.getMax() match { case Some(t) =>

              var (cid, cdid, crank) = t

              val ekMap = entityKeysMaps(cdid)
              val cModel = ekMap(cid)
              var tokenBlocks = blockGhoster.progressiveProcess(cid, cModel,
                tokenBlocker.getProgressiveBlocks(cid, cdid, ekMap(cid).getSignatures.asScala.toList))._3

              if (crank <= 0.0f) {
                System.out.println("SOMETHING WRONG")
              }

              if (tokenBlocks.size > 0) {
                val bestKeys = blockRanker.getBestBlocks(tokenBlocks.map(_._1))
                val keyBlock = tokenBlocks.filter(x => bestKeys.contains(x._1))

                // generate comparisons
                var comps = compGeneration.progressiveComparisons(cid, cModel, keyBlock)
                val newRank = -comps.size
                comps = compCleaner.execute(comps)
                val newSize = -comps.size

                // update block ranks
                comps.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).foreach(x => {
                  val blockCardinality = x._2.size
                  val blockRank = x._2.foldRight(0.0f)(_.sim + _)
                  blockRanker.update(x._1, -blockRank, -blockCardinality)
                })

                comps = globalCompCleaner.execute(comps)
                ranker.update(cid, cModel.getDatasetId, newRank, newSize)
                emit(out, Comparisons(comps))
              } else {
                ranker.reset(cid, cdid)
              }


            }
          }

          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          //println("Pulling...")
          pull(in)
        }
      })
    }
}