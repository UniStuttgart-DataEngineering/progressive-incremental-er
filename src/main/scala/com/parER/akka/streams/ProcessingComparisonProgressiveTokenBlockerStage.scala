package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.compcleaning.{ComparisonCleaning, GlobalHSCompCleaner, WNP2CompCleaner, WNPCompCleaner}
import com.parER.core.prioritizing.TreeMapPrioritizer
import com.parER.datastructure.{BlockRanker, ProgressiveComparison, ValueEntityRanker}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ProcessingComparisonProgressiveTokenBlockerStage(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {
  val in = Inlet[(Int, TokenNGrams)]("ProcessingTokenBlockerStage.in")
  val out = Outlet[Message]("ProcessingTokenBlockerStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)
      val blockGhoster = new BlockGhosting(Config.filteringRatio)
      val compGeneration = new CompGeneration
      val compCleaner = new WNP2CompCleaner//ComparisonCleaning.apply(Config.ccMethod)
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
          val tuple = tokenBlocker.progressiveProcess(i, p)
          val bIds = blockGhoster.progressiveProcess(tuple._1, tuple._2, tuple._3)
          var comps = compCleaner.execute(
            compGeneration.progressiveComparisons(bIds._1, bIds._2, bIds._3))

          // Update the model
          entityKeysMaps(p.getDatasetId).update(i, p)

          //println("Received i = "+i)
          //println("Comparisons = " + comps.size)

          if (comps.size > 0) {

            originalComparisons += comps.size

            // Rank input entity
            val rank = comps.foldRight(0.0f)(_.sim + _)
            val size = comps.size
            ranker.update(i, p.getDatasetId, rank, size)

            // Update rank of other entities involved
            comps.foreach(c => {
              val (id, did) = if (c.e1Model == null) (c.e1, 0) else (c.e2, 1)
              ranker.update(id, did, c.sim, 1)
            })

            // Rank the blocks
            comps.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).foreach(x => {
              val blockCardinality = x._2.size
              val blockRank = x._2.foldRight(0.0f)(_.sim + _)
              blockRanker.update(x._1, blockRank, blockCardinality)

              // Update sim of comps
              x._2.foreach(c => {
                assert(x._1 == c.key)
                c.sim = c.sim / blockCardinality.toFloat
                //println(c.sim)
              })
            })

            // Rank comparisons
            compRanker.update(comps)
            // Select best comparisons
            comps = compRanker.getBestComparisons()
            prunedComparisons += comps.size
          } else if (!compRanker.isEmpty()) {
            comps = compRanker.getBestComparisons()
            prunedComparisons += comps.size
          }

          if (comps != null)
            comps = globalCompCleaner.execute(comps)

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