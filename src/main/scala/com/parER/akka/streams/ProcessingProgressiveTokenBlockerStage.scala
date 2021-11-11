package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{BlockTuple, Comparisons, Message, Update}
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, Blocking, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.compcleaning.{ComparisonCleaning, GlobalHSCompCleaner}
import com.parER.datastructure.{BlockRanker, EntityRanker, ProgressiveComparison, ValueEntityRanker}
import org.scify.jedai.textmodels.TokenNGrams

import scala.jdk.CollectionConverters._
import scala.collection.mutable

class ProcessingProgressiveTokenBlockerStage(name: String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[(Int, TokenNGrams), Message]] {
  val in = Inlet[(Int, TokenNGrams)]("ProcessingTokenBlockerStage.in")
  val out = Outlet[Message]("ProcessingTokenBlockerStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)
      val blockGhoster = new BlockGhosting(Config.filteringRatio)
      val compGeneration = new CompGeneration
      val compCleaner = ComparisonCleaning.apply(Config.ccMethod)
      val globalCompCleaner = new GlobalHSCompCleaner

      val entityKeysMaps = List ( new mutable.HashMap[Int, TokenNGrams](), new mutable.HashMap[Int, TokenNGrams]() )

      val ranker = new ValueEntityRanker
      val blockRanker = new BlockRanker
      var count = 0

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

          // Rank entities and blocks
          if (comps.size > 0) {

            // Rank the input entity exploiting the similarity field after I-WNP
            val rank = comps.foldRight(0.0f)(_.sim + _)
            val size = comps.size
            ranker.update(i, p.getDatasetId, rank, size)

            // Update rank of the other entities involved
            comps.foreach(c => {
              val (id, did) = if (c.e1Model == null) (c.e1, 0) else (c.e2, 1)
              ranker.update(id, did, c.sim, 1)
            })

            // Rank the blocks
            comps.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).foreach(x => {
              val blockCardinality = x._2.size
              val blockRank = x._2.foldRight(0.0f)(_.sim + _)
              blockRanker.update(x._1, blockRank, blockCardinality)
            })

          }

          // Select the best comparisons
          if (!ranker.isEmpty(0) && !ranker.isEmpty(1)) {

            // Get the best candidate
            ranker.getMax() match { case Some(t) =>

                val (cid, cdid, crank) = t
                val (tokenBlocks, cModel) = if (i != cid && crank > 0) {
                  val ekMap = entityKeysMaps(cdid)
                  val cModel = ekMap(cid)
                  (blockGhoster.progressiveProcess(cid, cModel,
                    tokenBlocker.getProgressiveBlocks(cid, cdid, ekMap(cid).getSignatures.asScala.toList))._3,
                    cModel)
                } else {
                  (bIds._3, p)
                }

                // Best blocks are returned
                if (tokenBlocks.size > 0) {
                  val bestKeys = blockRanker.getBestBlocks(tokenBlocks.map(_._1))
                  val keyBlock = tokenBlocks.filter(x => bestKeys.contains(x._1))

                  // generate comparisons
                  comps = compGeneration.progressiveComparisons(cid, cModel, keyBlock)
                  val size0 = comps.size
                  val newRank = -comps.size
                  comps = compCleaner.execute(comps)
                  val newSize = -comps.size
                  val size1 = comps.size

                  // update block ranks
                  comps.asInstanceOf[List[ProgressiveComparison]].groupBy(_.key).foreach(x => {
                    val blockCardinality = x._2.size
                    val blockRank = x._2.foldRight(0.0f)(_.sim + _)
                    blockRanker.update(x._1, -blockRank, -blockCardinality)
                  })

                  comps = globalCompCleaner.execute(comps)
                  ranker.update(cid, cModel.getDatasetId, newRank, newSize)
                }
            }

          }

          // Push the comparisons
          if (noUpdate) {
            push(out, Comparisons( comps ))
          }
          else {
            if (comps != null && comps.size > 0) {
              val msg = List(Update(i, p), Comparisons(comps))
              emitMultiple[Message](out, msg)
            }
            else {
              push(out, Update(i,p))
            }
          }
        }

        override def onUpstreamFinish(): Unit = {
          println("Upstream finished")
          println("Count value = " + count)
          println("BlockRanker count = " + blockRanker.count)

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
          pull(in)
        }
      })
    }
}