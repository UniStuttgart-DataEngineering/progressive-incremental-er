package com.parER.akka.streams

import java.util.Comparator

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.google.common.collect.MinMaxPriorityQueue
import com.parER.akka.streams.messages.{Message, MessageComparisons, Terminate, Update, UpdateSeqAndMessageCompSeq}
import com.parER.akka.streams.utils.RatingControl
import com.parER.core.Config
import com.parER.core.blocking.CompGeneration
import com.parER.core.compcleaning.ScalableBFFilter
import com.parER.core.prioritizing.{BlockSizePrioritizer2, BlockSizePrioritizer3}
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class IncrementalBlockPrioritization2(val ignoreComparisonRequest: Boolean = false, val comparisonBudget: Int = 1000, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

  println(s"[${Config.prioritizer}] Mode ignoreComparisonRequest: $ignoreComparisonRequest")
  println(s"[${Config.prioritizer}] Comparison budget x increment: $comparisonBudget")

  val in = Inlet[Seq[(Int, TokenNGrams)]]("IncrementalBlockPrioritization.in")
  val out = Outlet[Message]("IncrementalBlockPrioritization.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val ccer = Config.ccer

      // For time measurements
      var tTotal = 0L
      var tBlocking = 0L
      var tCompGen = 0L
      var tBlockFetch = 0L

      val blockScheduling = //if (Config.prioritizer == "ipbs4")
          new BlockSizePrioritizer3(size1, size2, ro, ff)
        //else
        //  new BlockSizePrioritizer2(size1, size2, ro, ff)

      val compGeneration = new CompGeneration
//      val rankingCompCleaner = Ranking.apply(rankMethod)
//      val compRanker = Prioritizing.apply(progressiveMethod)
//      var count = 0
//      var originalComparisons = 0
//      var prunedComparisons = 0

      val rc = new RatingControl(1000)
      var tUpdate = System.currentTimeMillis()
      var upstreamTerminated, noMoreComparisons = false
      var count = 0
      val nprofiles = Config.nprofiles

      var K : Int = 1
      var maxK = 1

      var cmpList = ListBuffer[LightWeightComparison]()
      val globalCCFunc = new ScalableBFFilter(1000000, 0.1f)
      //val globalCCFunc = new ScalableBFCompCleaner(0.1f)
      //val globalCCFunc = new GlobalHSCompCleaner

      // Max number of comparisons that a PQ can contain
      val maxComparisons = (150.0 * Config.nduplicates).toInt

      println(s"[LightWeightPQPrioritizer] maxComparisons = $maxComparisons")

      val comparator = new Comparator[(LightWeightComparison, Int)] {
        override def compare(o1: (LightWeightComparison, Int), o2: (LightWeightComparison, Int)): Int = {
          //if (o1._3 == o2._3) {
            if (o1._2 == o2._2)
              -java.lang.Float.compare(o1._1.sim, o2._1.sim) // The bigger sim value wins
            else
              java.lang.Integer.compare(o1._2, o2._2) // Smallest number of comparisons wins
//          } else
//            java.lang.Integer.compare(o1._3, o2._3) // Smallest block size wins
        }
      }

      // Main pq
      val pq : MinMaxPriorityQueue[(LightWeightComparison, Int)] = MinMaxPriorityQueue
        .orderedBy(comparator)
        .maximumSize(maxComparisons)
        .create()

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Operate on increments.
          val items = grab(in)
          var TOT0, t0 = System.currentTimeMillis()

          // service time
          val ts = Math.max(rc.getTSTime, rc.getDSTime)
          val maxValue = (rc.getUSTime/ts).toInt


          if (ignoreComparisonRequest && comparisonBudget > 0) {
            K = comparisonBudget
          } else if ( upstreamTerminated ) {
            K = maxK
            //            K += (if (Math.abs(rc.getTSTime - rc.getDSTime) < 0.1f) 1 else -1)
            //            K = Math.max(1, K)
            //            K = Math.max(K, maxK)
          } else {
            K += (if (rc.getUSTime / ts > 1.0f) 1 else -1)
            K = Math.max(1, K)
            if (maxValue >= 1)
              K = Math.min(K, maxValue)
            maxK = if (K > maxK) K else maxK
          }

//          if (ignoreComparisonRequest && comparisonBudget > 0) {
//            K = comparisonBudget
//          } else if ( upstreamTerminated ) {
//            K = maxK
//          } else {
//            K += (if (rc.getUSTime / ts > 1.0f) 1 else -1)
//            K = Math.max(1, K)
//            if (maxValue >= 1)
//              K = Math.min(K, maxValue)
//
//            maxK = if (K > maxK) K else maxK
//          }

          //println(s"Get $K elements")
          // Update messages
          val updates = Seq.newBuilder[Update]
          for ((i, p) <- items) {
            updates += Update(i, p)
          }

          // Block Scheduling approach
          if (items.size > 0) {
            //println("HEY")
            count += items.size
            rc.updateUSTime(TOT0-tUpdate)
            tUpdate = TOT0
            blockScheduling.incrementalProcess(items)
          }
          tBlocking += (System.currentTimeMillis() - t0)

//          breakable {
//            while (cmpList.size < K) {
//              //while (pq.isEmpty || pq.size() < K || !pq.isEmpty && pq.peekFirst()._2 > blockScheduling.getMinimumBlockSize() ) {
//              val curr_bsize = blockScheduling.getMinimumBlockSize()
//              val top_bsize = if (!pq.isEmpty) pq.peekFirst()._2 else 1
//              t0 = System.currentTimeMillis()
//              val (tok, b1, b2) = blockScheduling.getMinimum()
//
//              tBlockFetch += (System.currentTimeMillis() - t0)
//
//              // Update pq only if...
//              if (pq.isEmpty || pq.size() < K && curr_bsize < top_bsize) {
//                t0 = System.currentTimeMillis()
//                val comparisons = (compGeneration.blockMessageComparisonsEmpty(0, b1._1, (tok, b1._2)) ++ (
//                  if (ccer) compGeneration.blockMessageComparisonsEmpty(1, b2._1, (tok, b2._2)) else List())).distinct
//
//                val blocksize = if (tok != null) blockScheduling.countBlockComparisons(tok) else 1
//                globalCCFunc.execute(comparisons).map(x => (x, blocksize)).foreach(x => {
//                  x._1.sim = blockScheduling.approxCBS(x._1, tok)
//                  pq.add(x)
//                })
//              }
//
//              while (!pq.isEmpty && cmpList.size < K) {
//                cmpList.addOne(pq.removeFirst()._1)
//              }
//
//              tCompGen += (System.currentTimeMillis() - t0)
//
//              if (tok == null)
//                break
//            }
//          }

          breakable {
            while (pq.isEmpty || pq.size() < K || !pq.isEmpty && pq.size > 100*K && pq.peekFirst()._2 > blockScheduling.getMinimumBlockSize() ) {
              t0 = System.currentTimeMillis()

              val (tok, b1, b2) = blockScheduling.getMinimum()
              val blocksize = if (tok != null) blockScheduling.countBlockComparisons(tok) else 1
              tBlockFetch += (System.currentTimeMillis() - t0)

              t0 = System.currentTimeMillis()
              //            val comparisons = compGeneration.blockComparisons(0, b1._1, (tok, b1._2)) ++
              //              compGeneration.blockComparisons(1, b2._1, (tok, b2._2))

              val comparisons = (compGeneration.blockMessageComparisonsEmpty(0, b1._1, (tok, b1._2)) ++ (
                if (ccer) compGeneration.blockMessageComparisonsEmpty(1, b2._1, (tok, b2._2)) else List())).distinct
              //val cmpsize = comparisons.size
              globalCCFunc.execute(comparisons).map(x => (x, blocksize)).foreach( x => {
                  x._1.sim = blockScheduling.approxCBS(x._1, tok)
                  pq.add(x)
                }
              )

              //if (blocksize > 100 + comparisons.size)
              //  println("LEL")
              //cmpList.addAll(globalCCFunc.execute(comparisons))
              //cmpList.addAll(compGeneration.blockComparisons(0, b1._1, (tok, b1._2)))
              //cmpList.addAll(compGeneration.blockComparisons(1, b2._1, (tok, b2._2)))
              tCompGen += (System.currentTimeMillis() - t0)

              if (tok == null)
                break
            }
          }

          while (!pq.isEmpty && cmpList.size < K) {
            cmpList.addOne(pq.removeFirst()._1)
          }



          // Push the comparisons, should be O(1)
          val comps = List.newBuilder[LightWeightComparison]
          for (i <- 0 to K if !cmpList.isEmpty) {
            comps += cmpList.head
            cmpList.remove(0)
          }

          // Total time
          tTotal += (System.currentTimeMillis() - TOT0)
          val delta = System.currentTimeMillis() - TOT0
          rc.updateTSTime(delta)

          //println(s"Send ${comps.knownSize} comparisons; ")
          val msg = if (items.size > 0)
            UpdateSeqAndMessageCompSeq(items.map(x => Update(x._1, x._2)), comps.result())
          else
            MessageComparisons(comps.result())

          push(out, msg)

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