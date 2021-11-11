package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages._
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.prioritizing.HPrioritizer6
import com.parER.core.ranking.WNP2CBSRanker
import com.parER.datastructure.{BaseComparison, LightWeightComparison}
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

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

class IncrementalComparisonRanking(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

  val in = Inlet[Seq[(Int, TokenNGrams)]]("IncrementalComparisonRanking.in")
  val out = Outlet[Message]("IncrementalComparisonRanking.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =

    new IncrementalComparisonRankingLogic(rankMethod, progressiveMethod, size1, size2, ro, ff, noUpdate, shape) {

      val minutes = Config.rankPrinting
      val rankingCompCleaner = new WNP2CBSRanker //new WNP2JSRanker //Ranking.apply(rankMethod)
      var incrCount = 0

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Operate on increments.
          val items = grab(in)
          incrCount +=1
          //println(s"Increment ${incrCount}")
          //println("incr ranking: receive")
          var TOT0, t0 = System.currentTimeMillis()


          if (t0 - tNow > minutes * 1000 * 60 && items.size > 1) {
            val mb = 1024*1024
            val runtime = Runtime.getRuntime
            var (i0, d0) = if (items.size > 0 ) (items(0)._1, items(0)._2.getDatasetId) else (-1, -1)
            println(s"Running... min: ${(t0-tOrigin)/(1000 * 60)} - items(0)=<${i0}, ${d0}>")
            println(s"Processed: $count")
            println(s"cBlocker = ${cBlocker}")
            println(s"cCompCleaner = ${cCompCleaner}")
            println(s"bufferSize = ${buffer.size}")
            println("tBlocking (s) = " + tBlocking/1000)
            println("tCompGen (s) = " + tCompGen/1000)
            println("tRank (s) = " + tRank/1000)
//            println("tCompPrioUpdate (s) = " + tCompPrioUpdate/1000)
//            println("tCompPrioFetch (s) = " + tCompPrioFetch/1000)
            println("OVERHEAD (s) = " + (tTotal/1000) )
            //compPrioritizer.overhead()
            println("***** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
            tNow = t0
          }

          if (items.size == 1 && items(0)._1 == -1) {

            val blockComparisons = List.newBuilder[LightWeightComparison]
            blockComparisons.addAll(getMinimumBlockComparisons())
            while (existBlocks() && blockComparisons.knownSize < nprofiles) {
              blockComparisons.addAll(getMinimumBlockComparisons())
            }
            val msg = MessageComparisons(blockComparisons.result())
            if (!existBlocks() && msg.comparisons.size == 0) {
              push(out, NoBlockComparisons())
            } else
              push(out, msg)

          } else {

            sorted = false
            count += items.size

            // Token blocking and update step
            t0 = System.currentTimeMillis()
//            val updates = Seq.newBuilder[Update]
//            val tuples = Seq.newBuilder[(Int, TokenNGrams, List[List[Int]])]
//            for ((i, p) <- items) {
//              updates += Update(i, p)
//              val t = tokenBlocker.process(i, p)
//              tuples += blockGhosting.process(t._1, t._2, t._3)
//            }
//            tBlocking += (System.currentTimeMillis() - t0)
//
//            println(s"tBlocking: $tBlocking ms")

            val seqRankedCmpsList = items
              .map( x => {
                val t = tokenBlocker.process(x._1, x._2)
                blockGhosting.process(t._1, t._2, t._3)
              })
              .map( x => x match {
                case (i, p, bl) => {
                  val pair = (p.getDatasetId, compGeneration.generateMessageComparisons(i, p, bl))
                  cBlocker += pair._2.size
                  pair
                }
              })
              .map( x => {
                val rankedComps= MessageComparisons( if (x._1 == 0)
                  rankingCompCleaner.executeMessageComparisons(x._2, _.e2)
                else
                  rankingCompCleaner.executeMessageComparisons(x._2, _.e1))
                cCompCleaner += rankedComps.comparisons.size
                rankedComps
              })
//            tuples.clear()

//            t0 = System.currentTimeMillis()
//            val seqCmpsList = Seq.newBuilder[(Int, List[LightWeightComparison])]
//            for ((i, p, bl) <- tuples.result()) {
//              val pair = (p.getDatasetId, compGeneration.generateMessageComparisons(i, p, bl))
//              cBlocker += pair._2.size
//              seqCmpsList += pair
//            }
//            tuples.clear()
//            tCompGen += (System.currentTimeMillis() - t0)

//            println(s"tCompGen: $tCompGen ms")

            // Ranking (I-WNP + weighting)
//            t0 = System.currentTimeMillis()
//            val seqRankedCmpsList = seqCmpsList.result().map(x => {
//                val rankedComps = MessageComparisons( if (x._1 == 0)
//                  rankingCompCleaner.executeMessageComparisons(x._2, _.e2)
//                else
//                  rankingCompCleaner.executeMessageComparisons(x._2, _.e1))
//                cCompCleaner += rankedComps.comparisons.size
//                rankedComps
//            })
//            seqCmpsList.clear()

//            val seqRankedCmpsList = Seq.newBuilder[MessageComparisons]
//            for ((dId, comps) <- seqCmpsList.result()) {
//              val rankedComps = if (dId == 0)
//                rankingCompCleaner.executeMessageComparisons(comps, _.e2)
//              else
//                rankingCompCleaner.executeMessageComparisons(comps, _.e1)
//              cCompCleaner += rankedComps.size
//              seqRankedCmpsList += MessageComparisons(rankedComps)
//            }
            tRank += (System.currentTimeMillis() - t0)

            //println(s"tRank: $tRank ms")

            //val msg = UpdateSeqAndMessageComparisonsSeq(updates.result(), MessageComparisonsSeq(seqRankedCmpsList.result()))
            //val msg = UpdateSeqAndMessageComparisonsSeq(updates.result(), MessageComparisonsSeq(seqRankedCmpsList))
            val msg = UpdateSeqAndMessageComparisonsSeq(items.map(x => Update(x._1, x._2)), MessageComparisonsSeq(seqRankedCmpsList))

            //println("incr ranking: send")

            if (count < nprofiles || upstreamTerminated)
              push(out, msg)
            else {
              emit(out, msg)
              emit(out, Terminate())
              upstreamTerminated = true
            }
          }
        }

        override def onUpstreamFinish(): Unit = {

          println("Upstream finished")
          println(s"cBlocker = ${cBlocker}")
          println(s"cCompCleaner = ${cCompCleaner}")
          println(s"bufferSize = ${buffer.size}")
          println("tBlocking (s) = " + tBlocking/1000)
          println("tCompGen (s) = " + tCompGen/1000)
          println("tRank (s) = " + tRank/1000)
          println("tCompPrioUpdate (s) = " + tCompPrioUpdate/1000)
          println("tCompPrioFetch (s) = " + tCompPrioFetch/1000)
          println("OVERHEAD (s) = " + (tTotal/1000) )
          if (!upstreamTerminated)
            push(out, Terminate())
          //compPrioritizer.overhead()

//          while ( !compPrioritizer.isEmpty()) {
//            val comps = compPrioritizer.getBestComparisons()
//            emit(out, Comparisons(comps))
//          }
//
//          println("Best comparisons terminated")

          completeStage()
        }
      })

//     var downstreamWaiting = false
//
//      setHandler(out, new OutHandler {
//        override def onPull(): Unit = {
//          // Comparison prioritization - fetch
//          if (buffer.isEmpty && compPrioritizer.isEmpty()) {
//            downstreamWaiting = true
//          } else if (!buffer.isEmpty) {
//            val t0 = System.currentTimeMillis()
//            val elem = buffer.dequeue()
//            tCompPrioFetch += (System.currentTimeMillis() - t0)
//            push(out, elem)
//          } else {
//            val t0 = System.currentTimeMillis()
//            val comparisons = compPrioritizer.getBestComparisons()
//            tCompPrioFetch += (System.currentTimeMillis() - t0)
//            if (comparisons != null && comparisons.size > 0) {
//              push(out, Comparisons(comparisons))
//            }
//          }
//          if (!hasBeenPulled(in)) {
//            pull(in)
//          }
//        }
//      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}