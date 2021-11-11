package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update, UpdateSeq, UpdateSeqAndCompSeq}
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.prioritizing.HPrioritizer6
import com.parER.core.ranking.WNP2CBSRanker
import com.parER.datastructure.BaseComparison
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

class IncrementalAdvancedProgressiveComparisonScheduling2(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

  val in = Inlet[Seq[(Int, TokenNGrams)]]("IncrementalProgressiveComparisonScheduling2.in")
  val out = Outlet[Message]("IncrementalProgressiveComparisonScheduling2.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // Message buffer
      val buffer = mutable.Queue[Message]()

      // For time measurements
      var tTotal = 0L
      var tBlocking = 0L
      var tCompGen = 0L
      var tRank = 0L
      var tCompPrioUpdate = 0L
      var tCompPrioFetch = 0L

      var tOrigin, tNow = System.currentTimeMillis()
      var count = 0L
      var cBlocker = 0L
      var cCompCleaner = 0L
      var sent = 0L

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)  // it performs block pruning
      val blockGhosting = new BlockGhosting(ff)
      val compGeneration = new CompGeneration
      var oldFreeTime = System.currentTimeMillis()

      val rankingCompCleaner = new WNP2CBSRanker //new WNP2JSRanker //Ranking.apply(rankMethod)
      val compPrioritizer = new HPrioritizer6 // HPrioritizer5 //new HPrioritizer4 //new HPrioritizer3 //new HPrioritizer2 //new HPrioritizer //new PQPrioritizer //Prioritizing.apply("pq")             // TODO change?

      tokenBlocker.setModelStoring(false)
      println(s"ro=$ro")
      println(s"ff=$ff")

      //override def preStart(): Unit = {
        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        //pull(in)
      //}

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Operate on increments.
          val items = grab(in)
          var TOT0, t0 = System.currentTimeMillis()

          count += items.size
          if (t0 - tNow > 1000 * 60) {
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
            println("tCompPrioUpdate (s) = " + tCompPrioUpdate/1000)
            println("tCompPrioFetch (s) = " + tCompPrioFetch/1000)
            println("OVERHEAD (s) = " + (tTotal/1000) )
            compPrioritizer.overhead()
            println("***** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
            tNow = t0
          }

          // Token blocking and update step
          t0 = System.currentTimeMillis()
          val updates = Seq.newBuilder[Update]
          val tuples = Seq.newBuilder[(Int, TokenNGrams, List[List[Int]])]
          for ((i, p) <- items) {
            updates += Update(i, p)
            val t = tokenBlocker.process(i, p)
            tuples += blockGhosting.process(t._1, t._2, t._3)
          }
          tBlocking += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          val seqCmpsList = Seq.newBuilder[(Int, List[BaseComparison])]
          for ((i, p, bl) <- tuples.result()) {
            val pair = (p.getDatasetId, compGeneration.generateComparisonsWithoutTextModel(i, p, bl))
            cBlocker += pair._2.size
            seqCmpsList += pair
          }
          tCompGen += (System.currentTimeMillis() - t0)

          // Ranking (I-WNP + weighting)
          t0 = System.currentTimeMillis()
          val seqRankedCmpsList = Seq.newBuilder[List[BaseComparison]]
          for ((dId, comps) <- seqCmpsList.result()) {
            val rankedComps = if (dId == 0)
                rankingCompCleaner.executeWithFunc(comps, _.e2)
              else
                rankingCompCleaner.executeWithFunc(comps, _.e1)
            cCompCleaner += rankedComps.size
            seqRankedCmpsList += rankedComps
          }
          tRank += (System.currentTimeMillis() - t0)

          // Until here is equal to IncrementalProgressiveComparisonScheduling
          // Here in the following we use the compPrioritizer that is an HPrioritizer

          // Comparison prioritizatiion - update
          t0 = System.currentTimeMillis()
          for (comps <- seqRankedCmpsList.result()) {
            if (comps.size > 0)
              compPrioritizer.update(comps)
          }
          tCompPrioUpdate += (System.currentTimeMillis() - t0)

          // Comparison prioritization - fetch
          t0 = System.currentTimeMillis()
          val comparisons = compPrioritizer.getBestComparisons()
          tCompPrioFetch += (System.currentTimeMillis() - t0)

          // Total time
          tTotal += (System.currentTimeMillis() - TOT0)

          push(out, ( if (noUpdate) {
            Comparisons(comparisons)
          }
          else {
            if (comparisons != null && comparisons.size > 0) {
              UpdateSeqAndCompSeq(updates.result(), comparisons)
            }
            else {
              UpdateSeq(updates.result())
            }
          }))

//          if (downstreamWaiting) {
//            downstreamWaiting = false
//            val bufferedElem = buffer.dequeue()
//            push(out, bufferedElem)
//          }

          //pull(in)
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
          compPrioritizer.overhead()

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