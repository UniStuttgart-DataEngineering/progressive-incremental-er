package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update, UpdateSeq}
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.prioritizing.HPrioritizer5
import com.parER.core.ranking.{WNP2CBSRanker, WNP2JSRanker}
import org.scify.jedai.textmodels.TokenNGrams

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

class IncrementalAdvancedProgressiveComparisonScheduling(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

  val in = Inlet[Seq[(Int, TokenNGrams)]]("IncrementalProgressiveComparisonScheduling.in")
  val out = Outlet[Message]("IncrementalProgressiveComparisonScheduling.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // For time measurements
      var tTotal = 0L
      var tBlocking = 0L
      var tCompGen = 0L
      var tRank = 0L
      var tCompPrioUpdate = 0L
      var tCompPrioFetch = 0L

      var tOrigin, tNow = System.currentTimeMillis()
      var count = 0L

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)  // it performs block pruning
      val blockGhosting = new BlockGhosting(ff)
      val compGeneration = new CompGeneration
      var oldFreeTime = System.currentTimeMillis()

      val rankingCompCleaner = new WNP2CBSRanker //new WNP2JSRanker //Ranking.apply(rankMethod)
      val compPrioritizer = new HPrioritizer5 //new HPrioritizer4 //new HPrioritizer3 //new HPrioritizer2 //new HPrioritizer //new PQPrioritizer //Prioritizing.apply("pq")             // TODO change?

      tokenBlocker.setModelStoring(false)
      println(s"ro=$ro")
      println(s"ff=$ff")

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
          val tuples = Seq.newBuilder[(Int, TokenNGrams, List[(String, List[Int])])]
          for ((i, p) <- items) {
            updates += Update(i, p)
            tuples += blockGhosting.progressiveProcess(i, p, tokenBlocker.progressiveProcess(i, p)._3)
          }
          tBlocking += (System.currentTimeMillis() - t0)

          // Comparison generation
//          t0 = System.currentTimeMillis()
//          var comps = compGeneration.emptySeqProgressiveComparisons(tuples.result())
//          tCompGen += (System.currentTimeMillis() - t0)

          t0 = System.currentTimeMillis()
          var pairs = compGeneration.emptySeqProgressiveComparisonsToPairs(tuples.result())
          tCompGen += (System.currentTimeMillis() - t0)

          // Ranking (I-WNP + weighting)
          t0 = System.currentTimeMillis()
          var comps = rankingCompCleaner.executePairs(pairs)
          tRank += (System.currentTimeMillis() - t0)

          // Until here is equal to IncrementalProgressiveComparisonScheduling
          // Here in the following we use the compPrioritizer that is an HPrioritizer

          // Comparison prioritizatiion - update
          if (comps.size > 0) {
            t0 = System.currentTimeMillis()
            compPrioritizer.update(comps)
            tCompPrioUpdate += (System.currentTimeMillis() - t0)
          }

          // Comparison prioritization - fetch
          t0 = System.currentTimeMillis()
          val comparisons = compPrioritizer.getBestComparisons()
          tCompPrioFetch += (System.currentTimeMillis() - t0)

//          val mb = 1024*1024
//          val runtime = Runtime.getRuntime
//          val usedMemory = (runtime.totalMemory - runtime.freeMemory) / mb
//          val maxMemory = runtime.maxMemory / mb
//          val ratio = usedMemory*100/maxMemory
//          if (ratio > 90) {
//            // It removes some comparisons if ratio > 90 and with a periodicity of ~10 minutes
//            val tenMinutes = 10*60*1000
//            if (System.currentTimeMillis() - oldFreeTime > tenMinutes ) {
//              println("ALL RESULTS IN MB")
//              println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
//              println("** Free Memory:  " + runtime.freeMemory / mb)
//              println("** Total Memory: " + runtime.totalMemory / mb)
//              println("** Max Memory:   " + runtime.maxMemory / mb)
//              compPrioritizer.free()
//              oldFreeTime = System.currentTimeMillis()
//            }
//          }

          // Total time
          tTotal += (System.currentTimeMillis() - TOT0)

          if (noUpdate) {
            emit(out, Comparisons(comparisons))
          }
          else {
            if (comparisons != null && comparisons.size > 0) {
              val msg = List(UpdateSeq(updates.result()), Comparisons(comparisons))
              emitMultiple[Message](out, msg)
            }
            else {
              emit(out, UpdateSeq(updates.result()))
            }
          }
        }

        override def onUpstreamFinish(): Unit = {

          println("Upstream finished")
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


      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          // Comparison prioritization - fetch
          val t0 = System.currentTimeMillis()
          val comparisons = compPrioritizer.getBestComparisons()
          tCompPrioFetch += (System.currentTimeMillis() - t0)
          if (comparisons != null && comparisons.size > 0) {
            emit(out, Comparisons(comparisons))
          }
          if (!hasBeenPulled(in)) {
            pull(in)
          }
        }
      })
    }
}