package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, Message, Update, UpdateSeq}
import com.parER.core.blocking.{CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.prioritizing.{BlockSizePrioritizer, PQPrioritizer, Prioritizing}
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

class IncrementalProgressiveComparisonScheduling(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false) extends GraphStage[FlowShape[Seq[(Int, TokenNGrams)], Message]] {

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

      val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)
      val compGeneration = new CompGeneration
      var oldFreeTime = System.currentTimeMillis()


      val rankingCompCleaner = Ranking.apply(rankMethod)
      val compPrioritizer = new PQPrioritizer //Prioritizing.apply("pq")             // TODO change?

      var count = 0
      var originalComparisons = 0
      var prunedComparisons = 0

      tokenBlocker.setModelStoring(false)

      // The cmpList
      var cmpList = ListBuffer[BaseComparison]()

      setHandler(in, new InHandler {

        override def onPush(): Unit = {

          // Operate on increments.
          val items = grab(in)
          var TOT0, t0 = System.currentTimeMillis()

          // Token blocking and update step
          val updates = Seq.newBuilder[Update]
          val tuples = Seq.newBuilder[(Int, TokenNGrams, List[(String, List[Int])])]
          for ((i, p) <- items) {
            updates += Update(i, p)
            tuples += tokenBlocker.progressiveProcess(i, p)
          }
          tBlocking += (System.currentTimeMillis() - t0)

          // Comparison generation
          t0 = System.currentTimeMillis()
          var comps = compGeneration.emptySeqProgressiveComparisons(tuples.result())
          tCompGen += (System.currentTimeMillis() - t0)

          // Ranking (I-WNP + weighting)
          t0 = System.currentTimeMillis()
          comps = rankingCompCleaner.execute(comps)
          tRank += (System.currentTimeMillis() - t0)

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

          val mb = 1024*1024
          val runtime = Runtime.getRuntime
          val usedMemory = (runtime.totalMemory - runtime.freeMemory) / mb
          val maxMemory = runtime.maxMemory / mb
          val ratio = usedMemory*100/maxMemory
          if (ratio > 90) {
            // It removes some comparisons if ratio > 90 and with a periodicity of ~10 minutes
            val tenMinutes = 10*60*1000
            if (System.currentTimeMillis() - oldFreeTime > tenMinutes ) {
              println("ALL RESULTS IN MB")
              println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
              println("** Free Memory:  " + runtime.freeMemory / mb)
              println("** Total Memory: " + runtime.totalMemory / mb)
              println("** Max Memory:   " + runtime.maxMemory / mb)
              oldFreeTime = System.currentTimeMillis()
            }
          }

          // Total time
          tTotal += (System.currentTimeMillis() - TOT0)

          if (noUpdate) {
            emit(out, Comparisons(comparisons))
          }
          else {
            if (comparisons != null) {
              val msg = List(UpdateSeq(updates.result()), Comparisons(comparisons))
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
          println("tRank (s) = " + tRank/1000)
          println("tCompPrioUpdate (s) = " + tCompPrioUpdate/1000)
          println("tCompPrioFetch (s) = " + tCompPrioFetch/1000)
          println("OVERHEAD (s) = " + (tTotal/1000) )

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
          pull(in)
        }
      })
    }
}