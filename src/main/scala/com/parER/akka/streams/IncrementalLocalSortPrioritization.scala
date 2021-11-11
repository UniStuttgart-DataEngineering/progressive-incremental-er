package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages._
import com.parER.akka.streams.utils.RatingControl
import com.parER.core.Config
import com.parER.core.prioritizing.LightWeightPQPrioritizer
import com.parER.datastructure.LightWeightComparison

class IncrementalLocalSortPrioritization(val ignoreComparisonRequest: Boolean = false, val comparisonBudget: Int = 1000) extends GraphStage[FlowShape[Message, Message]] {

  println(s"Mode ignoreComparisonRequest: $ignoreComparisonRequest")
  println(s"Comparison budget x increment: $comparisonBudget")

  val in = Inlet[Message]("IncrementalComparisonPrioritization.in")
  val out = Outlet[Message]("IncrementalComparisonPrioritization.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val minutes = Config.prioPrinting

      val rc = new RatingControl(1000)
      var tCompPrioUpdateFetch = 0L
      //val compPrioritizer = new LightWeightPQPrioritizer
      var tOrigin, tNow, tUpdate, tRequest, tPull = System.currentTimeMillis()
      var aUpdate, aRequest = 0.0f
      var cUpdate, cRequest, totCRequest = 0.0f
      var upstreamTerminated, noMoreComparisons = false
      var noBlockComparisons = false

      var comparisons = List[LightWeightComparison]()

      var K : Int = 1
      var maxK = 1

      var creqCount = 0
      var upCount = 0
      var totalNumberOfComparisons = 0

      var currIndex = 0

      def getTopComparisons(K: Int) = {
        currIndex += K
        if (currIndex < comparisons.size)
          comparisons.slice(currIndex-K, currIndex)
        else if (currIndex-K > 0 && currIndex-K > comparisons.size)
          comparisons.slice(currIndex-K, comparisons.size)
        else
          List()
      }

      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          val msg : Message = grab(in)
          val t0 = System.currentTimeMillis()

          // service time
          val ts = Math.max(rc.getTSTime, rc.getDSTime)
          val maxValue = (rc.getUSTime/ts).toInt

          if (ignoreComparisonRequest && comparisonBudget > 0) {
            K = comparisonBudget
          } else if ( upstreamTerminated ) {
            K = maxK
          } else {
            K += (if (rc.getUSTime / ts > 1.0f) 1 else -1)
            K = Math.max(1, K)
            if (maxValue >= 1)
              K = Math.min(K, maxValue)

            maxK = if (K > maxK) K else maxK
          }

          // TODO da cancellare
          //K *= 10

//          val compNum = Math.max(rc.getTSTime, rc.getUSTime)
//          K += (if (rc.getUSTime/rc.getTSTime > 1.0f) 1 else -1)
//          K = Math.max(1, K)
//          val maxValue = Math.max( (rc.getDSTime/rc.getTSTime).toInt, (rc.getUSTime/rc.getTSTime).toInt )
//          if (maxValue >= 1)
//            K = Math.min(maxValue, K)


          //val K : Int =
          //val K : Int = 1 + (compNum / rc.getDSTime).floor.toInt

          if (t0 - tNow > minutes *  1000 * 60) {
//            println(s"[HPrioritizer6:] Running ... min... ${(t0-tOrigin)/(1000 * 60)}")
//            println(s"[HPrioritizer6:] DS avg time = ${rc.getDSTime}")
//            println(s"[HPrioritizer6:] US avg time = ${rc.getUSTime}")
//            println(s"[HPrioritizer6:] TS avg time = ${rc.getTSTime}")
//            println(s"[HPrioritizer6:] TS/DS = ${rc.getTSTime/rc.getDSTime}")
//            println(s"[HPrioritizer6:] US/TS = ${rc.getUSTime/rc.getTSTime}")
//            println(s"[HPrioritizer6:] K = ${K}")
//            println(s"[HPrioritizer6:] maxK = ${maxK}")
//            println(s"[HPrioritizer6:] cRequest executed = ${cRequest}")
//            println(s"[HPrioritizer6:] totCRequest executed = $totCRequest")
//            println(s"[HPrioritizer6: OVERHEAD] tCompPrioUpdateFetch (s) = ${tCompPrioUpdateFetch/1000}")
            println(s"ICPS: min ${(t0-tOrigin)/(1000 * 60)}; K = $K")
            //compPrioritizer.overhead()
            tNow = t0
          }

          val (u, c) = msg match {
            case NoBlockComparisons() =>
              noBlockComparisons = true
              (null, null)
            case MessageComparisons(blockComparisons) =>
              comparisons = blockComparisons.sortWith(_.sim > _.sim)
              (null, getTopComparisons(K))
            case UpdateSeqAndMessageComparisonsSeq(updateSeq, comparisonsSeq) =>
              upCount += 1
              println(s"UpdateMessage ${upCount}; K = ${K} comparisons from requests ${creqCount}")
              println(s"Total # of comparisons sent = ${totalNumberOfComparisons}")
              println(s"Resetting current list of comparisons of size ${comparisons.size} and currIndex=${currIndex} ")
              println(s"Number of comparison requests: $totCRequest")
              rc.updateUSTime(t0-tUpdate)
              tUpdate = t0
              currIndex = 0
              totCRequest = 0 // reset here for debug purposes, TODO remove.
              comparisons = comparisonsSeq.comparisonSeq.toList.flatMap(_.comparisons).sortWith(_.sim > _.sim)
              (updateSeq, getTopComparisons(K))
            case ComparisonsRequest(size) =>
              totCRequest += 1
              if (upstreamTerminated || rc.getTSTime < rc.getUSTime || noMoreComparisons) {
                val p = (null, getTopComparisons(K))
                creqCount += p._2.size
                //println("receive cr message... " + K)
                if (upstreamTerminated) {
                  if (!noMoreComparisons && currIndex >= comparisons.size) {
                    println("IT IS EMPTY")
                    Config.isEmpty = true
                    noMoreComparisons = true
                  }
                }
                p
              } else
                (null, null)
            case Terminate() =>
              println("------- stream consumed -----------")
              upstreamTerminated = true
              Config.upstreamTerminated = true
              (null, null)
            case _ =>
              println("unexpected")
              (null, getTopComparisons(K))
          }
          // TODO
          val delta = System.currentTimeMillis() - t0
          rc.updateTSTime(delta)
          tCompPrioUpdateFetch += delta

          if (c != null)
            totalNumberOfComparisons += c.size

          if (upstreamTerminated && ignoreComparisonRequest) {
            emit(out, Terminate())
            completeStage()
          } else if (!noBlockComparisons && (noMoreComparisons && currIndex >= comparisons.size)) {
            if (c != null && c.nonEmpty)
              emit(out, MessageComparisons(c))
            emit(out, Terminate())
          } else if (u != null) {
            //tPull = System.currentTimeMillis()
            //rc.updateTSTime(delta)
            push(out, UpdateSeqAndMessageCompSeq(u, c))
          } else if (c != null && c.nonEmpty) {
            //tPull = System.currentTimeMillis()
            //rc.updateTSTime(delta)
            push(out, MessageComparisons(c))
          } else if (!hasBeenPulled(in))
            pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          println("Upstream finished")
          println("tCompPrioUpdate (s) = " + tCompPrioUpdateFetch/1000)
          //compPrioritizer.overhead()
          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          val t0 = System.currentTimeMillis()
          rc.updateDSTime(t0-tPull)
          tPull = t0
          //val delta = Math.max(System.currentTimeMillis() - tPull, 1)
          //rc.updateDSTime(delta)
          pull(in)
        }
      })
    }
}