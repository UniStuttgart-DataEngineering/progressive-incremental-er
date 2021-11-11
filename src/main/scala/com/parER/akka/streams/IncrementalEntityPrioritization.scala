package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages._
import com.parER.akka.streams.utils.RatingControl
import com.parER.core.Config
import com.parER.core.entityscheduling.EntityPrioritizer
import com.parER.core.prioritizing.HPrioritizer7

class IncrementalEntityPrioritization(val ignoreComparisonRequest: Boolean = false, val comparisonBudget: Int = 1000) extends GraphStage[FlowShape[Message, Message]] {

  println(s"Mode ignoreComparisonRequest: $ignoreComparisonRequest")
  println(s"Comparison budget x increment: $comparisonBudget")

  val in = Inlet[Message]("IncrementalEntityPrioritization.in")
  val out = Outlet[Message]("IncrementalEntityPrioritization.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val minutes = Config.prioPrinting
      val rc = new RatingControl(1000)
      val rc2 = new RatingControl(1000)
      var tCompPrioUpdateFetch = 0L
      val compPrioritizer = new EntityPrioritizer
      var tOrigin, tNow, tUpdate, tRequest, tPull, tUpdate2 = System.currentTimeMillis()
      var aUpdate, aRequest = 0.0f
      var cUpdate, cRequest, totCRequest = 0.0f
      var upstreamTerminated, noMoreComparisons = false
      var totBlockComparisons, messageComparisons = 0.0f
      var emittedComparisons = 0.0f
      var countAlmostEmpty = 0
      var noBlockComparisons = false

      // debug?
      var terminationMessages = 0
      var pullInRequests = 0

      var K : Int = 1
      var maxK = 1

      var creqCount = 0
      var upCount = 0

      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          val msg : Message = grab(in)
          //println("ieps: Received msg")
          val t0 = System.currentTimeMillis()

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

          if (t0 - tNow > minutes *  1000 * 60 || (t0 - tNow > minutes *  1000 * 60 && upstreamTerminated)) {
            println(s"IEPS: min ${(t0-tOrigin)/(1000 * 60)}; K = $K")
//            println(s"IEPS: messageComparisons: ${messageComparisons}")
//            println(s"IEPS: totBlockComparisons: ${totBlockComparisons}")
//            println(s"IEPS: comparisons request: ${totCRequest}")
//            println(s"IEPS: executed comparisons request: ${cRequest}")
            println(s"IEPS: Upstream ts: ${rc.getUSTime}")
            println(s"IEPS: comparisonsRequest ts: ${rc2.getUSTime}")
            println(s"IEPS: DS time: ${rc.getDSTime}")
            println(s"IEPS: TS time: ${rc.getTSTime}")
//            println(s"IEPS: emittedComparisons: $emittedComparisons")
//            println(s"IEPS: TopK: ${rc2.getTSTime}")
//            println(s"IEPS: countAlmostEmpty: ${countAlmostEmpty}")
            println(s"IEPS: time overhead onPush (s): ${tCompPrioUpdateFetch/1000}")
            println(s"IEPS: termination messages: ${terminationMessages}")
            println(s"IEPS: pullin messages: ${pullInRequests}")

            emittedComparisons = 0
            compPrioritizer.overhead()
            tNow = t0
          }

          val (u, c) = msg match {
            case NoBlockComparisons() =>
              noBlockComparisons = true
              (null, null)
            case MessageComparisons(blockComparisons) =>
//              if (blockComparisons.size == 0) {
//                println(s"IEPS: block comparisons vuote... $messageComparisons")
//                println(s"IEPS: emittedMultiple ... ${emittedComparisons}")
//              }
              // todo modify K?
              //println(s"receive block message... + $K + ... size ${blockComparisons.size}")
              messageComparisons += 1
              totBlockComparisons += blockComparisons.size
              rc.updateUSTime(t0-tUpdate)
              tUpdate = t0
              compPrioritizer.setPruning(false)
              compPrioritizer.update(blockComparisons)
              //noMoreComparisons = false
              //println("Receicevd block message. Is empty? " + compPrioritizer.isEmpty())
              //compPrioritizer.overhead()
              (null, compPrioritizer.getTopComparisons(K))
            case UpdateSeqAndMessageComparisonsSeq(updateSeq, comparisonsSeq) =>
              upCount += 1
              //println(s"UpdateMessage ${upCount}; comparisons from requests ${creqCount}")
              rc.updateUSTime(t0-tUpdate)
              tUpdate = t0
              compPrioritizer.setPruning(true)
              for (s <- comparisonsSeq.comparisonSeq)
                if (s.comparisons.size > 0)
                  compPrioritizer.update(s.comparisons)
              (updateSeq, compPrioritizer.getTopComparisons(K))
            case ComparisonsRequest(size) =>
              rc2.updateUSTime(t0-tUpdate2)
              tUpdate2 = t0
              totCRequest += 1
              if (upstreamTerminated || rc.getTSTime < rc.getUSTime || noMoreComparisons) {
                cRequest += 1
                var time0 = System.currentTimeMillis()
                val p = (null, compPrioritizer.getTopComparisons(K))
                creqCount += p._2.size
                rc2.updateTSTime(System.currentTimeMillis()-time0)
                if (upstreamTerminated) {
                  //if (noMoreComparisons && !compPrioritizer.isEmpty()) {
                    //println("IT IS NOT EMPTY")
                  //}
                  if (!noMoreComparisons && compPrioritizer.isEmpty()) {
                    //println("IT IS EMPTY")
                    Config.isEmpty = true
                    noMoreComparisons = true
                  }
                }
//                if (noMoreComparisons) {
//                  println("CompPrioritizer is empty? " + compPrioritizer.isEmpty())
//                  println(s"Mando comparisons: ${p._2.size}")
//                }
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
              (null, compPrioritizer.getBestComparisons())
          }
          // TODO
          val delta = System.currentTimeMillis() - t0
          //println(s"ieps: time $delta ms")
          rc.updateTSTime(delta)
          tCompPrioUpdateFetch += delta

          if (upstreamTerminated && compPrioritizer.almostEmpty(K)) {
            countAlmostEmpty += 1
          }

          //println("ieps: Send msg")

          if (upstreamTerminated && ignoreComparisonRequest) {
            emit(out, Terminate())
            completeStage()
          } else if (!noBlockComparisons && (upstreamTerminated && compPrioritizer.almostEmpty(K) || noMoreComparisons && compPrioritizer.isEmpty())) {
            terminationMessages += 1
            if (c != null && c.nonEmpty) {
              emittedComparisons += c.size
              emit(out, MessageComparisons(c))
            }
            emit(out, Terminate())
          } else if (u != null) {
            if (c != null) emittedComparisons += c.size
            push(out, UpdateSeqAndMessageCompSeq(u, c))
          } else if (c != null && c.nonEmpty) {
            emittedComparisons += c.size
            push(out, MessageComparisons(c))
          } else if (!hasBeenPulled(in)) {
            pullInRequests += 1
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          println("Upstream finished")
          println("tCompPrioUpdate (s) = " + tCompPrioUpdateFetch/1000)
          compPrioritizer.overhead()
          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          val t0 = System.currentTimeMillis()
          rc.updateDSTime(t0-tPull)
          tPull = t0
          pull(in)
        }
      })
    }
}