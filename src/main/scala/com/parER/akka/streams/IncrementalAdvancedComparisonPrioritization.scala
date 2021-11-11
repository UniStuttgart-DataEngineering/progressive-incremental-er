package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages._
import com.parER.akka.streams.utils.RatingControl
import com.parER.core.Config
import com.parER.core.prioritizing.{HPrioritizer7, HPrioritizer8}

class IncrementalAdvancedComparisonPrioritization(val ignoreComparisonRequest: Boolean = false, val comparisonBudget: Int = 1000) extends GraphStage[FlowShape[Message, Message]] {

  println(s"Mode ignoreComparisonRequest: $ignoreComparisonRequest")
  println(s"Comparison budget x increment: $comparisonBudget")

  val in = Inlet[Message]("IncrementalAdvancedComparisonPrioritization.in")
  val out = Outlet[Message]("IncrementalAdvancedComparisonPrioritization.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val minutes = Config.rankPrinting

      val rc = new RatingControl(1000)
      var tCompPrioUpdateFetch = 0L
      val compPrioritizer = new HPrioritizer8 //new HPrioritizer7 //new HPrioritizer6 // HPrioritizer5 //new HPrioritizer4 //new HPrioritizer3 //new HPrioritizer2 //new HPrioritizer //new PQPrioritizer //Prioritizing.apply("pq")             // TODO change?
      var tOrigin, tNow, tUpdate, tRequest, tPull = System.currentTimeMillis()
      var aUpdate, aRequest = 0.0f
      var cUpdate, cRequest, totCRequest = 0.0f
      var upstreamTerminated, noMoreComparisons = false
      var noBlockComparisons = false

      var K : Int = 1
      var maxK = 1

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
          //K = 100

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
            println(s"IACPS: min ${(t0-tOrigin)/(1000 * 60)}; K = $K")
            compPrioritizer.overhead()
            tNow = t0
          }

          val (u, c) = msg match {
            case NoBlockComparisons() =>
              noBlockComparisons = true
              (null, null)
            case MessageComparisons(blockComparisons) =>
              //println("received block comparisons...")
              compPrioritizer.pruning = false
              compPrioritizer.update(blockComparisons)
              (null, compPrioritizer.getTopComparisons(K))
            case UpdateSeqAndMessageComparisonsSeq(updateSeq, comparisonsSeq) =>
              rc.updateUSTime(t0-tUpdate)
              tUpdate = t0
              compPrioritizer.pruning = true
              for (s <- comparisonsSeq.comparisonSeq)
                if (s.comparisons.size > 0)
                  compPrioritizer.update(s.comparisons)
              //compPrioritizer.refine()
              (updateSeq, compPrioritizer.getTopComparisons(K))
//            case UpdateSeqAndComparisonsSeq(updateSeq, comparisonsSeq) =>
//              rc.updateUSTime(t0-tUpdate)
//              tUpdate = t0
//              for (s <- comparisonsSeq.comparisonSeq)
//                if (s.comparisons.size > 0)
//                  compPrioritizer.update(s.comparisons)
//              //compPrioritizer.refine()
//              (updateSeq, compPrioritizer.getTopComparisons(K))
            case ComparisonsRequest(size) =>
              totCRequest += 1
              if (upstreamTerminated || rc.getTSTime < rc.getUSTime || noMoreComparisons) {
                cRequest += 1
                val p = (null, compPrioritizer.getTopComparisons(K))
                if (upstreamTerminated) {
                  if (!noMoreComparisons && compPrioritizer.isEmpty()) {
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
              (null, compPrioritizer.getBestComparisons())
          }
          // TODO
          val delta = System.currentTimeMillis() - t0
          rc.updateTSTime(delta)
          tCompPrioUpdateFetch += delta

          if (upstreamTerminated && ignoreComparisonRequest) {
            emit(out, Terminate())
            completeStage()
          } else if (!noBlockComparisons && (noMoreComparisons && compPrioritizer.isEmpty())) {
            if (c != null && c.nonEmpty)
              emit(out, MessageComparisons(c))
            emit(out, Terminate())
          }
          else if (u != null) {
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
          compPrioritizer.overhead()
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