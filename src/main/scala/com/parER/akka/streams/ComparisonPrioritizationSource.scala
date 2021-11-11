package com.parER.akka.streams

import akka.actor.{Actor, ActorRef}
import com.parER.akka.streams.messages._
import com.parER.core.blocking.StoreModel
import com.parER.core.prioritizing.HPrioritizer6
import com.parER.datastructure.BaseComparison

object ComparisonPrioritizationSource {
  case object Ack
  case object StreamInitialized
  case object StreamCompleted
  final case class StreamFailure(ex: Throwable)
}

class ComparisonPrioritizationSource(ackWith: Any)  extends Actor {
  import ComparisonPrioritizationSource._

  val storeModel = new StoreModel()
  val compPrioritizer = new HPrioritizer6
  var delayComparisons = List[BaseComparison]()

  override def receive: Receive = {
    case m: ComparisonsSeq =>
      println("eheh")
      sender() ! Ack

    case m: UpdateSeqAndComparisonsSeq =>
      println("UpdateSeqAndComparisonsSeq")
      m.updateSeq.map( u => storeModel.solveUpdate(u.id, u.model) )
      for (s <- m.comparisonsSeq.comparisonSeq )
        if (s.comparisons.size > 0)
          compPrioritizer.update(s.comparisons)
      sender() ! Ack

    case ComparisonsRequest =>
      println("ComparisonsRequest")
      val lc = compPrioritizer.getBestComparisons()
      try {
          val llc = if (delayComparisons.isEmpty) storeModel.betterSolveComparisons(lc)
          else storeModel.betterSolveComparisons(delayComparisons++lc)
          sender() ! Comparisons(llc)
      }
      catch {
        case e => {
          println("Key not found exception...");
          delayComparisons ++= lc
          sender() ! Comparisons(List[BaseComparison]())
      }
    }

  }
}