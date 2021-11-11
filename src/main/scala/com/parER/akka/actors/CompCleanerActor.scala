package com.parER.akka.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.parER.core.compcleaning.HSCompCleaner
import com.parER.datastructure.{BaseComparison, Comparison}

object CompCleanerActor {
  final case class Comparisons(comparisons: List[BaseComparison])

  def apply(next: ActorRef[MatcherActor.Comparisons]): Behavior[Comparisons] = Behaviors.receive { (context, message) =>
    next ! MatcherActor.Comparisons(new HSCompCleaner().execute(message.comparisons))
    Behaviors.same
  }
}
