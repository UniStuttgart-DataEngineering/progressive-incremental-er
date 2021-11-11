package com.parER.akka.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.parER.core.matching.JSMatcher
import com.parER.datastructure.{BaseComparison, Comparison}

object MatcherActor {
  final case class Comparisons(comparisons: List[BaseComparison])
  def apply(next: ActorRef[CollectorActor.Comparisons]): Behavior[Comparisons] = {
    Behaviors.setup(context => new MatcherActor(next, context))
  }
}

private class MatcherActor(next: ActorRef[CollectorActor.Comparisons], context: ActorContext[MatcherActor.Comparisons]) extends AbstractBehavior[MatcherActor.Comparisons](context) {
  import MatcherActor._

  private val matcher = new JSMatcher()

  override def onMessage(message: Comparisons): Behavior[Comparisons] = {
    //println("total:\t" + matcher.counter + "\t\tby " + Thread.currentThread.getId )
    next ! CollectorActor.Comparisons(matcher.execute(message.comparisons))
    this
  }

}
