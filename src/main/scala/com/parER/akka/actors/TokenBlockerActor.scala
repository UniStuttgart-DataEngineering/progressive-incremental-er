package com.parER.akka.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.parER.core.blocking.TokenBlocker
import org.scify.jedai.textmodels.TokenNGrams


object TokenBlockerActor {
  final case class TokenizedProfile(idx: Int, profile: TokenNGrams)
  def apply(next: ActorRef[CompCleanerActor.Comparisons]): Behavior[TokenizedProfile] = {
    Behaviors.setup(context => new TokenBlockerActor(next, context))
  }
}

private class TokenBlockerActor(next: ActorRef[CompCleanerActor.Comparisons], context: ActorContext[TokenBlockerActor.TokenizedProfile]) extends AbstractBehavior[TokenBlockerActor.TokenizedProfile](context) {
  import TokenBlockerActor._

  private val tokenBlocker = new TokenBlocker()

  override def onMessage(message: TokenizedProfile): Behavior[TokenizedProfile] = {
    //println("\t\tby " + Thread.currentThread.getId )
    next ! CompCleanerActor.Comparisons(tokenBlocker.execute(message.idx, message.profile))
    this
  }

}