package com.parER.akka.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.parER.core.Tokenizer
import org.scify.jedai.datamodel.EntityProfile

object TokenizerActor {

  final case class Profile(idx: Int, profile: EntityProfile)

  def apply(next: ActorRef[TokenBlockerActor.TokenizedProfile]): Behavior[Profile] = Behaviors.receive { (context, message) =>
    val (i, p) = (new Tokenizer()).execute(message.idx, 0, message.profile)
    next ! TokenBlockerActor.TokenizedProfile(i , p)
    Behaviors.same
  }

}
