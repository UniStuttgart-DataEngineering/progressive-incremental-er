package com.parER.akka.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.parER.datastructure.{BaseComparison, Comparison}

import scala.collection.mutable.ListBuffer

object CollectorActor {

  final case class Comparisons(comparisons: List[BaseComparison])

  val comparisons = new ListBuffer[BaseComparison]()

  def apply(): Behavior[Comparisons] = Behaviors.receive { (context, message) =>
    comparisons ++= message.comparisons
    apply()
  }

}
