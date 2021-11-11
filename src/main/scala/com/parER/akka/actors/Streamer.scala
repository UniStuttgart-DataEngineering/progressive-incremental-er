package com.parER.akka.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation

object Streamer {
  import scala.jdk.CollectionConverters._

  final case class Start(eFile: String, gtFile: String, ccer: Boolean)
  final case class Comparisons(idx: Int, profile: EntityProfile)

  def apply(): Behavior[Start] = {
    Behaviors.setup { context =>

      // spawn the other actors
      val collector = context.spawn(CollectorActor(), "CollectorActor")
      val matcher = context.spawn(MatcherActor(collector), "MatcherActor") // TODO this should be internally parallel
      val compCleaner = context.spawn(CompCleanerActor(matcher), "BFCompCleaner")
      val tokenBlocker = context.spawn(TokenBlockerActor(compCleaner), "TokenBlockerActor")
      val tokenizer = context.spawn(TokenizerActor(tokenBlocker), "TokenizerActor")

      Behaviors.receiveMessage { message =>
        // Read datasets
        val eReader = new EntitySerializationReader(message.eFile)
        val profiles = eReader.getEntityProfiles.asScala.toList
        val gtReader = new GtSerializationReader(message.gtFile)
        val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))

        println("Input Entity Profiles\t:\t" + profiles.size)
        println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

        for ((p,i) <- profiles.view.zipWithIndex) {
          tokenizer ! TokenizerActor.Profile(i, p)
        }

        Behaviors.same
      }
    }
  }
}
