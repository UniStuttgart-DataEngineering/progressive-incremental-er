package com.parER.progressive.main

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl._
import com.esotericsoftware.minlog.Log
import com.parER.akka.streams._
import com.parER.akka.streams.messages._
import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import com.parER.utils.{ExtendedProfileMatcher, GtSerializationReaderPartial, JedaiPrioritization}
import org.scify.jedai.blockbuilding.StandardBlocking
import org.scify.jedai.blockprocessing.blockcleaning.{BlockFiltering, SizeBasedBlockPurging}
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.prioritization.IPrioritization
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation
import org.scify.jedai.utilities.enumerations.{RepresentationModel, SimilarityMetric, WeightingScheme}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object AkkaJedaiProgressiveIncrementalCCMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    Log.set(Log.LEVEL_WARN)

    // Argument parsing
    Config.commandLine(args)

    println("\n\n==================")
    Config.name = Config.ranker + "-loop" + Config.prioritizer + "-" + Config.compFilter + "-" + Config.budget + "m-" + s"partition=${Config.partition}"

    var incremental = true
    var printAll = false
    val maxTimeInMinutes : Long = Config.budget // * 60L
    val maxTime : Long = maxTimeInMinutes * 60L * 1000L

    val threads = ""
    val throttleElements = Config.pOption
    val throttleMode = s"-throttle-${Config.batches}-"+throttleElements.toInt+"x"+Config.time

    if (maxTime == 0 && Config.batches > 1) {
      incremental = true
      printAll = false
      Config.name = "incremental" + Config.batches + threads
    } else if (maxTime > 0 && Config.batches >= 1) {
      incremental = false
      printAll = true
      Config.name = "batch"+throttleMode + threads
    } else {
      incremental = false
      printAll = true
      Config.name = "error"
    }

    if (Config.prioritizer == "pps-local")
      Config.local = true
    else
      Config.local = false

    println(" TEST FOR " + Config.name)
    println(" Local solution? " + Config.local)
    println("======================")


    val priority = Config.priority
    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2
    val threshold = Config.threshold
    val groupNum = 1000//Config.pOption
    val millNum = 5//Config.pOption2

    // STEP 1. Initialization and read dataset - gt file
    val t0 = System.currentTimeMillis()
    val eFile1  = Config.mainDir + Config.getsubDir() + Config.dataset1 + "Profiles"
    val eFile2  = Config.mainDir + Config.getsubDir() + Config.dataset2 + "Profiles"
    val gtFile = Config.mainDir + Config.getsubDir() + Config.groundtruth + "IdDuplicates"

    var hm = new mutable.HashMap[org.scify.jedai.datamodel.Comparison, Float]()

    if (Config.print) {
      println(s"Max memory: ${maxMemory} MB")
      println("File1\t:\t" + eFile1)
      println("File2\t:\t" + eFile2)
      println("gtFile\t:\t" + gtFile)
    }

    val eReader1 = new EntitySerializationReader(eFile1)
    var profiles1 = eReader1.getEntityProfiles.asScala.map((_,0)).toArray
    val limitD1 = profiles1.size/Config.partition
    profiles1 = profiles1.slice(0, limitD1)
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val eReader2 = new EntitySerializationReader(eFile2)
    var profiles2 = eReader2.getEntityProfiles.asScala.map((_,1)).toArray
    val limitD2 = profiles2.size/Config.partition
    profiles2 = profiles2.slice(0, limitD2)
    if (Config.print) System.out.println("Input Entity Profiles2\t:\t" + profiles2.size)

    //val gtReader = new GtSerializationReader(gtFile)
    val gtReader = new GtSerializationReaderPartial(gtFile, limitD1, limitD2)
    val dp = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    Config.nprofiles = profiles1.size + profiles2.size
    Config.nduplicates = dp.getDuplicates.size

    // STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("QuickStart") // TODO optional?
    implicit val ec = system.dispatcher

    val N = Config.batches
    val num = (Config.nprofiles.toFloat/N.toFloat).ceil.toInt
    val delta = 1000
    val subParts = ( num.toDouble / delta.toDouble ).ceil.toInt

    val comparisonBudget = 10*dp.getDuplicates.size.toDouble / (N.toDouble * subParts)
    val ignoreComparisonRequests  = maxTime == 0 && comparisonBudget > 0
    if (ignoreComparisonRequests) {
      println("Comparison budget mode.")
      println("Comparison budget: comparisonBudget")
    } else {
      println("Comparison in time mode!")
    }

    val t1 = System.currentTimeMillis()
    val time = Config.time
    val collector = new ProgressiveCollectorSinkWithIncrCount(t0, t1, dp, printAll, incremental, subParts)

    val matcherFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val sourceEmpty = Source.repeat(MessagePrioritization(null))
      val merge = b.add(MergePrioritized[MessagePrioritization](Seq(10000,1)))

      val matcherStage = b.add( Flow[MessagePrioritization].statefulMapConcat { () =>

        val pm = new ExtendedProfileMatcher(profiles1.map( el => el._1).toList.asJava, profiles2.map( el => el._1 ).toList.asJava, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY)
        var prioritization: IPrioritization = null
        var increments = 0

        { msg =>
          if (msg.prioritization != null) {
            increments += 1
            //println(s"INCREMENTO del sium ${increments}")
            prioritization = msg.prioritization
          }
          var comps = ListBuffer[BaseComparison]()
          while (prioritization != null && prioritization.hasNext && comps.isEmpty) {
            val c1 = prioritization.next()
            var similarity = 0.0f
            if (!hm.contains(c1)) {
              similarity = pm.executeComparison(c1)
              hm.update(c1, similarity)
              comps += Comparison(c1.getEntityId1, null, c1.getEntityId2, null, similarity)
            }
          }
          IncrComparisons(increments, comps.result()) :: Nil
        }

      })

      sourceEmpty ~>
        Flow[MessagePrioritization].takeWhile(_ => {
          System.currentTimeMillis() - t1 < maxTime
          //!ignoreComparisonRequests
          }
        ) ~>
        merge.in(1)

      merge.out ~>
        Flow[MessagePrioritization]
          .takeWhile(_ => {
            System.currentTimeMillis() - t1 < maxTime || ignoreComparisonRequests
          }) ~>
        matcherStage

      FlowShape(merge.in(0), matcherStage.out)
    })
    //val sharedKillSwitch = KillSwitches.shared("my-kill-switch")



    // Throttle number
    // 1000 increments in 5 min for Dag, Dda, Dmovies
    // ---> 200 increments/min ---> 3.33 increments/s
    // ---> approx 4 increments/s
    // 30000 increments in 1h for Ddbpedia
    // ---> 500 increments/min ---> 8.33 increments/s
    // ---> approx 9 increments/s for dbpedia
    println(s"Increment size: ${num.toInt}")
    println(s"Increments per second: ${throttleElements.toInt}/s")
    val done = Source.combine(
      Source(profiles1).zipWithIndex,
      Source(profiles2).zipWithIndex)(Merge(_))
      .grouped(num.toInt)
      //.groupedWithin(num.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
      .throttle(throttleElements.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
      .statefulMapConcat{ () =>
        var counter = 0


        { x =>
          counter += 1
          val dt1 = System.currentTimeMillis() - t1
          val s = s"$dt1,arrives,$counter,0,${Config.prioritizer}"
          println(s"$dt1 : Increment $counter")
          println(s)
          x :: Nil
        }

      }
      .statefulMapConcat{ () =>
        val bb = new StandardBlocking
        val bc = new SizeBasedBlockPurging(Config.cuttingRatio)
        val bf = new BlockFiltering(Config.filteringRatio)
        var p1 = new java.util.ArrayList[EntityProfile]()
        var p2 = new java.util.ArrayList[EntityProfile]()

        // we return the function that will be invoked for each element
        { lc =>
          if (Config.local) {
            p1.clear()
            p2.clear()
          }
          lc.map(x => x match {
            case ((e, dId), id) => if (dId == 0) p1.add(e)
            else p2.add(e)
          })

          var blocks =  bb.getBlocks(p1, p2)
          if (blocks.size > 0)
            blocks = bc.refineBlocks(blocks)
          if (blocks.size > 0)
            blocks = bf.refineBlocks(blocks)
          //( p1.size, p2.size, bf.refineBlocks ( bc.refineBlocks( bb.getBlocks(p1, p2) ) ) ) :: Nil
          if (blocks.size > 0)
            ( p1.size, p2.size, blocks ) :: Nil
          else
            Nil
        }
      }
      .async
      .map { triple =>
            val p1Size = triple._1
            val p2Size = triple._2
            val blocks = triple._3
            val prioritization = JedaiPrioritization.apply(Config.prioritizer, p1Size + p2Size, 1e8.toInt, WeightingScheme.CBS)
            prioritization.developBlockBasedSchedule(blocks)
            MessagePrioritization(prioritization)
      }
      .async
      .via(matcherFlow)
      .mapConcat(x => x.comparisons.map(cmps => IncrComparisons(x.incrIdx, List(cmps))))
      .runWith(collector)

    done.onComplete( result => {
      val delta = System.currentTimeMillis() - t1
      println(s"1: ${delta} ms")
      system.terminate()
    })
  }
}


