import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{KillSwitches, ThrottleMode}
import akka.stream.scaladsl._
import com.parER.akka.streams.messages.{BlockTuple, Comparisons, IncrComparisons, Message, Update}
import com.parER.akka.streams.{ProcessingTokenBlockerStage, ProgressiveCollectorSink, StoreModelStage, TokenBlockerStage}
import com.parER.core.blocking.{BlockGhostingFun, CompGenerationFun}
import com.parER.core.compcleaning.{ComparisonCleaning, WNP2CompCleanerFun}
import com.parER.core.matching.Matching
import com.parER.core.{Config, Tokenizer}
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.textmodels.TokenNGrams
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object AkkaStreamSequentialCCMain {

  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.commandLine(args)

    Config.showMatches = false

    val throttleElements = Config.pOption
    val throttleMode = s"-throttle-${Config.batches}-"+throttleElements.toInt+"x"+Config.time
    Config.name = "batch"+throttleMode
    println( Config.name )

    val priority = Config.priority
    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2
    val threshold = Config.threshold

    // STEP 1. Initialization and read dataset - gt file
    val t0 = System.currentTimeMillis()
    val eFile1  = Config.mainDir + Config.getsubDir() + Config.dataset1 + "Profiles"
    val eFile2  = Config.mainDir + Config.getsubDir() + Config.dataset2 + "Profiles"
    val gtFile = Config.mainDir + Config.getsubDir() + Config.groundtruth + "IdDuplicates"

    if (Config.print) {
      println(s"Max memory: ${maxMemory} MB")
      println("File1\t:\t" + eFile1)
      println("File2\t:\t" + eFile2)
      println("gtFile\t:\t" + gtFile)
    }

    val eReader1 = new EntitySerializationReader(eFile1)
    val profiles1 = eReader1.getEntityProfiles.asScala.map((_,0)).toArray
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val eReader2 = new EntitySerializationReader(eFile2)
    val profiles2 = eReader2.getEntityProfiles.asScala.map((_,1)).toArray
    if (Config.print) System.out.println("Input Entity Profiles2\t:\t" + profiles2.size)

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    Config.nprofiles = profiles1.size + profiles2.size

    // STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("QuickStart") // TODO optional?
    implicit val ec = system.dispatcher

    val tokenizer: (((EntityProfile, Int), Long)) => (Int, TokenNGrams) = {
      case ((e,dId),id) => new Tokenizer().execute(id.toInt, dId, e)
    }

    val tokenBlocker = new ProcessingTokenBlockerStage(Config.blocker, profiles1.size, profiles2.size, Config.cuttingRatio, Config.filteringRatio)

    val ff = Config.filteringRatio

    val bGhost = Flow[Message].map( x => x match {
      case BlockTuple(id, model, blocks) => {
        val t = BlockGhostingFun.process(id, model, blocks, ff)
        BlockTuple(t._1, t._2, t._3)
      }
      case Update(_,_) => x
    })

    val cGener = Flow[Message].map( x => x match {
      case BlockTuple(id, model, blocks) => Comparisons( CompGenerationFun.generateComparisons(id, model, blocks) )
      case Update(_,_) => x
    })

    val compCl = Flow[Message].map( x => x match {
      case Comparisons(comparisons) => Comparisons( WNP2CompCleanerFun.execute(comparisons) )
      case Update(_,_) => x
    } )

    //val compCleanerFun = (lc: List[BaseComparison]) => ComparisonCleaning.apply(Config.ccMethod).execute(lc)
    //val matcherFun = (c: BaseComparison) => {c.sim = c.e1Model.getSimilarity(c.e2Model); c}
    //val matcher = (lc: List[BaseComparison]) => lc.map(matcherFun)

    //val matcherFun = Matching.getMatcher(Config.matcher)
    //val matcher = (lc: List[BaseComparison]) => lc.map(matcherFun)
    var tMatcher = 0L
    val matcherFun = Matching.getMatcher(Config.matcher)
    val matcher = (lc: List[BaseComparison]) => {
      var t0 = System.currentTimeMillis()
      val comps = lc.map(matcherFun)
      tMatcher += (System.currentTimeMillis()-t0)
      comps
    }

    val t1 = System.currentTimeMillis()
    val collector = new ProgressiveCollectorSink(t0, t1, dp, true)

    val N = Config.batches
    val num = (Config.nprofiles.toFloat/N.toFloat).ceil.toInt
    //val num = 500
    val time = Config.time
    val budget = Config.budget * 60 * 1000   // in minutes

    //val sharedKillSwitch = KillSwitches.shared("my-kill-switch")
    println(s"Number of increments ${N}")
    println(s"Increment size: ${num.toInt}")
    println(s"Increments per second: ${throttleElements.toInt}/s")

    val done = Source.combine(
      Source(profiles1).zipWithIndex,
      Source(profiles2).zipWithIndex)(Merge(_))
      .grouped(num.toInt)
      //.groupedWithin(num.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
//      .throttle(throttleElements.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
//      .statefulMapConcat{ () =>
//        var counter = 0
//
//
//        { x =>
//          counter += 1
//          val dt1 = System.currentTimeMillis() - t1
//          val s = s"$dt1,arrives,$counter,0,${Config.prioritizer}"
//          println(s"$dt1 : Increment $counter")
//          println(s)
//          x :: Nil
//        }
//
//      }
//      .groupedWithin(num.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
//      .throttle(throttleElements.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS), num.toInt*2, ThrottleMode.Shaping)
      .mapConcat(x => x)
      .map(tokenizer)
      .via(tokenBlocker)
      .via(bGhost)
      .via(cGener)
      .async
      .via(compCl)
      //.map (x => x match {
      //  case Comparisons(c) => Comparisons(compCleanerFun(c))
      //  case Update(_,_) => x
      //})
      .async
      .via(new StoreModelStage)
      //.via(sharedKillSwitch.flow)
//      .mapConcat(x => x)
//      .map(x => List(x))
      .map(matcher)
      .takeWhile(_ => System.currentTimeMillis() - t1 < budget )
      .runWith(collector)

//    Thread.sleep(budget)
//    println("Shutting down the stream...")
//    sharedKillSwitch.shutdown()

    done.onComplete( result => {
      val delta = System.currentTimeMillis() - t1
      println(s"1: ${delta} ms")
      println("tMatcher (ms) = "+tMatcher)
      system.terminate()
    })
  }
}


