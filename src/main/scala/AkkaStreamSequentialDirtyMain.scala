import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import com.parER.akka.streams.messages.{Comparisons, Update}
import com.parER.akka.streams.{ProgressiveCollectorSink, StoreModelStage, TokenBlockerStage}
import com.parER.core.compcleaning.ComparisonCleaning
import com.parER.core.matching.Matching
import com.parER.core.{Config, Tokenizer}
import com.parER.datastructure.{BaseComparison, Comparison}
import com.parER.utils.UnilateralDuplicatePropagation
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.textmodels.TokenNGrams

import scala.concurrent.duration.FiniteDuration

object AkkaStreamSequentialDirtyMain {

  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.commandLine(args)

    val throttleElements = Config.pOption
    val throttleMode = "-throttle-"+throttleElements.toInt
    Config.name = "batch"+throttleMode
    println( Config.name )

    val priority = Config.priority
    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2
    val threshold = Config.threshold

    // STEP 1. Initialization and read dataset - gt file
    val t0 = System.currentTimeMillis()
    val eFile1  = Config.mainDir + Config.getsubDir() + Config.dataset1 + "Profiles"
    val gtFile = Config.mainDir + Config.getsubDir() + Config.groundtruth + "IdDuplicates"

    if (Config.print) {
      println(s"Max memory: ${maxMemory} MB")
      println("File1\t:\t" + eFile1)
      println("gtFile\t:\t" + gtFile)
    }

    val eReader1 = new EntitySerializationReader(eFile1)
    val profiles1 = eReader1.getEntityProfiles.asScala.map((_,0)).toArray
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    Config.nprofiles = profiles1.size

    // STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("QuickStart") // TODO optional?
    implicit val ec = system.dispatcher

    // TODO in logic code Tokenizer and HSCompCleaner as objects? If objects problems in inner parallelization?
    val tokenizer: (((EntityProfile, Int), Long)) => (Int, TokenNGrams) = {
      case ((e,dId),id) => new Tokenizer().execute(id.toInt, dId, e)
    }

    val tokenBlocker = new TokenBlockerStage(Config.blocker, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
    val compCleaner = (lc: List[BaseComparison]) => ComparisonCleaning.apply(Config.ccMethod).execute(lc)
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
    val time = 1000
    val budget = Config.budget * 60 * 1000   // in minutes

    println(s"Number of increments ${N}")
    println(s"Increment size: ${num.toInt}")
    println(s"Increments per second: ${throttleElements.toInt}/s")

    val done =
        Source(profiles1).zipWithIndex
        .grouped(num.toInt)
        .throttle(throttleElements.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
        .mapConcat(x => x)
        .map(tokenizer)
        .via(tokenBlocker)
        .map (x => x match {
          case Comparisons(c) => new Comparisons(compCleaner(c))
          case Update(_,_) => x
        })
        .async
        .via(new StoreModelStage)
        .map(matcher)
        .takeWhile(_ => System.currentTimeMillis() - t1 < budget )
        .runWith(collector)



    done.onComplete( result => {
      val delta = System.currentTimeMillis() - t1
      println(s"1: ${delta} ms")
      println("tMatcher (ms) = "+tMatcher)
      system.terminate()
    })
  }
}


