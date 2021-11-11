import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.FlowShape
import akka.stream.scaladsl._
import com.parER.akka.streams._
import com.parER.akka.streams.messages._
import com.parER.core.compcleaning.{BFFilter, NoneFilter, ScalableBFFilter}
import com.parER.core.matching.Matching
import com.parER.core.{Config, TokenizerFun}
import com.parER.utils.{GtSerializationReaderPartial, UnilateralDuplicatePropagation}
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.textmodels.TokenNGrams

import scala.concurrent.duration.FiniteDuration

object AkkaProgressiveIncrementalDirtyMain {

  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.commandLine(args)
    Config.showMatches = false

    println("\n\n==================")
    Config.name = Config.ranker + "-loop" + Config.prioritizer + "-" + Config.compFilter + "-" + Config.budget + "m-" + s"partition=${Config.partition}"

    var incremental = true
    var printAll = false
    val maxTimeInMinutes : Long = Config.budget // * 60L
    val maxTime : Long = maxTimeInMinutes * 60L * 1000L

    val threads = ""
    val throttleElements = Config.pOption
    val throttleMode = "-throttle-"+throttleElements.toInt
    //val throttleMode = s"-${Config.batches}-"

    if (maxTime == 0 && Config.batches > 0) {
      incremental = true
      printAll = false
      Config.name = "incremental" + Config.batches + threads
    } else if (maxTime > 0 && Config.batches > 0) {
      incremental = false
      printAll = true
      Config.name = "batch"+throttleMode + threads
    } else {
      incremental = false
      printAll = true
      Config.name = "error"
    }

    println(" TEST FOR " + Config.name)
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

    val gtReader = new GtSerializationReader(gtFile)
    //val gtReader = new GtSerializationReaderPartial(gtFile, limitD1, limitD2)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    Config.nprofiles = profiles1.size
    Config.nduplicates = dp.getDuplicates.size

    // STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("QuickStart") // TODO optional?
    implicit val ec = system.dispatcher

    val tokenizer = (lc: Seq[((EntityProfile, Int), Long)]) => {
      lc.map(x => x match {
        case ((e, dId), id) => TokenizerFun.execute(id.toInt, dId, e)
      })
    }



    //val tokenBlocker = new IncrementalComparisonRanking(Config.ranker, Config.prioritizer, profiles1.size, profiles2.size, Config.cuttingRatio, Config.filteringRatio)
    //val tokenBlocker = new IncrementalComparisonRankingWithBJS(Config.ranker, Config.prioritizer, profiles1.size, profiles2.size, Config.cuttingRatio, Config.filteringRatio)
    //val comparisonPrioritization = new IncrementalAdvancedComparisonPrioritization
    //val comparisonPrioritization = new IncrementalEntityPrioritization

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

    var tMatcher = 0L
    var cMatcher = 0
    val compFilter = Config.compFilter match {
      case "sbf" => new ScalableBFFilter(1000000, 0.1f)
      case "bf" => new BFFilter(5e8.toInt, 0.1f)
      case "none" => new NoneFilter
      case _ => new NoneFilter
    }
    val matcherFun = Matching.getMatcher(Config.matcher)
    val matcher = (mlc: IncrComparisons) => {
      var t0 = System.currentTimeMillis()
      cMatcher += mlc.comparisons.size
      val ccc = mlc.comparisons.map(matcherFun)
      tMatcher += (System.currentTimeMillis()-t0)
      IncrComparisons(mlc.incrIdx, ccc)
    }

//    val prioFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
//      import GraphDSL.Implicits._
//
//      val sourceEmpty = Source.repeat(ComparisonsRequest(1))
//      val merge = b.add(MergePrioritized[Seq[((EntityProfile, Int), Long)]](Seq(10000,1)))
//
//      sourceEmpty ~> merge.in(1)
//      FlowShape(merge.in(0), merge.out)
//    })

    val t1 = System.currentTimeMillis()
    val time = 1000
    val collector = new ProgressiveCollectorSinkWithIncrCount(t0, t1, dp, printAll, incremental, subParts)

    val loopFlow = if (Config.prioritizer == "ipbs" || Config.prioritizer == "ipbs2" || Config.prioritizer == "ipbs3")
      // CASE WHERE PRIORITIZER IS IPBS
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val sourceEmpty = Source.repeat(Seq[(Int, TokenNGrams)]())
        val merge = b.add(MergePrioritized[Seq[(Int, TokenNGrams)]](Seq(10000,1)))

        // TODO modify incrementalBlockPrioritization for dirty ER
        val tokenBlocker = if (Config.prioritizer == "ipbs")
              b.add(new IncrementalBlockPrioritization(
                ignoreComparisonRequests, comparisonBudget.floor.toInt,
                profiles1.size, 0, Config.cuttingRatio,
                Config.filteringRatio))
          else
              b.add(new IncrementalBlockPrioritization2(
                ignoreComparisonRequests, comparisonBudget.floor.toInt,
                profiles1.size, 0, Config.cuttingRatio,
                Config.filteringRatio))

        sourceEmpty ~>
          Flow[Seq[(Int, TokenNGrams)]].takeWhile(_ => {
            !ignoreComparisonRequests}
          ) ~>
          merge.in(1)

        merge.out ~>
          Flow[Seq[(Int, TokenNGrams)]]
            .takeWhile(_ => {
              System.currentTimeMillis() - t1 < maxTime || ignoreComparisonRequests
            }) ~>
          tokenBlocker

        FlowShape(merge.in(0), tokenBlocker.out)
      }) else
        // CASE WHERE PRIORITIZER IS NOT IPBS
        Flow.fromGraph(GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._

          val loopHandler  = (msg: Message) => {
            Seq((-1, null))
          } : Seq[(Int, TokenNGrams)]

          // TODO modify ranking steps for dirty ER (dovrebbe funzionare)
          val tokenBlocker = Config.ranker match  {
            case "cbs"   =>  new IncrementalComparisonRanking(Config.ranker, Config.prioritizer, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
            case "bjs"  =>  new IncrementalComparisonRankingWithBJS(Config.ranker, Config.prioritizer, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
            case _      =>  new IncrementalComparisonRanking(Config.ranker, Config.prioritizer, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
          }

          val comparisonPrioritization = Config.prioritizer match {
            case "ieps"   =>  new IncrementalEntityPrioritization(ignoreComparisonRequests, comparisonBudget.floor.toInt)
            case "icps"   =>  new IncrementalComparisonPrioritization(ignoreComparisonRequests, comparisonBudget.floor.toInt)
            case "iacps"  =>  new IncrementalAdvancedComparisonPrioritization(ignoreComparisonRequests, comparisonBudget.floor.toInt)
            case "ilsps"  =>  new IncrementalLocalSortPrioritization(ignoreComparisonRequests, comparisonBudget.floor.toInt)
          }

          val sourceEmpty = Source.repeat(ComparisonsRequest(1))
          val mergePF     = b.add(MergePrioritized[Message](Seq(1000,1)))
          val merge       = b.add(MergePrioritized[Seq[(Int, TokenNGrams)]](Seq(1,1)))
          val partition   = b.add(Partition[Message](2, msg => msg match {
            case Terminate() => 0
            case _ => 1
          } ))

          merge.out ~>
            tokenBlocker.async ~> // HERE PARALLELISM
            mergePF.in(0)

          sourceEmpty ~>
            Flow[ComparisonsRequest].takeWhile(_ => !ignoreComparisonRequests) ~>
            mergePF.in(1)

          mergePF.out ~>
            Flow[Message].takeWhile(m => m match {
              //case NoBlockComparisons() => false
              case _ => (System.currentTimeMillis() - t1 < maxTime || ignoreComparisonRequests)
            }) ~>
            comparisonPrioritization ~>
            partition.in

          partition.out(0) ~>
            Flow[Message].takeWhile(_ => !ignoreComparisonRequests) ~> // TODO care??
            Flow[Message].map(loopHandler) ~>
            merge.in(1)

          FlowShape(merge.in(0), partition.out(1))

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
    println(s"${throttleElements.toInt} every ${time} ms")
    val done =
      Source(profiles1).zipWithIndex
      .grouped(num.toInt)
      //.groupedWithin(num.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))
      .throttle(throttleElements.toInt, FiniteDuration.apply(time, TimeUnit.MILLISECONDS))

//      .mapConcat(x => {
//        val s = x.grouped(delta).toSeq
//        if (s.size != subParts) {
//          println(s"s.size = ${s.size} ;;; subParts=$subParts ")
//          assert(s.size == subParts)
//        }
//        s
//      })
      .map(tokenizer)
      .via(loopFlow)
      .async // HERE PARALLELISM
      .map( m => m match {
        case UpdateSeqAndMessageCompSeq(u, c) => UpdateSeqAndMessageCompSeq(u, compFilter.execute(c))
        case MessageComparisons(c) => MessageComparisons(compFilter.execute(c))
        case m => m
      })
      .filter ( m => m match {
        case MessageComparisons(c) => c.nonEmpty
        case _ => true
      })
      .via(new StoreSeqModelStageWithIncrCount(profiles1.size, 0))
      .map(matcher)
      .recover {
        case e => { println("BOOOOOOM MATCHER!" + e.getMessage) ; throw e }
      }
      .runWith(collector)

    done.onComplete( result => {
      val delta = System.currentTimeMillis() - t1
      println(s"TIME MATCHER ALLA FINE: $tMatcher ms")
      println(s"COMPARISONS EXECUTED:  $cMatcher")
      println(s"1: ${delta} ms")
      system.terminate()
    })
  }
}


