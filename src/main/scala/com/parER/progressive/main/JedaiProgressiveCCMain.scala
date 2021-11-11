package com.parER.progressive.main

import java.util

import com.parER.core.Config
import com.parER.core.collecting.{ProgressiveCollector, ProgressiveCollectorWithStatistics}
import com.parER.datastructure.{BaseComparison, Comparison}
import com.parER.utils.{CsvWriter, ExtendedProfileMatcher, GtSerializationReaderPartial, JedaiComparisonCleaning, JedaiMatching, JedaiPrioritization}
import org.scify.jedai.blockbuilding.StandardBlocking
import org.scify.jedai.blockprocessing.blockcleaning.{BlockFiltering, SizeBasedBlockPurging}
import org.scify.jedai.datamodel.SimilarityPairs
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.entitymatching.ProfileMatcher
import org.scify.jedai.prioritization.{GlobalProgressiveSortedNeighborhood, IPrioritization, LocalProgressiveSortedNeighborhood, ProgressiveBlockScheduling, ProgressiveEntityScheduling, ProgressiveGlobalTopComparisons, ProgressiveLocalTopComparisons}
import org.scify.jedai.textmodels.TokenNGrams
import org.scify.jedai.utilities.BlocksPerformance
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation
import org.scify.jedai.utilities.enumerations.{ProgressiveWeightingScheme, RepresentationModel, SimilarityMetric, WeightingScheme}

import scala.jdk.CollectionConverters._

object JedaiProgressiveCCMain extends App {

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Time variables
    var t = 0L
    var tBlocker = 0L
    var tCompCleaner = 0L
    var tMatcher = 0L
    var tCollector = 0L

    // Argument parsing
    Config.commandLine(args)
    Config.name = s"jedai-${Config.ccMethod}-${Config.prioritizer}-${Config.matcher}-${Config.cuttingRatio}-${Config.filteringRatio}-part=${Config.partition}-ARCS"

    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2

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
    var profiles1 = eReader1.getEntityProfiles
    var limitD1 = profiles1.size()/Config.partition
    profiles1 = profiles1.subList(0, limitD1)
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val eReader2 = new EntitySerializationReader(eFile2)
    var profiles2 = eReader2.getEntityProfiles
    var limitD2 = profiles2.size()/Config.partition
    profiles2 = profiles2.subList(0, limitD2)
    if (Config.print) System.out.println("Input Entity Profiles2\t:\t" + profiles2.size)

    Config.nprofiles = profiles1.size + profiles2.size

    //val gtReader = new GtSerializationReader(gtFile)
    val gtReader = new GtSerializationReaderPartial(gtFile, limitD1, limitD2)

    val dp = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    // Progressive collector
    val proCollector = new ProgressiveCollectorWithStatistics(t0, System.currentTimeMillis(), dp, Config.print)

    val workflowConf = new StringBuilder
    val workflowName = new StringBuilder

    t = System.currentTimeMillis()
    val bb = new StandardBlocking
    var blocks = bb.getBlocks(profiles1, profiles2)
    workflowConf.append("\n").append(bb.getMethodConfiguration)
    workflowName.append("->").append(bb.getMethodName)

    val bc = new SizeBasedBlockPurging(Config.cuttingRatio)
    blocks = bc.refineBlocks(blocks)
    workflowConf.append("\n").append(bc.getMethodConfiguration)
    workflowName.append("->").append(bc.getMethodName)

    var totalBlockComparisons = 0.0f
    for (b <- blocks.asScala) {
        totalBlockComparisons += b.getNoOfComparisons
    }
    System.out.println(s"BP: Total Block comparisons\t:$totalBlockComparisons\t in ${System.currentTimeMillis() - t} ms")
    t = System.currentTimeMillis()

    val bf = new BlockFiltering(Config.filteringRatio)
    blocks = bf.refineBlocks(blocks)
    workflowConf.append("\n").append(bf.getMethodConfiguration)
    workflowName.append("->").append(bf.getMethodName)
    tBlocker += (System.currentTimeMillis() - t)

//    var bbStats = new BlocksPerformance(blocks, dp)
//    bbStats.setStatistics()
//    bbStats.printStatistics(System.currentTimeMillis() - t0, workflowConf.toString, workflowName.toString)

    totalBlockComparisons = 0.0f
    for (b <- blocks.asScala) {
        totalBlockComparisons += b.getNoOfComparisons
    }
    System.out.println(s"BF: Total Block comparisons\t:$totalBlockComparisons\t in ${System.currentTimeMillis() - t} ms")
    t = System.currentTimeMillis()

    val cc = JedaiComparisonCleaning.apply(Config.ccMethod, WeightingScheme.CBS)
    val cnpBlocks = cc.refineBlocks(new util.ArrayList(blocks))
    //blocks = cc.refineBlocks(blocks)
    workflowConf.append("\n").append(cc.getMethodConfiguration)
    workflowName.append("->").append(cc.getMethodName)
    tCompCleaner += (System.currentTimeMillis() - t)

//    var ccStats = new BlocksPerformance(blocks, dp)
//    ccStats.setStatistics()
//    ccStats.printStatistics(System.currentTimeMillis() - t0, workflowConf.toString, workflowName.toString)

    var totalComparisons = 0.0
    for (b <- cnpBlocks.asScala) {
        totalComparisons += b.getNoOfComparisons
    }
    System.out.println(s"CC: Total comparisons\t:$totalComparisons\t in ${System.currentTimeMillis() - t} ms")
    t = System.currentTimeMillis()

    val maxComparisons = 2.5e8
    totalComparisons = if(totalComparisons > maxComparisons) maxComparisons else totalComparisons
    System.out.println(s"BudgetComparisons = $totalComparisons")

    System.out.println("Prioritizer " + Config.prioritizer)
    val prioritization = if (Config.prioritizer.equalsIgnoreCase("efilt" ))
        new EntityFiltering(WeightingScheme.JS, cnpBlocks, profiles1.size+profiles2.size) else
        JedaiPrioritization.apply(Config.prioritizer, profiles1.size+profiles2.size, totalComparisons.toInt, WeightingScheme.CBS)

    //val prioritization =  JedaiPrioritization.apply(Config.prioritizer, profiles1.size+profiles2.size, Int.MaxValue-1, WeightingScheme.ARCS)
    //val prioritization = new EntityFiltering(WeightingScheme.ARCS, blocks, profiles1.size+profiles2.size)
    //val prioritization = new CepCnpEntities(WeightingScheme.ARCS, profiles1.size+profiles2.size)
    //val prioritization = new CepCnp(WeightingScheme.ARCS, profiles1.size+profiles2.size)

    //val prioritization = new ProgressiveBlockScheduling(totalComparisons.asInstanceOf[Int], WeightingScheme.JS)
    // Next: does not work with CoCl
    //val prioritization = new ProgressiveEntityScheduling(totalComparisons.asInstanceOf[Int], WeightingScheme.JS)
    //val prioritization = new ProgressiveLocalTopComparisons(totalComparisons.asInstanceOf[Int], WeightingScheme.JS)
    //val prioritization = new ProgressiveGlobalTopComparisons(totalComparisons.asInstanceOf[Int], WeightingScheme.JS)

    // These two do not work:
    //val prioritization = new GlobalProgressiveSortedNeighborhood(totalComparisons.asInstanceOf[Int], ProgressiveWeightingScheme.NCF)
    //val prioritization = new LocalProgressiveSortedNeighborhood(totalComparisons.asInstanceOf[Int], ProgressiveWeightingScheme.NCF)

    val maxTimeInMinutes : Long = Config.budget // * 60L
    val maxTime : Long = maxTimeInMinutes * 60L * 1000L

    prioritization.developBlockBasedSchedule(cnpBlocks)

    System.out.println(s"Prioritization in ${System.currentTimeMillis() - t} ms")
    println(s"${prioritization.getMethodConfiguration}")
    println(s"Budget: $maxTimeInMinutes")
    t = System.currentTimeMillis()

    //val simPairs = new SimilarityPairs(true, totalComparisons.asInstanceOf[Int])
    if (JedaiMatching.apply(Config.matcher) != null) {
        t = System.currentTimeMillis()
        var pm = new ExtendedProfileMatcher(profiles1, profiles2, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY)
        var count = 0L
        while (prioritization.hasNext && (System.currentTimeMillis() - t < maxTime)) {
            val c1 = prioritization.next()
            val sim = pm.executeComparison(c1)
            val bc1 = Comparison(c1.getEntityId1, null, c1.getEntityId2, null, sim)
            count += 1
            //simPairs.addComparison(c1)
            proCollector.executeCmp(bc1)
        }
        if (prioritization.hasNext) {
            println("Time is over! But still more comparisons...")
        } else {
            println("No more comparisons...")
        }
        println(s"Executed $count comparisons")
        //val simPairs = pm.executeComparisons(blocks)
        //println("Size of simPairs " + simPairs.getNoOfComparisons)
        tMatcher += (System.currentTimeMillis() - t)
    }

    println("\nTime measurements: ")
    println("tBlocker = " + tBlocker + " ms")
    println("tCompCleaner = " + tCompCleaner + " ms")
    println("tMatcher = " + tMatcher + " ms")
    println("Total = " + (tBlocker+tCompCleaner+tMatcher) + " ms")

    proCollector.printLast()
    proCollector.writeFile("test-outputs/"+Config.name+".txt")

    if (Config.output) {
        val csv = new CsvWriter("name, CoCl, ro, ff, PC, PQ, BB(T), CC(T), MA(T), O(T), OD(T), BB(C), CC(C)")

        val CoCl = "j" + Config.ccMethod
        val ro = Config.cuttingRatio.toString
        val ff = Config.filteringRatio.toString
        val name = CoCl + "-" + ro + "-" + ff

        val PC = proCollector.getPC().toString
        val PQ = proCollector.getPQ().toString
        val BBT = tBlocker.toString
        val CCT = tCompCleaner.toString
        val MAT = tMatcher.toString
        val OT = (tBlocker+tCompCleaner+tMatcher).toString
        val ODT = (System.currentTimeMillis() - t0).toString

        // TODO proper getCardinality after BB
        val BBC = proCollector.getCardinaliy().toString
        val CCC = proCollector.getCardinaliy().toString

        val line = List[String](name, CoCl, ro, ff, PC, PQ, BBT, CCT, MAT, OT, ODT, BBC, CCC)
        println(line)
        csv.newLine(line)
        csv.writeFile(Config.file, Config.append)
    }

}