package com.parER.progressive.main

import com.parER.core.Config
import com.parER.core.collecting.ProgressiveCollectorWithStatistics
import com.parER.datastructure.{BaseComparison, Comparison}
import com.parER.utils.{ExtendedProfileMatcher, JedaiComparisonCleaning, JedaiMatching, JedaiPrioritization, UnilateralDuplicatePropagation}
import org.scify.jedai.blockbuilding.StandardBlocking
import org.scify.jedai.blockprocessing.blockcleaning.{BlockFiltering, SizeBasedBlockPurging}
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.utilities.enumerations.{RepresentationModel, SimilarityMetric, WeightingScheme}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object JedaiProgressiveIncrementalDirtyMain extends App {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.ranker = "cbs"
    Config.commandLine(args)

    var incremental = true
    var printAll = false

    val maxTimeInMinutes : Long = Config.budget // * 60L
    val maxTime : Long = maxTimeInMinutes * 60L * 1000L
    if (maxTime == 0 && Config.batches > 1) {
        incremental = true
        printAll = false
        Config.name = "incremental" + Config.batches
    } else if (maxTime > 0 && Config.batches == 1) {
        incremental = false
        printAll = true
        Config.name = "batch"
    } else {
        incremental = false
        printAll = true
        Config.name = "error"
    }

    println(s"==== ${Config.name} ===== ")

    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2

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
    val profiles1 = eReader1.getEntityProfiles
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    // Number of sublists
    val N = Config.batches
    Config.nprofiles = profiles1.size


    val chunkSize1 = (profiles1.size().toFloat/N.toFloat).ceil.toInt
    val groupedProfiles1 = profiles1.asScala.grouped(chunkSize1).toList

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    //val mutable_dp = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    val nduplicates = 10*dp.getDuplicates.size.toDouble / N.toDouble

    // Progressive collector
    val proCollector = new ProgressiveCollectorWithStatistics(t0, System.currentTimeMillis(), dp, printAll, incremental)

    var aggTime = 0L
    for ( i <- 1 to N) {

        //println(s"====== increment ${i} =======")

        var tStart =  System.currentTimeMillis()

        // Time variables
        var t = 0L
        var tBlocker = 0L
        var tCompCleaner = 0L
        var tMatcher = 0L
        var tCollector = 0L

        val p1 = groupedProfiles1.slice(0,i).flatten.toList.asJava
        //val p2 = groupedProfiles2.slice(0,i).flatten.toList.asJava

        t = System.currentTimeMillis()
        val bb = new StandardBlocking
        var blocks = bb.getBlocks(p1, null)

        val bc = new SizeBasedBlockPurging(Config.cuttingRatio)
        blocks = bc.refineBlocks(blocks)

        val bf = new BlockFiltering(Config.filteringRatio)
        blocks = bf.refineBlocks(blocks)
        tBlocker += (System.currentTimeMillis() - t)

        var totalBlockComparisons = 0.0f
        for (b <- blocks.asScala) {
            totalBlockComparisons += b.getNoOfComparisons
        }
        //System.out.println(s"BF: Total Block comparisons at increment $i\t:$totalBlockComparisons\t in $tBlocker ms")
        //var bbStats = new BlocksPerformance(blocks, mutable_dp)
        //bbStats.setStatistics()

        t = System.currentTimeMillis()
        val cc = JedaiComparisonCleaning.apply(Config.ccMethod, WeightingScheme.CBS)
        blocks = cc.refineBlocks(blocks)
        tCompCleaner += (System.currentTimeMillis() - t)

        var totalComparisons = 0.0
        for (b <- blocks.asScala) {
            totalComparisons += b.getNoOfComparisons
        }
        //System.out.println(s"CC: Total comparisons at increment $i \t:$totalComparisons\t in $tCompCleaner ms")
        //var ccStats = new BlocksPerformance(blocks, mutable_dp)
        //ccStats.setStatistics()

        val maxComparisons = 1e8
        totalComparisons = if(totalComparisons > maxComparisons) maxComparisons else totalComparisons
        //System.out.println(s"BudgetComparisons = $totalComparisons")

        // Prioritization step
        //System.out.println("Prioritizer " + Config.prioritizer)
        val prioritization = if (Config.prioritizer.equalsIgnoreCase("efilt" ))
            new EntityFiltering(WeightingScheme.JS, blocks, profiles1.size) else
            JedaiPrioritization.apply(Config.prioritizer, profiles1.size, totalComparisons.toInt, WeightingScheme.CBS)

        prioritization.developBlockBasedSchedule(blocks)

        System.out.println(s"Prioritization in ${System.currentTimeMillis() - t} ms")
        //println(s"${prioritization.getMethodConfiguration}")
        //println(s"Budget: $maxTimeInMinutes")

        var count = 0L
        if (JedaiMatching.apply(Config.matcher) != null) {
            t = System.currentTimeMillis()
            var pm = new ExtendedProfileMatcher(profiles1, null, RepresentationModel.TOKEN_UNIGRAMS, SimilarityMetric.JACCARD_SIMILARITY)
            var comps = ListBuffer[BaseComparison]()
            while (prioritization.hasNext && (System.currentTimeMillis() - t < maxTime || (maxTime == 0 && count < nduplicates )) ) {
                val c1 = prioritization.next()
                var similarity = 0.0f
                if (!hm.contains(c1)) {
                    similarity = pm.executeComparison(c1)
                    hm.update(c1, similarity)
                    count += 1

                    if (Config.batches > 1)
                        comps.addOne( Comparison(c1.getEntityId1, null, c1.getEntityId2, null, similarity) )
                    else
                        proCollector.executeCmp(Comparison(c1.getEntityId1, null, c1.getEntityId2, null, similarity) )
                }
            }
            if (Config.batches > 1)
                proCollector.execute(comps.toList)
            //if (prioritization.hasNext) {
            //    println("Time is over! But still more comparisons...")
            //} else {
            //    println("No more comparisons...")
            //}

            //val simPairs = pm.executeComparisons(blocks)
            tMatcher += (System.currentTimeMillis() - t)
        }

        var tEnd = System.currentTimeMillis() - tStart
        aggTime+=tEnd;

        println("\nTime measurements: ")
        println("tBlocker = " + tBlocker + " ms")
        println("tCompCleaner = " + tCompCleaner + " ms")
        println("tMatcher = " + tMatcher + " ms")
        println("Aggregate Total = " + (tBlocker+tCompCleaner+tMatcher) + " ms")
        println("Measured Total = " + (tEnd) + " ms")

        if (Config.output) {
//            val csv = new CsvWriter("method, increment, time, aggregate-time, comparisons, name, CoCl, ro, ff, PC, PQ, BB(T), CC(T), MA(T), O(T), OD(T), BB(C), CC(C)")
//
//            val CoCl = "j" + Config.ccMethod
//            val ro = Config.cuttingRatio.toString
//            val ff = Config.filteringRatio.toString
//            val name = prioritization.getMethodConfiguration + "-" + CoCl + "-" + ro + "-" + ff

//            val PC = ccStats.getPc.toString
//            val PQ = ccStats.getPq.toString
//            val BBT = tBlocker.toString
//            val CCT = tCompCleaner.toString
//            val MAT = tMatcher.toString
//            val OT = (tBlocker+tCompCleaner+tMatcher).toString
//            val ODT = (System.currentTimeMillis() - t0).toString
//            val BBC = bbStats.getAggregateCardinality.toString
//            val CCC = ccStats.getAggregateCardinality.toString
//
//            val line = List[String](s"batch", i.toString, tEnd.toString, aggTime.toString, hm.size.toString, name, CoCl, ro, ff, PC, PQ, BBT, CCT, MAT, OT, ODT, BBC, CCC)
//            println("method, increment, time, aggregate-time, comparisons, name, CoCl, ro, ff, PC, PQ, BB(T), CC(T), MA(T), O(T), OD(T), BB(C), CC(C)")
//            println(line)
//            println(s"Executed $count")
            val line = proCollector.getLast()
            println(line)
//            csv.newLine(line.split("\t").toList)
//            csv.writeFile(Config.file, Config.append || i != 1)
        }
    }

    proCollector.writeFile(s"test-outputs/${Config.name}-${Config.groundtruth}-${Config.matcher}.txt")

}