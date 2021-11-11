package com.parER.core.collecting

import java.io.{BufferedWriter, File, FileWriter}

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.IdDuplicates
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation

import scala.collection.mutable.ListBuffer

class ProgressiveCollectorWithStatistics(t0: Long, t1: Long, dp: AbstractDuplicatePropagation, printAll: Boolean = true, incremental: Boolean = false, th: Float = 0.1f, parts: Int = 1) {
  import scala.jdk.CollectionConverters._

  var ec = 0.0
  var em = 0.0
  var emR = 0.0
  var emP = 0.0
  var sizeCmps = 0.0
  var numCmps = 0.0
  var filling = true
  val duplicates = dp.getDuplicates.asScala
  val buffer = new ListBuffer[String]()
  val nworkers = Config.workers
  val nprofiles = Config.nprofiles
  var partIndex = 0

  val showMatches = Config.showMatches

  var increment = 0
  var updateIncr = true
  //println("Value of the threshold: "+th)
  //println("Number of parts is : " + parts)

  def incrExecute(incrIdx: Int, comparisons: List[BaseComparison]) = {
    updateIncr = incremental
    if (!updateIncr)
      increment = incrIdx
    execute(comparisons)
  }

  def execute(comparisons: List[BaseComparison]) = {
    //println(s"Increment $increment: size ${comparisons.size}")
    if (updateIncr) {
      partIndex += 1
      if (partIndex % parts == 0)
        increment += 1
    }

    for (cmp <- comparisons) {
      executeCmp(cmp)
    }

    if (incremental && !printAll && (updateIncr && partIndex % parts == 0)) {
      val s = getLast()
      buffer.addOne(s)
      println(s)
    }
  }

  def executeCmp(cmp: BaseComparison) = {
    // Statistics
    try {
      val e1Size = if (cmp.e1Model == null) 1 else cmp.e1Model.getItemsFrequency.size()
      val e2Size = if (cmp.e2Model == null) 1 else cmp.e2Model.getItemsFrequency.size()
      sizeCmps += (e1Size + e2Size)
      numCmps += 1

      //TODO distinguish dirty-cc
      if (Config.ccer && duplicates.contains(new IdDuplicates(cmp.e1, cmp.e2))) {
        update(cmp.e1, cmp.e2, cmp.sim)
      } else if (Config.ccer) {
        if (cmp.sim >= th)
          emP += 1
      }
      if (!Config.ccer) {
        val (a, b) = (duplicates.contains(new IdDuplicates(cmp.e1, cmp.e2)), duplicates.contains(new IdDuplicates(cmp.e2, cmp.e1)))
        if (a || b)
          (a, b) match {
            case (true, _) => {
              update(cmp.e1, cmp.e2, cmp.sim)
            }
            case (_, true) => {
              update(cmp.e2, cmp.e1, cmp.sim)
            }
          }
        else if (cmp.sim >= th)
          emP += 1

//        if (cmp.e1 >= cmp.e2) {
//          println("SBAGLIATO")
//          assert(false)
//        }
      }
      if (printAll && showMatches)
        printMatches(cmp) //printForPC()
      if (printAll && !showMatches)
        printForPC()
    } catch{
      case _ => println("Exception...")
    }
    ec += 1
  }

  def update(e1: Int, e2: Int, sim: Float) = {
    val isSuperfluous = dp.isSuperfluous(e1, e2)
    if (!isSuperfluous)
      em += 1
    if (sim >= th && !isSuperfluous)
      emR += 1
  }

  def getRecall() = {
    val recall = emR / duplicates.size.toDouble
    recall
  }

  def getPrecision() = {
    val precision = emR / (emR + emP)
    precision
  }

  def getPC() = {
    val pc = em / duplicates.size.toDouble
    pc
  }

  def getPQ() = {
    val pq = em / ec
    pq
  }

  def getCardinaliy() = {
    em
  }

  var oldRec = 0.0//-th
  var oldNum = 0.0
  def print() = {
    if (ec % duplicates.size == 0) {
      val ecX = ec / duplicates.size.toDouble
      val rec = em / duplicates.size.toDouble
      val t = System.currentTimeMillis()
      val dt0 = t - t0
      val dt1 = t - t1
      if (oldNum != em) {
        val s = s"${ecX}\t${nworkers}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}"
        buffer.addOne(s)
        println(s)
        oldNum = em
        oldRec = rec
      }
    }
  }



  var oldEm = 0
  var oldIncr = 0
  var oldTime = 0.0
  def printMatches(cmp: BaseComparison, force: Boolean = false) = {
    val dt1 = System.currentTimeMillis()-t1
    //var typ = "emits"
    if (Config.prioritizer != "noprio" && oldIncr != 0 && increment > oldIncr) {
      val s = s"$oldTime,emits,$oldIncr,$oldEm,(0-0),${Config.pOption},${Config.time},${Config.prioritizer}"
      buffer.addOne(s)
      println(s)
    }
    oldTime = dt1
    oldIncr = increment.toInt
    if (em > oldEm || force) {
      oldEm = em.toInt
      val s = s"$dt1,emits,${increment.toInt},${em.toInt},(${cmp.e1}-${cmp.e2}),${Config.pOption},${Config.time},${Config.prioritizer}"
      buffer.addOne(s)
      println(s)
    }
  }

  var usTerminatedBefore = false
  var noElementsBefore = false
  var old = 0.0f
  var firstTime = true

  def printForPC() = {

    val ecM = ec / 1e6.toDouble
    val ecX = ec / duplicates.size.toDouble
    val ecP = ec / nprofiles.toDouble
    val rec = em / duplicates.size.toDouble



    var usTerminatedNow = Config.upstreamTerminated
    var noElementsNow   = Config.isEmpty
    var ecPrintable     = Config.ecPrintable

    if (rec - oldRec > 0.02 || ecPrintable && ec > 0 && ec % duplicates.size == 0 || !usTerminatedBefore && usTerminatedNow || !noElementsBefore && noElementsNow || usTerminatedBefore && (ec % duplicates.size == 0) && (ecX % 5 == 0) ) {
      val t = System.currentTimeMillis()
      val dt0 = t - t0
      val dt1 = t - t1
      val fecP = f"$ecP%.2f"
      val feCx = f"$ecX%.2f"
      val fecM = f"$ecM%.2f"
      if (!usTerminatedBefore && usTerminatedNow)
        usTerminatedBefore = true
      if (!noElementsBefore && noElementsNow)
        noElementsBefore = true

      val terminated = if (noElementsNow) 2 else if (usTerminatedNow) 1 else 0
      val s = s"${feCx}\t${fecM}\t$fecP\t${increment}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}\t$numCmps\t${sizeCmps / numCmps}\t${terminated}\t${Config.prioritizer}\t${Config.ranker}"
      buffer.addOne(s)
      numCmps = 0.0f
      sizeCmps = 0.0f

      //println(s"dt1_new - dt1_old = ${(dt1 - old) / 1000.0f} s")
      old = dt1
      //println(s)

      val ecT = ec / 1e3.toDouble // Thousands
      val fecT = f"$ecT%.2f"
      if (firstTime) {
        firstTime = false
        println(s"incr\tec*\tcmps(x1K)\truntime (ms)\tPC")

      }
      println(s"$increment\t$feCx\t$fecT\t$dt1\t$rec\t")

      oldRec = rec
    }

  }

  def getLast() = {
    val ecM = ec / 1e6.toDouble
    val ecX = ec / duplicates.size.toDouble
    val ecP = ec / nprofiles.toDouble
    val fecP = f"$ecP%.2f"
    val fecX = f"$ecX%.2f"
    val fecM = f"$ecM%.2f"
    val rec = em / duplicates.size.toDouble
    val pq = em / ec
    val t = System.currentTimeMillis()
    val dt0 = t - t0
    val dt1 = t - t1
    val terminated = if (Config.isEmpty) 2 else if (Config.upstreamTerminated) 1 else 0
    val s = s"${fecX}\t${fecM}\t$fecP\t$increment\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}\t$numCmps\t${sizeCmps/numCmps}\t${terminated}\t${Config.prioritizer}\t${Config.ranker}"
    s
  }

  def printLast() = {
    if (printAll && showMatches) {
      printMatches(Comparison(0, null, 0, null), true)
    }
    val s = getLast
    buffer.addOne(s)
    println(s)
  }

  def printEC() = println(s"ec: $ec")

  def getHeader() = s"ec*\tecM\tecP\tincrements\tdt0\tdt1\tpc\tem\tduplicates\tf\trec\tpr\tcomps\tcompsize\tupstream_terminated\tmethod\tranking\n"

  def writeFile(filename: String): Unit = {
    println(s"Saving file ${filename}")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file, Config.append))
    if (!Config.append) {
      val header = getHeader()
      bw.write(header)
    }
    for (line <- buffer) {
      bw.write(line)
      bw.newLine()
    }
    bw.close()
  }
}
