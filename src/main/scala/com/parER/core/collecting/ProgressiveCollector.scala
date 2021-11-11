package com.parER.core.collecting

import java.io.{BufferedWriter, File, FileWriter}

import com.parER.core.Config
import com.parER.datastructure.{BaseComparison, Comparison}
import org.scify.jedai.datamodel.IdDuplicates
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation

import scala.collection.mutable.ListBuffer

class ProgressiveCollector(t0: Long, t1: Long, dp: AbstractDuplicatePropagation, printAll: Boolean = true, th: Float = 0.1f) {
  import scala.jdk.CollectionConverters._

  var ec = 0.0f
  var em = 0.0f
  var emR = 0.0f
  var emP = 0.0f
  var filling = true
  val duplicates = dp.getDuplicates.asScala
  val buffer = new ListBuffer[String]()
  val nprofiles = Config.nprofiles
  val nworkers = Config.workers
  var sizeCmps = 0.0f
  var numCmps = 0.0f
  println("Value of the threshold: "+th)

  def execute(comparisons: List[BaseComparison]) = {
    for (cmp <- comparisons) {
      executeCmp(cmp)
    }
  }

  def executeCmp(cmp: BaseComparison) = {
    //TODO distinguish dirty-cc
    if ( Config.ccer && duplicates.contains(new IdDuplicates(cmp.e1, cmp.e2))) {
      update(cmp.e1, cmp.e2, cmp.sim)
    } else if (Config.ccer) {
      if (cmp.sim >= th)
        emP += 1
    }
    if ( !Config.ccer ) {
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
    }
    if (printAll)
      printForPC()
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
    val recall = emR / duplicates.size.toFloat
    recall
  }

  def getPrecision() = {
    val precision = emR / (emR + emP)
    precision
  }

  def getPC() = {
    val pc = em / duplicates.size.toFloat
    pc
  }

  def getPQ() = {
    val pq = em / ec
    pq
  }

  def getCardinaliy() = {
    em
  }

  var oldRec = 0.0f//-th
  var oldNum = 0.0f
  def print() = {
    if (ec % duplicates.size == 0) {
      val ecX = ec / duplicates.size.toFloat
      val rec = em / duplicates.size.toFloat
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


//  def printForPC() = {
//
//    val ecX = ec / duplicates.size.toFloat
//    val rec = em / duplicates.size.toFloat
//
//    if (rec - oldRec > 0.02) {
//      val t = System.currentTimeMillis()
//      val dt0 = t - t0
//      val dt1 = t - t1
//      val feCx = f"$ecX%.2f"
//      val s = s"${feCx}\t${nworkers}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}"
//      buffer.addOne(s)
//      println(s)
//
//      oldRec = rec
//
//    }
//
//  }

  var usTerminatedBefore = false
  var noElementsBefore = false

  def printForPC() = {

    val ecM = ec / 1e6.toFloat
    val ecX = ec / duplicates.size.toFloat
    val ecP = ec / nprofiles.toFloat
    val rec = em / duplicates.size.toFloat

    var usTerminatedNow = Config.upstreamTerminated
    var noElementsNow   = Config.isEmpty

    if (rec - oldRec > 0.02 || !usTerminatedBefore && usTerminatedNow || !noElementsBefore && noElementsNow ) {
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
      val terminated = if(noElementsNow) 2 else if (usTerminatedNow) 1 else 0
      val s = s"${feCx}\t${fecM}\t$fecP\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}\t$numCmps\t${sizeCmps/numCmps}\t${terminated}"
      buffer.addOne(s)
      numCmps = 0.0f
      sizeCmps = 0.0f
      println(s)

      oldRec = rec

    }

  }

  def printLast() = {
    val ecM = ec / 1e6.toFloat
    val ecX = ec / duplicates.size.toFloat
    val ecP = ec / nprofiles.toFloat
    val rec = em / duplicates.size.toFloat
    val pq = em / ec
    val t = System.currentTimeMillis()
    val dt0 = t - t0
    val dt1 = t - t1
    val terminated = if (Config.isEmpty) 2 else if (Config.upstreamTerminated) 1 else 0
    val s = s"${ecX}\t${ecM}\t$ecP\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}\t$numCmps\t${sizeCmps/numCmps}\t${terminated}"
    buffer.addOne(s)
    println(s)
  }

//  def printLast() = {
//    val ecX = ec / duplicates.size.toFloat
//    val rec = em / duplicates.size.toFloat
//    val pq = em / ec
//    val t = System.currentTimeMillis()
//    val dt0 = t - t0
//    val dt1 = t - t1
//    val s = s"${ecX}\t${nworkers}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${getRecall()}\t${getPrecision()}"
//    buffer.addOne(s)
//    println(s)
//  }

  def writeFile(filename: String): Unit = {
    println(s"Saving file ${filename}")
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    val header = s"ec*\tecM\tecP\tdt0\tdt1\tpc\tem\tduplicates\tf\trec\tpr\tcomps\tcompsize\tupstream_terminated\n"

//    val header = s"ec*\tnworkers\tdt0\tdt1\tpc\tem\tduplicates\tf\trec\tpr\n"
    bw.write(header)
    for (line <- buffer) {
      bw.write(line)
      bw.newLine()
    }
    bw.close()
  }
}
