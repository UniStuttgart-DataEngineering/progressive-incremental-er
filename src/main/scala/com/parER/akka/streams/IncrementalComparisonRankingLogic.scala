package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, Shape}
import com.parER.akka.streams.messages._
import com.parER.core.Config
import com.parER.core.blocking.{BlockGhosting, CompGeneration, EntityTokenBlockerRefiner}
import com.parER.core.ranking.WNP2CBSRanker
import com.parER.datastructure.LightWeightComparison
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

/**
 *
 * This stage simply applies I-PBS
 *
 * @param rankMethod
 * @param progressiveMethod
 * @param size1
 * @param size2
 * @param ro
 * @param ff
 * @param noUpdate
 */

abstract class IncrementalComparisonRankingLogic(rankMethod: String, progressiveMethod:String, size1: Int, size2: Int = 0, ro: Float = 0.005f, ff: Float = 0.01f, noUpdate: Boolean = false, shape: Shape) extends GraphStageLogic(shape) {

  // Message buffer
  val buffer = mutable.Queue[Message]()

  // For time measurements
  var tTotal = 0L
  var tBlocking = 0L
  var tCompGen = 0L
  var tRank = 0L
  var tCompPrioUpdate = 0L
  var tCompPrioFetch = 0L

  var tOrigin, tNow = System.currentTimeMillis()
  var count = 0L
  var cBlocker = 0L
  var cCompCleaner = 0L
  var sent = 0L

  val tokenBlocker = new EntityTokenBlockerRefiner(size1, size2, ro, ff)  // it performs block pruning
  val blockGhosting = new BlockGhosting(ff)
  val compGeneration = new CompGeneration
  var oldFreeTime = System.currentTimeMillis()
  val nprofiles = Config.nprofiles
  var upstreamTerminated = false

  tokenBlocker.setModelStoring(false)
  println(s"ro=$ro")
  println(s"ff=$ff")

  var sorted = false
  var index = 0

  var sortedBlocks : Array[(String, Int)]  = null
  var executed : Array[Int] = null

  def existBlocks() = {
    index < sortedBlocks.length
  }

  def getMinimumBlock() = {
    if (!sorted) {
      var t = System.currentTimeMillis()
      println("SORTING the blocks...")
      sortedBlocks = tokenBlocker.getBlockSizes.sortBy(_._2)
      if (executed == null || executed.size < sortedBlocks.size) {
        executed = new Array[Int](sortedBlocks.size)
      }
      sorted = true
      index = 0
      println(s"Sorted!! In ${(System.currentTimeMillis() - t)} ms")
    }
    if (index < sortedBlocks.length) {
      var mb = sortedBlocks(index)
      while (executed(index) == mb._2) {
        index += 1
      }
      executed(index) = mb._2
      index += 1
      mb
    } else {
      //println("No more comparisons...")
      (null, -1)
    }
  }

  def getMinimumBlockComparisons() = {
    val (key, size) = getMinimumBlock()
    if (key != null) {
      val b0 = tokenBlocker.getBlock(1, key) // it return the block that matches with entities in did = 1
      val b1 = tokenBlocker.getBlock(0, key)
      //println(s"b0 = ${b0.size} ; b1 = ${b1.size}")
      compGeneration.generateBlockMessageComparisons(b0, b1)
    } else {
      //println("getMinimumBlockComparisons: key is null")
      List()
    }
  }
}