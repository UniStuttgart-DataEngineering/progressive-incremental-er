package com.parER.datastructure

import com.parER.core.Config

import scala.collection.mutable

class ComparisonRankerExtended {

  // Multiplier
  val mul = Config.pOption.toFloat

  // Maximum # of comparisons
  val maxSize = Config.pOption2

  // RankMap: long value represents a range
  // All sim values in [1,2) are represented by value 1 // OLD

  // TODO implement data structure.
  // TODO A node is a Set or a TreeMap.
  // TODO Start with a TreeMap with a node.
  // TODO Start with range function
  // TODO When getting best comparisons, if node has size < MaxNodeSize
  // TODO ---> return all the comparisons
  // TODO If node has size >= MaxNodeSize
  // TODO ---> node becomes a TreeMap with his proper range function
  // TODO ---> node is restructured as a TreeMap of two nodes
  val rankMap = new mutable.TreeMap[Long, mutable.Set[BaseComparison]]()

  // Time: O(Log(#ranges)+Log(#comparisons
  def update(cmps: List[BaseComparison]) = {
    // update TreeMap with comparisons
    cmps.foreach(c => {
      //println(c.sim)
      val sim = math.min(mul.toLong, (c.sim * mul).floor.toLong)
      val ts = rankMap.getOrElseUpdate(sim, new mutable.HashSet[BaseComparison]())
      ts += c
    })
    // remove lower range leaves if sum is too high
    while (rankMap.size > 1 && size() > maxSize) {
      rankMap.remove(rankMap.firstKey)
    }
  }

  def size() = {
    rankMap.values.map(_.size).reduceLeft(_+_)
  }

  def isEmpty() = rankMap.isEmpty

  def getTopComparison() = {
    val lc = rankMap.last
    if (lc._2.size > 1) {
      val m = lc._2.toList.maxBy(x => x.sim)
      lc._2.remove(m)
      List(m)
    } else  {
      rankMap.remove(lc._1)
      lc._2.toList
    }
  }

  var totSumSizes = 0L
  var totSumSizesCount = 0L
  var maxSumSize = 0L
  var countOne = 0L
  var countGreaterThanOne = 0L
  var performedComparisons = 0L

  // Time: O(1): return the best comparisons
  // Remove the node
  def getBestComparisons() = {
    //println("Number of nodes " + rankMap.size)
    // TODO for debug
    //bw.write(s"${rankMap.size}\t${rankMap.firstKey}\t${rankMap.lastKey}\t${size()}\t${rankMap.last._2.size}\n")

    // get statistics
    totSumSizes += rankMap.size
    totSumSizesCount += 1
    maxSumSize = if (rankMap.size > maxSumSize) rankMap.size else maxSumSize
    countOne += (if (rankMap.size == 1) 1 else 0)
    countGreaterThanOne += (if (rankMap.size > 1) 1 else 0)

    val lc = rankMap.last
    rankMap.remove(lc._1)

    performedComparisons += lc._2.size
    lc._2.toList
  }

  def getAvg() = totSumSizes.toFloat / totSumSizesCount.toFloat
  def getMax() = maxSumSize
  def getOnes = countOne
  def getGreaterThanOne = countGreaterThanOne

  def getNComparisons() = performedComparisons
  def getAvgNComparisons() = performedComparisons.toFloat / totSumSizesCount.toFloat

}
