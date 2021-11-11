package com.parER.core.prioritizing

import com.parER.core.Config
import com.parER.datastructure.BaseComparison

import scala.collection.mutable

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

class ExtendedTreeMapPrioritizer extends Prioritizing {

  println("ExtendedTreeMapPrioritizer")

  val mul = Config.pOption.toFloat
  val maxSize = Config.pOption2
  // TODO make it adaptive?
  val maxNodeSize = 2 //maxSize/10000

  class Node(baseValue: Float, maxValue: Float) {

    val thValue = (maxValue+baseValue)/2.0f
    var nodeLeft : Node  = null //new Node(baseValue, thValue)
    var nodeRight : Node = null //new Node(thValue, maxValue)
    val bucket = mutable.Set[BaseComparison]()

    //println(s"${baseValue} -  ${maxValue}  - ${thValue}")

    def add(c: BaseComparison) = bucket += c

    def addAll(lc: mutable.Iterable[BaseComparison]) = bucket ++= lc

    def innerSize = (if (nodeLeft == null) 0 else nodeLeft.size) + (if (nodeRight == null) 0 else nodeRight.size)

    def comparisons(): List[BaseComparison] = {

      var cmps = List[BaseComparison]()
      if (innerSize == 0 && bucket.size < maxNodeSize) {
        cmps ++= bucket
        bucket.clear()
      } else if (bucket.size > 0) {
        //println("bucket size???")
        var (ll1, ll2) = bucket.partition(_.sim * mul < thValue)
        if (ll1.size != 0 && ll2.size != 0) {
          //println("splitting...")
          if (nodeLeft == null) nodeLeft = new Node(baseValue, thValue)
          if (nodeRight == null) nodeRight = new Node(thValue, maxValue)
          nodeLeft.addAll(ll1)
          nodeRight.addAll(ll2)
          bucket.clear()
          cmps ++= this.comparisons()
        } else {
          cmps ++= bucket
          bucket.clear()
        }
      } else if (bucket.size == 0 && innerSize > 0) {
        if (nodeRight.size > 0) {
          cmps ++= nodeRight.comparisons()
        } else if (nodeLeft.size > 0) {
          cmps ++= nodeLeft.comparisons()
        }
      }
      cmps
    }

    def size : Int = bucket.size + innerSize
  }

  val rankMap = new mutable.TreeMap[Long, Node]()

  override def update(cmps: List[BaseComparison]) = {

    cmps.foreach(c => {
      val base = math.min(mul.toLong, (c.sim * mul).floor.toLong)
      val ts = rankMap.getOrElseUpdate(base, new Node(base, base+1.0f))
      ts.add(c)
    })

    // remove lower range leaves if sum is too high
    while (rankMap.size > 1 && size() > maxSize) {
      rankMap.remove(rankMap.firstKey)
    }
  }

  def size() = {
    rankMap.values.map(_.size).reduceLeft(_+_)
  }

  override def isEmpty() = rankMap.isEmpty

  // Time: O(1): return the best comparisons
  // Remove the node
  override def getBestComparisons() = {
    var (id, ln) = rankMap.last
    if (ln.size == 0) {
      rankMap.remove(id)
    }
    ln.comparisons()
  }

}
