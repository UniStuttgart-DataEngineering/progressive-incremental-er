package com.parER.datastructure

import scala.collection.mutable

class BlockRanker {

  val blockMap = new mutable.HashMap[String, (Float, Int)]()
  val rankMap = new mutable.TreeMap[Float, mutable.TreeSet[String]]()
  var sumR = 0.0f
  var sumS = 0
  var avgRank = 0.0f
  var totSize = 0
  var count = 0

  // Time: O(log(#blocks))
  // number of blocks == number of keys
  def update(key: String, rank: Float, size: Int) = {
    updateRankMap(key, rank, size, rankMap)
  }

  private def updateRankMap(key: String, rank: Float, size: Int,
                            rankMap: mutable.TreeMap[Float, mutable.TreeSet[String]]) = {

    var newRank = 0.0f
    var updatable = true

    if (blockMap.contains(key)) {
      val (r, s) = blockMap(key)
      val oldRank = r/s

      avgRank -= oldRank

      newRank = (r+rank)/(s+size)
      val updatable = s+size > 0 && r+rank > 0.0f
      if (updatable) {
        blockMap(key) = (r+rank, s+size)
        sumR += rank
        sumS += size
        avgRank += newRank
      } else {
        sumR -= r
        sumS -= s
        blockMap.remove(key)
        totSize -= 1
      }

      val ts = rankMap(oldRank)
      if (ts.size == 1)
        rankMap.remove(oldRank)
      else
        ts.remove(key)
    } else {
      newRank = rank/size
      sumR += rank
      sumS += size
      avgRank += newRank
      totSize += 1
      blockMap.addOne(key, (rank,size))
    }

    if (updatable) {
      val ts = rankMap.getOrElseUpdate(newRank,
        new mutable.TreeSet[String]())
      ts += key
    }
  }

  // Time: O(1)
  def getMax() = {
    rankMap.last
  }

  def getMax(keys: List[String]) = {
    def max(k1: String, k2: String): String = {
      val (r1,s1) = blockMap.getOrElse(k1, (-1.0f,1))
      val (r2,s2) = blockMap.getOrElse(k2, (-1.0f,1))
      if (r1/s1 > r2/s2) k1 else k2
    }
    keys.reduceLeft(max)
  }

  def getBestBlocks(keys: List[String]) = {
    // compute sum of R and S
    //var blocks = blockMap.filter(x => keys.contains(x._1)).values
    //var value = blocks.map(_._1).sum / blocks.map(_._2).sum
    //var sR = blockMap.filter(x => keys.contains(x._1)).values.map(_._1).sum
    //var sS = blockMap.filter(x => keys.contains(x._2)).values.map(_._2).sum
    //var blockKeys = keys.intersect(blockMap.keys.toSeq)
    //if (blockKeys.size > 0) count+=1
    var value = avgRank/totSize
    def goodBlock(k: String) = {
      val (r,s) = blockMap.getOrElse(k, (-1.0f,1))
      val rs = r/s
      (rs > value)
    }
    val blocks = keys.filter(goodBlock)
    if (blocks.size == 0) {
      List(getMax(keys))
    } else
      blocks
  }

  def get(key: String) = blockMap(key)

  def reset(key: String) = {
    val (or, os) = blockMap(key)
    blockMap.remove(key)
    val ts = rankMap(or/os)
    if (ts.size == 1)
      rankMap.remove(or/os)
    else
      ts.remove(key)
  }

  def isEmpty(dId: Int) = {
    blockMap.isEmpty
  }

}
