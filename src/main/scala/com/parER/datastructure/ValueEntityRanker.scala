package com.parER.datastructure

import scala.collection.mutable

class ValueEntityRanker {

  // RankMaps
  val entityMaps = List (new mutable.HashMap[Int, (Float, Int)](),
    new mutable.HashMap[Int, (Float, Int)]())

  // RankMaps
  val rankMaps = List (new mutable.TreeMap[Float, mutable.TreeSet[Int]](),
    new mutable.TreeMap[Float, mutable.TreeSet[Int]]())

  // Time: O(Log(n)) -> may be inefficient
  // Notice that # of values == # of entities
  // Rank and size values may be negative values
  def update(id: Int, dId: Int, rank: Float, size: Int) = {
    val em = entityMaps(dId)
    val rm = rankMaps(dId)

    var newEntity = true
    var updatable = true
    var oldRank = 0.0f
    var newRank = 0.0f

    if (em.contains(id)) {
      val (or, os) = em(id)
      updatable = (or + rank > 0.0f) && (os+size > 0)

      oldRank = or/os
      newRank = (or+rank)/(os+size)

      if (updatable)
        em.update(id, (or+rank, os+size))
      else
        em.remove(id)

      val ts = rm(oldRank)
      if (ts.size == 1)
        rm.remove(oldRank)
      else
        ts.remove(id)
    } else {
      em.addOne((id, (rank, size)))
      oldRank = rank/size
      newRank = rank/size
    }

    if (updatable) {
      if (rm.contains(newRank))
        rm(newRank) += id
      else {
        val ts = new mutable.TreeSet[Int]()
        ts.addOne(id)
        rm.update(newRank, ts)
      }
    }
  }

  // Time: O(Log(n)) -> may be inefficient
  def reset(id: Int, dId: Int) = {
    val em = entityMaps(dId)
    val rm = rankMaps(dId)
    val (or, os) = em(id)

    em.remove(id)
    val ts = rm(or/os)
    if (ts.size == 1)
      rm.remove(or/os)
    else
      ts.remove(id)
  }

  // Time: O(1)
  def getMax() = {
    val m0 = getMaxDID(0)
    val m1 = getMaxDID(1)
    (m0, m1) match {
      case (Some(max0), Some(max1)) => {
        if (max0._1 > max1._1)
          Some (max0._2.head, 0, max0._1)
        else Some (max1._2.head, 1, max1._1)
      }
      case _ => None
    }
  }

  // Time: O(1)
  def isEmpty(dId: Int) = {
    entityMaps(dId).isEmpty
  }

  private def getMaxDID(dId: Int) = {
    val rm = rankMaps(dId)
    if (rm.size > 0)
      Some(rm.last)
    else
      None
  }
}
