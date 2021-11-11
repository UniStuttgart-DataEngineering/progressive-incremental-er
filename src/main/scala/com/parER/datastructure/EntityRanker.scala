package com.parER.datastructure

import scala.collection.mutable

class EntityRanker {

  val rankMaps = List ( new mutable.HashMap[Int, Float](), new mutable.HashMap[Int, Float]() )
  val sizeMaps = List ( new mutable.HashMap[Int, Int](), new mutable.HashMap[Int, Int]() )
  val entityMaps = List ( new mutable.TreeMap[Int, Float](), new mutable.TreeMap[Int, Float]() )

  //val pq = mutable.PriorityQueue[(Int, Int, Float)]()(Ordering.by(_._3))
  val pq = mutable.SortedSet[(Int, Int, Float)]()(Ordering.by(_._3))

  val maxSize = 10000

  def update(id: Int, dId: Int, rank: Float, size: Int) = {
    val rmap = rankMaps(dId)
    val rold = rmap.getOrElseUpdate(id, 0.0f)
    rmap.update(id, rold+rank)

    val smap = sizeMaps(dId)
    val sold = smap.getOrElseUpdate(id, 0)
    smap.update(id, sold+size)

    val value = rmap(id) / math.max( 1.0f, smap(id).toFloat )
    if (pq.size == maxSize) {
      val m = pq.min
      if (m._3 < value) {
        pq.remove(m)
        pq.add((id, dId, value))
      }
    } else {
      pq.add((id, dId, value))
    }

    val emap = entityMaps(dId)
    emap(id) = value
  }


  def reset(id: Int, dId: Int) = {
    val rmap = rankMaps(dId)
    rmap(id) = 0.0f
    val smap = sizeMaps(dId)
    smap(id) = 0
    val emap = entityMaps(dId)
    emap(id) = 0
  }

  def getMax() = {
    if (!pq.isEmpty) {
      val m = pq.max
      pq.remove(m)
      m
    } else {
      val ed0 = entityMaps(0).maxBy(_._2)
      val ed1 = entityMaps(1).maxBy(_._2)
      if (ed0._2 > ed1._2) (ed0._1, 0, ed0._2) else (ed1._1, 1, ed1._2)
    }
  }

  def isEmpty(dId: Int) = {
    sizeMaps(dId).isEmpty
  }

}
