package com.parER.core.entityscheduling

import com.google.common.collect.MinMaxPriorityQueue
import com.parER.akka.streams.utils.EntityIndexComparator

class EntityContainer(entity: Int, dId: Short) {

  // Max number of comparisons that a PQ can contain
  val maxComparisons = 60

  val PQ : MinMaxPriorityQueue[(Int, Float)] = MinMaxPriorityQueue
    .orderedBy(EntityIndexComparator)
    .maximumSize(maxComparisons)
    .create()

  //private val PQ = mutable.PriorityQueue()(Ordering.by[(Int, Float), Float](_._2))
  //private val stack = mutable.Stack[(Int, Float)]()

  var tot = 0.0f
  var count = 0L
  var rank = 0f

  def size = PQ.size

  def maxSize = maxComparisons

  def inserted = count

  def computeWeight = {
    val it = PQ.iterator()
    var sum = 0.0
    var count = 0
    while (it.hasNext) {
      sum += it.next()._2
      count += 1
    }
    sum/count
  }

//  def enqueue(e: Int, rank: Float) = {
//    if (!PQ.isEmpty) {
//      tot += rank
//      count += 1
//      if (!EntityContainer.pruning || rank > (tot / count) || count < 1)
//        PQ.add((e, rank)) //PQ.enqueue((e, rank))
//    } else if (!EntityContainer.pruning || rank > (tot / count) || count < 1) {
//      //tot = rank
//      //count = 1
//      PQ.add((e, rank)) //PQ.enqueue((e, rank))
//    }
//  }

  def enqueue(e: Int, rank: Float) = {
    //TODO computeWeight not sure is good.
    if (PQ.size() < maxComparisons || computeWeight > rank) {
      tot += rank
      count += 1
      PQ.add((e, rank))
    }
//    if (!PQ.isEmpty) {
//      tot += rank
//      count += 1
//      if (!EntityContainer.pruning || rank > (tot / count) || count < 1)
//        PQ.add((e, rank)) //PQ.enqueue((e, rank))
//    } else if (!EntityContainer.pruning || rank > (tot / count) || count < 1) {
//      //tot = rank
//      //count = 1
//      PQ.add((e, rank)) //PQ.enqueue((e, rank))
//    }
  }

  def push(e: Int, rank: Float) = {
    tot += rank
    count += 1
    PQ.add((e, rank)) //PQ.enqueue((e, rank))
    //stack.push(PQ.peekFirst()) //stack.push(PQ.head)
    this.rank = PQ.peekFirst()._2 //this.rank = PQ.head._2
  }

  def pop() = {
    //stack.pop()
    val pair = PQ.removeFirst() //val pair = PQ.dequeue()
    //this.rank = if (stack.size > 0) stack.head._2 else 0
    this.rank = if (!PQ.isEmpty) PQ.peekFirst()._2 else 0
    pair
  }

  def topToStack() = {
    if (PQ.size > 0) {
      val (e, rank) = PQ.peekFirst() //PQ.head
      //stack.push((e, rank))
      this.rank = rank
      (e, rank)
    } else
      null
  }

}

object EntityContainer {
  var pruning = true
}

