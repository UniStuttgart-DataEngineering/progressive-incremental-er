package com.parER.core.matching

import com.parER.datastructure.{BaseComparison, Comparison}
import com.parER.utils.BloomFilter
import org.scify.jedai.textmodels.TokenNGrams

import scala.collection.mutable

// TODO consider also length of representation
// TODO if length of e1 or e2 is higher than numberOfBits, half (or reduce of some percentage, maybe related to the length of the string itself) the priority score
class BFMatcher(val numberOfBits: Long, val numberOfHashes: Int) extends Matcher {

  val bfIndex = Array( mutable.HashMap[Int, BloomFilter[String]](), mutable.HashMap[Int, BloomFilter[String]]())
  val epsilon = 0.1f // TODO make it locally computable

  override def execute(comparisons: List[BaseComparison]) = {
    val i = comparisons.iterator
    while (i.hasNext) {
      val c = i.next()
      execute(c)
    }
    comparisons
  }

  override def execute(c: BaseComparison): BaseComparison = {
    update(c.e1, c.e1Model)
    update(c.e2, c.e2Model)
    val bf1 = bfIndex(c.e1Model.getDatasetId)(c.e1)
    val bf2 = bfIndex(c.e2Model.getDatasetId)(c.e2)
    val c1 = bf1.cardinality()
    val c2 = bf2.cardinality()
    val (c_min, m_min) = if (c1 < c2) (c1, c.e1Model)
    else (c2, c.e2Model)
    val (c_max, m_max) = if (c1 < c2) (c2, c.e2Model)
    else (c1, c.e1Model)
    val malus = if (c_max > numberOfBits - epsilon) math.min(1.0f , (m_max.getSignatures.size() - numberOfBits + epsilon).toFloat / m_min.getSignatures.size())
    else 0
    val c_and : Float = bf1.intersect(bf2).cardinality()
    c.sim = c_and / c_min - malus
    c
  }

  override def getName(): String = s"BFMatcher(${numberOfBits},${numberOfHashes})"

  private def update(e: Int, t: TokenNGrams) = {
    import scala.jdk.CollectionConverters._
    val dId = t.getDatasetId
    if (!bfIndex(dId).contains(e)) {
      val signatures = t.getSignatures.asScala.toList
      bfIndex(dId)(e) = new BloomFilter[String](numberOfBits, numberOfHashes)
      signatures.foreach( bfIndex(dId)(e).add )
    }
  }
}

object BFMatcher {

  def apply[T](numberOfItems: Long, falsePositiveRate: Float) = {
    val nb = BloomFilter.optimalNumberOfBits(numberOfItems, falsePositiveRate)
    val nh = BloomFilter.optimalNumberOfHashes(numberOfItems, nb)
    new BFMatcher(nb, nh)

  }
}
