import scala.collection.{AbstractIterable, AbstractIterator, mutable}

class Conversion private[Conversion] (val sourceAlphabet: Int, val targetAlphabet: Int, val sources: Array[Int], val targets: Array[Int]) extends AbstractIterable[(Int, Int)] {

  if (sources.length != targets.length) {
    throw new IllegalArgumentException()
  }

  private[Conversion] class ConversionPairIterator extends AbstractIterator[(Int, Int)] {
    var index = 0
    override def hasNext: Boolean  = index < sources.length
    override def next(): (Int, Int) = {
      val value = (sources(index), targets(index))
      index += 1
      value
    }
  }

  override def iterator = new ConversionPairIterator()
}

object Conversion {
  def apply(sourceAlphabet: Int, targetAlphabet: Int, pairs: Iterable[(Int, Int)]): Conversion = {
    val sources = mutable.ArrayBuilder.make[Int]()
    val targets = mutable.ArrayBuilder.make[Int]()
    for (pair <- pairs) {
      sources += pair._1
      targets += pair._2
    }

    new Conversion(sourceAlphabet, targetAlphabet, sources.result(), targets.result())
  }
}