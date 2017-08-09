import scala.collection.mutable.ArrayBuffer

case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)
case class Acceptation(word: Int, concept: Int)
case class BunchWord(bunchConcept: Int, word: Int)

object InvalidRegister {
  val wordRepresentation = WordRepresentation(-1, -1, -1)
}

/**
  * Contain all mutable collections representing in memory the current database state.
  */
class BufferSet {
  val wordRepresentations = ArrayBuffer[WordRepresentation]()
  val acceptations = ArrayBuffer[Acceptation]()
  val symbolArrays = ArrayBuffer[String]()
  val bunchWords = ArrayBuffer[BunchWord]()
  val conversions = scala.collection.mutable.Set[Conversion]()

  // This is currently really specific for Japanese, this must be adapted for any alphabet
  val kanjiKanaCorrelations = ArrayBuffer[(Int /* kanji symbol array */, Int /* kana symbol array */)]()

  // This is currently really specific for Japanese.
  // So far Japanese is the only language using correlations, thus for now it is assumed that
  // source alphabet is always kanji and target alphabet is always kana
  // TODO: Make this for any language.
  //val jaWordCorrelations = scala.collection.mutable.Map[Int /* acc id */, Vector[Int /* Indexes within kanjiKanaCorrelations */]]()
  val jaWordCorrelations = scala.collection.mutable.Map[Int /* word id */, Set[(Set[Int] /* concepts */, Vector[Int /* Indexes within kanjiKanaCorrelations */])]]()

  override def hashCode: Int = {
    symbolArrays.length + acceptations.length + bunchWords.length
  }

  override def equals(other: Any): Boolean = {
    other != null && other.isInstanceOf[BufferSet] && {
      val that = other.asInstanceOf[BufferSet]

      symbolArrays == that.symbolArrays &&
      acceptations == that.acceptations &&
      wordRepresentations == that.wordRepresentations &&
      kanjiKanaCorrelations == that.kanjiKanaCorrelations &&
      jaWordCorrelations == that.jaWordCorrelations &&
      bunchWords == that.bunchWords
    }
  }

  /**
    * Checks if the given symbol array already exist in the list.
    * If so, the index is returned. If not it is appended into
    * the list and the index is returned.
    */
  def addSymbolArray(symbolArray: String): Int = {
    if (symbolArray == null) {
      throw new IllegalArgumentException()
    }

    // This should be optimized indexing the string instead of check if it exists one by one.
    var found = -1
    var i = 0
    val size = symbolArrays.size
    while (found < 0 && i < size) {
      if (symbolArrays(i) == symbolArray) found = i
      i += 1
    }

    if (found >= 0) found
    else {
      symbolArrays += symbolArray
      size
    }
  }

  def charCountMap: Map[Char, Int] = {
    symbolArrays.foldLeft(scala.collection.mutable.Map[Char, Int]()) {
      (map, string) =>
        for (char <- string) {
          map(char) = map.getOrElse(char, 0) + 1
        }
        map
    }.toMap
  }

  def fullCharSet: Array[Char] = charCountMap.toArray.sortWith((a,b) => a._2 > b._2).map(_._1)

  def maxWordAndConceptIndexes: (Int, Int) = {
    acceptations.foldLeft((-1, -1)) {
      case ((word, concept), acc) =>
        val maxWord = if (acc.word > word) acc.word else word
        val maxConcept = if (acc.concept > concept) acc.concept else concept
        (maxWord, maxConcept)
    }
  }

  def accRepresentationsMap: Map[(Int /* word id */, String), Set[Int] /* concepts */] = {
    jaWordCorrelations.foldLeft(Map[(Int, String), Set[Int]]()) { (map, jaWordCorr) =>
      val wordId = jaWordCorr._1
      val newEntries = for (setEntry <- jaWordCorr._2) yield {
        val concepts = setEntry._1
        val str = setEntry._2.map(index => symbolArrays(kanjiKanaCorrelations(index)._1)).mkString("")
        ((wordId, str), concepts)
      }

      map ++ newEntries
    }
  }
}
