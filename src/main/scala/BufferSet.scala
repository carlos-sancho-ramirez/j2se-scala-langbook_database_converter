import scala.collection.mutable.ArrayBuffer

case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)
case class Acceptation(word: Int, concept: Int)
case class AccRepresentation(acc: Int, symbolArray: Int) // There is no alphabet because so far it is always "kanji"

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
  val accRepresentations = ArrayBuffer[AccRepresentation]()

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
}
