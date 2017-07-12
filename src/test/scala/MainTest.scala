import org.scalatest.{Matchers, FlatSpec}

class MainTest extends FlatSpec with Matchers {

  behavior of "Main.convertCollections"
  it should "include a word and its acceptation properly" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray = "英語"
    val kanaArray = "えいご"
    val esArray = "Inglés"

    val oldWords = Iterable(Main.OldWord(kanjiArray, kanaArray, esArray))
    Main.convertCollections(oldWords)

    val kanjiIndex = bufferSet.symbolArrays.indexOf(kanjiArray)
    kanjiIndex should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex = bufferSet.symbolArrays.indexOf(esArray)
    esIndex should be >= 0

    val kanjiReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.representations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.representations(kanaReprIndex).word shouldBe jaWord

    val esWord = bufferSet.representations(esReprIndex).word
    esWord should be >= 0
    jaWord should not be esWord

    val jaAccIndex = bufferSet.acceptations.indexWhere(_.word == jaWord)
    jaAccIndex should be >= 0

    val esAccIndex = bufferSet.acceptations.indexWhere(_.word == esWord)
    esAccIndex should be >= 0

    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex).concept
  }
}
