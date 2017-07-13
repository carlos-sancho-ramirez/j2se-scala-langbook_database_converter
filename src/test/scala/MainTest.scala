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

  it should "include a word with multiple Spanish words" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArrays = Array("recibir", "realizar (un exámen, classes...)", "aceptar (un reto)")
    val kanjiArray = "受ける"
    val kanaArray = "うける"
    val esArray = esArrays.mkString(", ")

    val oldWords = Iterable(Main.OldWord(kanjiArray, kanaArray, esArray))
    Main.convertCollections(oldWords)

    val kanjiIndex = bufferSet.symbolArrays.indexOf(kanjiArray)
    kanjiIndex should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArrays(0))
    esIndex1 should be >= 0

    val esIndex2 = bufferSet.symbolArrays.indexOf(esArrays(1))
    esIndex2 should be >= 0

    val esIndex3 = bufferSet.symbolArrays.indexOf(esArrays(2))
    esIndex3 should be >= 0

    val kanjiReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.representations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.representations(kanaReprIndex).word shouldBe jaWord

    val esWord1 = bufferSet.representations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWord2 = bufferSet.representations(esReprIndex2).word
    esWord2 should be >= 0
    jaWord should not be esWord2
    esWord1 should not be esWord2

    val esWord3 = bufferSet.representations(esReprIndex3).word
    esWord3 should be >= 0
    jaWord should not be esWord3
    esWord1 should not be esWord3
    esWord2 should not be esWord3

    val jaAccIndex = bufferSet.acceptations.indexWhere(_.word == jaWord)
    jaAccIndex should be >= 0

    val esAccIndex1 = bufferSet.acceptations.indexWhere(_.word == esWord1)
    esAccIndex1 should be >= 0

    val esAccIndex2 = bufferSet.acceptations.indexWhere(_.word == esWord2)
    esAccIndex2 should be >= 0

    val esAccIndex3 = bufferSet.acceptations.indexWhere(_.word == esWord3)
    esAccIndex3 should be >= 0

    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex1).concept
    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex2).concept
    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex3).concept
  }

  it should "include a word with multiple Spanish concepts" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArray1 = "caramelo"
    val esArray2 = "pastel"
    val esArray3 = "pasta"
    val kanjiArray = "菓子"
    val kanaArray = "かし"
    val esArray = esArray1 + "; " + esArray2 + ", " + esArray3

    val oldWords = Iterable(Main.OldWord(kanjiArray, kanaArray, esArray))
    Main.convertCollections(oldWords)

    val kanjiIndex = bufferSet.symbolArrays.indexOf(kanjiArray)
    kanjiIndex should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArray1)
    esIndex1 should be >= 0

    val esIndex2 = bufferSet.symbolArrays.indexOf(esArray2)
    esIndex2 should be >= 0

    val esIndex3 = bufferSet.symbolArrays.indexOf(esArray3)
    esIndex3 should be >= 0

    val kanjiReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.representations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.representations(kanaReprIndex).word shouldBe jaWord

    val esWord1 = bufferSet.representations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWord2 = bufferSet.representations(esReprIndex2).word
    esWord2 should be >= 0
    jaWord should not be esWord2
    esWord1 should not be esWord2

    val esWord3 = bufferSet.representations(esReprIndex3).word
    esWord3 should be >= 0
    jaWord should not be esWord3
    esWord1 should not be esWord3
    esWord2 should not be esWord3

    val jaAccIndex1 = bufferSet.acceptations.indexWhere(_.word == jaWord)
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.acceptations.indexWhere(_.word == jaWord, jaAccIndex1 + 1)
    jaAccIndex2 should be >= 0

    val esAccIndex1 = bufferSet.acceptations.indexWhere(_.word == esWord1)
    esAccIndex1 should be >= 0

    val esAccIndex2 = bufferSet.acceptations.indexWhere(_.word == esWord2)
    esAccIndex2 should be >= 0

    val esAccIndex3 = bufferSet.acceptations.indexWhere(_.word == esWord3)
    esAccIndex3 should be >= 0

    val concept1 = bufferSet.acceptations(jaAccIndex1).concept
    val concept2 = bufferSet.acceptations(jaAccIndex2).concept

    if (bufferSet.acceptations(esAccIndex1).concept == concept1) {
      bufferSet.acceptations(esAccIndex2).concept shouldBe concept2
      bufferSet.acceptations(esAccIndex3).concept shouldBe concept2
    }
    else {
      bufferSet.acceptations(esAccIndex1).concept shouldBe concept2
      bufferSet.acceptations(esAccIndex2).concept shouldBe concept1
      bufferSet.acceptations(esAccIndex3).concept shouldBe concept1
    }
  }

  it should "reuse Spanish words already included" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "大事"
    val kanaArray1 = "だいじ"
    val kanjiArray2 = "大切"
    val kanaArray2 = "たいせつ"
    val esArray = "importante"

    val oldWords = Iterable(
      Main.OldWord(kanjiArray1, kanaArray1, esArray),
      Main.OldWord(kanjiArray2, kanaArray2, esArray)
    )
    Main.convertCollections(oldWords)

    val kanjiIndex1 = bufferSet.symbolArrays.indexOf(kanjiArray1)
    kanjiIndex1 should be >= 0

    val kanaIndex1 = bufferSet.symbolArrays.indexOf(kanaArray1)
    kanaIndex1 should be >= 0

    val kanjiIndex2 = bufferSet.symbolArrays.indexOf(kanjiArray2)
    kanjiIndex2 should be >= 0

    val kanaIndex2 = bufferSet.symbolArrays.indexOf(kanaArray2)
    kanaIndex2 should be >= 0

    val esIndex = bufferSet.symbolArrays.indexOf(esArray)
    esIndex should be >= 0

    val kanjiReprIndex1 = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanjiIndex1 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex1 = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanaIndex1 && repr.alphabet == Main.kanaAlphabet)
    val kanjiReprIndex2 = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanjiIndex2 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex2 = bufferSet.representations.indexWhere(repr => repr.symbolArray == kanaIndex2 && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.representations.indexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord1 = bufferSet.representations(kanjiReprIndex1).word
    jaWord1 should be >= 0
    bufferSet.representations(kanaReprIndex1).word shouldBe jaWord1

    val jaWord2 = bufferSet.representations(kanjiReprIndex2).word
    jaWord2 should be >= 0
    bufferSet.representations(kanaReprIndex2).word shouldBe jaWord2

    val esWord = bufferSet.representations(esReprIndex).word
    esWord should be >= 0
    jaWord1 should not be esWord
    jaWord2 should not be esWord

    val jaAccIndex1 = bufferSet.acceptations.indexWhere(_.word == jaWord1)
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.acceptations.indexWhere(_.word == jaWord2)
    jaAccIndex2 should be >= 0

    val esAccIndex = bufferSet.acceptations.indexWhere(_.word == esWord)
    esAccIndex should be >= 0

    bufferSet.acceptations(jaAccIndex1).concept shouldBe bufferSet.acceptations(esAccIndex).concept
    bufferSet.acceptations(jaAccIndex2).concept shouldBe bufferSet.acceptations(esAccIndex).concept
  }
}
