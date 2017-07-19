import org.scalatest.{Matchers, FlatSpec}

class MainTest extends FlatSpec with Matchers {

  behavior of "Main.convertCollections"
  it should "include a word and its acceptation properly" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray = "家"
    val kanaArray = "いえ"
    val esArray = "casa"

    val oldWords = Iterable(Main.OldWord(kanjiArray, kanaArray, esArray))
    Main.convertCollections(oldWords)

    val kanjiIndex = bufferSet.symbolArrays.indexOf(kanjiArray)
    kanjiIndex should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex = bufferSet.symbolArrays.indexOf(esArray)
    esIndex should be >= 0

    val kanjiReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex).word shouldBe jaWord

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
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

    val kanjiReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex).word shouldBe jaWord

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0
    jaWord should not be esWord2
    esWord1 should not be esWord2

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
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

    val kanjiReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex).word shouldBe jaWord

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0
    jaWord should not be esWord2
    esWord1 should not be esWord2

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
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

    val kanjiReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex1 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex1 && repr.alphabet == Main.kanaAlphabet)
    val kanjiReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex2 && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord1 = bufferSet.wordRepresentations(kanjiReprIndex1).word
    jaWord1 should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex1).word shouldBe jaWord1

    val jaWord2 = bufferSet.wordRepresentations(kanjiReprIndex2).word
    jaWord2 should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex2).word shouldBe jaWord2

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
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

  it should "include 2 accRepresentations when only matching its pronunciation" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "早い"
    val kanjiArray2 = "速い"
    val kanaArray = "はやい"
    val esArray1 = "temprano"
    val esArray2 = "rápido"

    val oldWords = Iterable(
      Main.OldWord(kanjiArray1, kanaArray, esArray1),
      Main.OldWord(kanjiArray2, kanaArray, esArray2)
    )
    Main.convertCollections(oldWords)

    val kanjiIndex1 = bufferSet.symbolArrays.indexOf(kanjiArray1)
    kanjiIndex1 should be >= 0

    val kanjiIndex2 = bufferSet.symbolArrays.indexOf(kanjiArray2)
    kanjiIndex2 should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArray1)
    esIndex1 should be >= 0

    val esIndex2 = bufferSet.symbolArrays.indexOf(esArray2)
    esIndex2 should be >= 0

    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word

    val kanjiReprIndex1 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex1)
    val kanjiReprIndex2 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2)

    val jaAccIndex1 = bufferSet.accRepresentations(kanjiReprIndex1).acc
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.accRepresentations(kanjiReprIndex2).acc
    jaAccIndex2 should be >= 0

    bufferSet.acceptations(jaAccIndex1).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord

    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0

    jaWord should not be esWord1
    jaWord should not be esWord2

    val esConcept1 = bufferSet.acceptations.find(_.word == esWord1).get.concept
    val esConcept2 = bufferSet.acceptations.find(_.word == esWord2).get.concept
    val jaConcept1 = bufferSet.acceptations(jaAccIndex1).concept
    val jaConcept2 = bufferSet.acceptations(jaAccIndex2).concept

    jaConcept1 shouldBe esConcept1
    jaConcept2 shouldBe esConcept2
    jaConcept1 should not be jaConcept2

    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex1) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex2) shouldBe false
  }

  it should "include 3 accRepresentations when 3 words match its pronunciation" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "髪"
    val kanjiArray2 = "紙"
    val kanjiArray3 = "神"
    val kanaArray = "かみ"
    val esArray1 = "cabello"
    val esArray2 = "papel"
    val esArray3 = "dios"

    val oldWords = Iterable(
      Main.OldWord(kanjiArray1, kanaArray, esArray1),
      Main.OldWord(kanjiArray2, kanaArray, esArray2),
      Main.OldWord(kanjiArray3, kanaArray, esArray3)
    )
    Main.convertCollections(oldWords)

    val kanjiIndex1 = bufferSet.symbolArrays.indexOf(kanjiArray1)
    kanjiIndex1 should be >= 0

    val kanjiIndex2 = bufferSet.symbolArrays.indexOf(kanjiArray2)
    kanjiIndex2 should be >= 0

    val kanjiIndex3 = bufferSet.symbolArrays.indexOf(kanjiArray3)
    kanjiIndex3 should be >= 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArray1)
    esIndex1 should be >= 0

    val esIndex2 = bufferSet.symbolArrays.indexOf(esArray2)
    esIndex2 should be >= 0

    val esIndex3 = bufferSet.symbolArrays.indexOf(esArray3)
    esIndex3 should be >= 0

    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word

    val kanjiReprIndex1 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex1)
    val kanjiReprIndex2 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2)
    val kanjiReprIndex3 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex3)

    val jaAccIndex1 = bufferSet.accRepresentations(kanjiReprIndex1).acc
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.accRepresentations(kanjiReprIndex2).acc
    jaAccIndex2 should be >= 0

    val jaAccIndex3 = bufferSet.accRepresentations(kanjiReprIndex3).acc
    jaAccIndex3 should be >= 0

    bufferSet.acceptations(jaAccIndex1).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex3).word shouldBe jaWord

    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
    esWord3 should be >= 0

    jaWord should not be esWord1
    jaWord should not be esWord2
    jaWord should not be esWord3

    val esConcept1 = bufferSet.acceptations.find(_.word == esWord1).get.concept
    val esConcept2 = bufferSet.acceptations.find(_.word == esWord2).get.concept
    val esConcept3 = bufferSet.acceptations.find(_.word == esWord3).get.concept
    val jaConcept1 = bufferSet.acceptations(jaAccIndex1).concept
    val jaConcept2 = bufferSet.acceptations(jaAccIndex2).concept
    val jaConcept3 = bufferSet.acceptations(jaAccIndex3).concept

    jaConcept1 shouldBe esConcept1
    jaConcept2 shouldBe esConcept2
    jaConcept3 shouldBe esConcept3
    jaConcept1 should not be jaConcept2
    jaConcept1 should not be jaConcept3
    jaConcept2 should not be jaConcept3

    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex1) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex2) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex3) shouldBe false
  }

  private def checkSameJapaneseWordWithMoreThan2Concepts(shifter: Iterable[Main.OldWord] => Iterable[Main.OldWord]) = {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "訪ねる"
    val kanjiArray2 = "尋ねる"
    val kanaArray = "たずねる"
    val esArray1 = "visitar"
    val esArrays2 = Array(
      Array("preguntar", "indagar"),
      Array("buscar", "investigar")
    )
    val esArray2 = esArrays2.map(_.mkString(", ")).mkString("; ")

    val oldWords = shifter(Iterable(
      Main.OldWord(kanjiArray1, kanaArray, esArray1),
      Main.OldWord(kanjiArray2, kanaArray, esArray2)
    ))
    Main.convertCollections(oldWords)

    val kanjiIndex1 = bufferSet.symbolArrays.indexOf(kanjiArray1)
    kanjiIndex1 should be >= 0
    bufferSet.symbolArrays.indexOf(kanjiArray1, kanjiIndex1 + 1) should be < 0

    val kanjiIndex2 = bufferSet.symbolArrays.indexOf(kanjiArray2)
    kanjiIndex2 should be >= 0
    bufferSet.symbolArrays.indexOf(kanjiArray2, kanjiIndex2 + 1) should be < 0

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0
    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArray1)
    esIndex1 should be >= 0
    bufferSet.symbolArrays.indexOf(esArray1, esIndex1 + 1) should be < 0

    val esIndexes2 = esArrays2.map(_.map(bufferSet.symbolArrays.indexOf))
    for (arrays <- esIndexes2; array <- arrays) {
      array should be >= 0
    }

    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex1) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex2) shouldBe false

    val kanjiAccReprIndex1 = bufferSet.accRepresentations.indexWhere(_.symbolArray == kanjiIndex1)
    kanjiAccReprIndex1 should be >= 0

    val kanjiAccReprIndex2a = bufferSet.accRepresentations.indexWhere(_.symbolArray == kanjiIndex2)
    kanjiAccReprIndex2a should be >= 0

    val kanjiAccReprIndex2b = bufferSet.accRepresentations.indexWhere(_.symbolArray == kanjiIndex2, kanjiAccReprIndex2a + 1)
    kanjiAccReprIndex2b should be >= 0

    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    kanaReprIndex should be >= 0

    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    esReprIndex1 should be >= 0

    val esReprIndexes2 = esIndexes2.map(_.map(index => bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == index && repr.alphabet == Main.esAlphabet)))
    for (indexes <- esReprIndexes2; index <- indexes) index should be >= 0

    val accIndex1 = bufferSet.accRepresentations(kanjiAccReprIndex1).acc
    val accIndex2a = bufferSet.accRepresentations(kanjiAccReprIndex2a).acc
    val accIndex2b = bufferSet.accRepresentations(kanjiAccReprIndex2b).acc

    val concept1 = bufferSet.acceptations(accIndex1).concept
    val concept2a = bufferSet.acceptations(accIndex2a).concept
    val concept2b = bufferSet.acceptations(accIndex2b).concept
    concept1 should not be concept2a
    concept1 should not be concept2b
    concept2a should not be concept2b

    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word
    bufferSet.acceptations(accIndex1).word shouldBe jaWord
    bufferSet.acceptations(accIndex2a).word shouldBe jaWord
    bufferSet.acceptations(accIndex2b).word shouldBe jaWord

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWords2 = esReprIndexes2.map(_.map(index => bufferSet.wordRepresentations(index).word))
    for (words <- esWords2; word <- words) {
      word should be >= 0
      word should not be jaWord
      word should not be esWord1
    }

    val esConceptIndexes2 = esWords2.map(_.map { word =>
      bufferSet.acceptations.collectFirst {
        case acc if acc.word == word => acc.concept
      }.get
    })

    val esConcepts2 = esConceptIndexes2.map(_.reduce { (a,b) =>
      if (a != b) throw new AssertionError()
      a
    }).toSet

    esConcepts2 should contain (concept2a)
    esConcepts2 should contain (concept2b)
  }

  it should "include same Japanese word with more than 2 concepts" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a)
  }

  it should "include same Japanese word with more than 2 concepts (order reversed)" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a.foldLeft(List[Main.OldWord]())((list, e) => e :: list))
  }

  it should "not include the kanji if it matches the kana" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanaArray = "だから"
    val esArray = "por esa razón"

    val oldWords = Iterable(Main.OldWord(kanaArray, kanaArray, esArray))
    Main.convertCollections(oldWords)

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0

    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val esIndex = bufferSet.symbolArrays.indexOf(esArray)
    esIndex should be >= 0

    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex)
    bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex, kanaReprIndex + 1) should be < 0
    bufferSet.wordRepresentations(kanaReprIndex).alphabet shouldBe Main.kanaAlphabet

    val esReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word
    jaWord should be >= 0

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
    esWord should be >= 0
    jaWord should not be esWord

    val jaAccIndex = bufferSet.acceptations.indexWhere(_.word == jaWord)
    jaAccIndex should be >= 0

    val esAccIndex = bufferSet.acceptations.indexWhere(_.word == esWord)
    esAccIndex should be >= 0

    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex).concept
  }

  it should "include a word with kanji representation even if the word is included without kanji for other concept" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanaArray = "ただ"
    val kanjiArray2 = "只"
    val esArray1 = "gratis"
    val esArrays2 = Array("ordinario", "común")
    val esArray2 = esArrays2.mkString(", ")

    val oldWords = Iterable(
      Main.OldWord(kanaArray, kanaArray, esArray1),
      Main.OldWord(kanjiArray2, kanaArray, esArray2)
    )
    Main.convertCollections(oldWords)

    val kanaIndex = bufferSet.symbolArrays.indexOf(kanaArray)
    kanaIndex should be >= 0
    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val kanjiIndex2 = bufferSet.symbolArrays.indexOf(kanjiArray2)
    kanjiIndex2 should be >= 0

    val esIndex1 = bufferSet.symbolArrays.indexOf(esArray1)
    esIndex1 should be >= 0

    val esIndex2 = esArrays2.map(bufferSet.symbolArrays.indexOf)
    for (index <- esIndex2) index should be >= 0

    val kanaReprIndex = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex)
    bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex, kanaReprIndex + 1) should be < 0
    bufferSet.wordRepresentations(kanaReprIndex).alphabet shouldBe Main.kanaAlphabet

    val kanjiAccReprIndex2 = bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2)
    kanjiAccReprIndex2 should be >= 0
    bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2, kanjiAccReprIndex2 + 1) should be < 0
    bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanaReprIndex) should be < 0

    val esReprIndex1 = bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    esReprIndex1 should be >= 0

    val esReprIndex2 = esIndex2.map(index => bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == index && repr.alphabet == Main.esAlphabet))
    for (index <- esReprIndex2) index should be >= 0

    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word
    jaWord should be >= 0

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0
    jaWord should not be esWord1

    val esWord2 = esReprIndex2.map(index => bufferSet.wordRepresentations(index).word)
    for (index <- esWord2) {
      index should be >= 0
      index should not be jaWord
      index should not be esWord1
    }
    esWord2.toSet.size shouldBe esWord2.length

    val (_, jaAccIndexes) = bufferSet.acceptations.foldLeft((0, Set[Int]())) { case ((i, set), acc) =>
      if (acc.word == jaWord) (i + 1, set + i)
      else (i + 1, set)
    }
    jaAccIndexes should have size 2

    val esAccIndex1 = bufferSet.acceptations.indexWhere(_.word == esWord1)
    esAccIndex1 should be >= 0

    val esAccIndex2 = esWord2.map(index => bufferSet.acceptations.indexWhere(_.word == index))
    for (index <- esAccIndex2) {
      index should be >= 0
      index should not be esAccIndex1
      jaAccIndexes should not contain index
    }

    val concept1 = bufferSet.acceptations(esAccIndex1).concept
    val concept2 = esAccIndex2.reduce { (a,b) =>
      val aConcept = bufferSet.acceptations(a).concept
      val bConcept = bufferSet.acceptations(b).concept
      if (aConcept == bConcept) aConcept else -1
    }

    concept1 should be >= 0
    concept2 should be >= 0
    concept1 should not be concept2

    val jaConcepts = jaAccIndexes.map(index => bufferSet.acceptations(index).concept)
    jaConcepts should have size 2
    jaConcepts(concept1) shouldBe true
    jaConcepts(concept2) shouldBe true
  }
}
