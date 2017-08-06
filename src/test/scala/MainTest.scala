import org.scalatest.{Matchers, FlatSpec}

class MainTest extends FlatSpec with Matchers {

  case class TestableIndexedSeq[U](seq: IndexedSeq[U]) {
    def findIndexOf(element: U): Int = {
      val index = seq.indexOf(element)
      index should be >= 0
      index
    }

    def findIndexWhere(predicate: U => Boolean): Int = {
      val index = seq.indexWhere(predicate)
      index should be >= 0
      index
    }

    def findIndexWhere(predicate: U => Boolean, from: Int): Int = {
      val index = seq.indexWhere(predicate, from)
      index should be >= 0
      index
    }
  }

  implicit def indexedSeq2TestableIndexedSeq[U](seq: IndexedSeq[U]):TestableIndexedSeq[U] = {
    TestableIndexedSeq(seq)
  }

  behavior of "Main.convertCollections"
  it should "include a word and its acceptation properly" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray = "家"
    val kanaArray = "いえ"
    val esArray = "casa"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation("家", "いえ")
      )
    )

    val oldWords = Iterable(OldWord(1, kanjiArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex = bufferSet.symbolArrays findIndexOf kanjiArray
    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    val esIndex = bufferSet.symbolArrays findIndexOf esArray

    val kanjiReprIndex = bufferSet.wordRepresentations findIndexWhere (repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere (repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.wordRepresentations findIndexWhere (repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanjiReprIndex).word
    jaWord should be >= 0
    bufferSet.wordRepresentations(kanaReprIndex).word shouldBe jaWord

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
    esWord should be >= 0
    jaWord should not be esWord

    val jaAccIndex = bufferSet.acceptations findIndexWhere (_.word == jaWord)
    val esAccIndex = bufferSet.acceptations findIndexWhere (_.word == esWord)

    val concept = bufferSet.acceptations(jaAccIndex).concept
    concept shouldBe bufferSet.acceptations(esAccIndex).concept

    val kanjiKanaCorrelationIndex = bufferSet.kanjiKanaCorrelations.indexOf((kanjiIndex, kanaIndex))
    val corrSet = bufferSet.jaWordCorrelations(jaWord)
    corrSet.size shouldBe 1
    corrSet.head._1 shouldBe Set(concept)
    corrSet.head._2 shouldBe Vector(kanjiKanaCorrelationIndex)
  }

  it should "include a word with multiple Spanish words" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArrays = Array("recibir", "realizar (un exámen, classes...)", "aceptar (un reto)")
    val kanjiArray = "受ける"
    val kanaArray = "うける"
    val esArray = esArrays.mkString(", ")

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation("受", "う"),
        OldPronunciation("け", "け"),
        OldPronunciation("る", "る")
      )
    )

    val oldWords = Iterable(OldWord(1, kanjiArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex = bufferSet.symbolArrays findIndexOf kanjiArray
    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArrays(0)
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArrays(1)
    val esIndex3 = bufferSet.symbolArrays findIndexOf esArrays(2)

    val kanjiReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

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

    val jaAccIndex = bufferSet.acceptations findIndexWhere(_.word == jaWord)
    val esAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == esWord1)
    val esAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == esWord2)
    val esAccIndex3 = bufferSet.acceptations findIndexWhere(_.word == esWord3)

    val concept = bufferSet.acceptations(jaAccIndex).concept
    concept shouldBe bufferSet.acceptations(esAccIndex1).concept
    concept shouldBe bufferSet.acceptations(esAccIndex2).concept
    concept shouldBe bufferSet.acceptations(esAccIndex3).concept

    val uKanjiSymbolArray = bufferSet.symbolArrays findIndexOf "受"
    val uKanaSymbolArray = bufferSet.symbolArrays findIndexOf "う"
    val keSymbolArray = bufferSet.symbolArrays findIndexOf "け"
    val ruSymbolArray = bufferSet.symbolArrays findIndexOf "る"

    val kanjiKanaCorrelationIndex1 = bufferSet.kanjiKanaCorrelations findIndexOf (uKanjiSymbolArray, uKanaSymbolArray)
    val kanjiKanaCorrelationIndex2 = bufferSet.kanjiKanaCorrelations findIndexOf (keSymbolArray, keSymbolArray)
    val kanjiKanaCorrelationIndex3 = bufferSet.kanjiKanaCorrelations findIndexOf (ruSymbolArray, ruSymbolArray)

    val corrSet = bufferSet.jaWordCorrelations(jaWord)
    corrSet.size shouldBe 1
    corrSet.head._1 shouldBe Set(concept)
    corrSet.head._2 shouldBe Vector(kanjiKanaCorrelationIndex1, kanjiKanaCorrelationIndex2, kanjiKanaCorrelationIndex3)
  }

  it should "include a word with multiple Spanish concepts" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArray1 = "caramelo"
    val esArray2 = "pastel"
    val esArray3 = "pasta"
    val kanjiArray = "菓子"
    val kanaArray = "かし"
    val esArray = esArray1 + "; " + esArray2 + ", " + esArray3

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation("菓", "か"),
        OldPronunciation("子", "し")
      )
    )

    val oldWords = Iterable(OldWord(1, kanjiArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex = bufferSet.symbolArrays findIndexOf kanjiArray
    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArray2
    val esIndex3 = bufferSet.symbolArrays findIndexOf esArray3

    val kanjiReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

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

    val jaAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == jaWord)
    val jaAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == jaWord, jaAccIndex1 + 1)
    val esAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == esWord1)
    val esAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == esWord2)
    val esAccIndex3 = bufferSet.acceptations findIndexWhere(_.word == esWord3)

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

    val kanjiSymbolArray1 = bufferSet.symbolArrays findIndexOf "菓"
    val kanjiSymbolArray2 = bufferSet.symbolArrays findIndexOf "子"
    val kanaSymbolArray1 = bufferSet.symbolArrays findIndexOf "か"
    val kanaSymbolArray2 = bufferSet.symbolArrays findIndexOf "し"

    val kanjiKanaCorrelationIndex1 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiSymbolArray1, kanaSymbolArray1)
    val kanjiKanaCorrelationIndex2 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiSymbolArray2, kanaSymbolArray2)

    val corrSet = bufferSet.jaWordCorrelations(jaWord)
    corrSet.size shouldBe 1
    corrSet.head._1 shouldBe Set(concept1, concept2)
    corrSet.head._2 shouldBe Vector(kanjiKanaCorrelationIndex1, kanjiKanaCorrelationIndex2)
  }

  it should "reuse Spanish words already included" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "大事"
    val kanaArray1 = "だいじ"
    val kanjiArray2 = "大切"
    val kanaArray2 = "たいせつ"
    val esArray = "importante"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation("大", "だい"),
        OldPronunciation("事", "じ")
      ),
      2 -> IndexedSeq(
        OldPronunciation("大", "たい"),
        OldPronunciation("切", "せつ")
      )
    )

    val oldWords = Iterable(
      OldWord(1, kanjiArray1, kanaArray1, esArray),
      OldWord(2, kanjiArray2, kanaArray2, esArray)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex1 = bufferSet.symbolArrays findIndexOf kanjiArray1
    val kanaIndex1 = bufferSet.symbolArrays findIndexOf kanaArray1
    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    val kanaIndex2 = bufferSet.symbolArrays findIndexOf kanaArray2
    val esIndex = bufferSet.symbolArrays findIndexOf esArray

    val kanjiReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex1 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex1 && repr.alphabet == Main.kanaAlphabet)
    val kanjiReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex2 && repr.alphabet == Main.kanjiAlphabet)
    val kanaReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex2 && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

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

    val jaAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == jaWord1)
    val jaAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == jaWord2)
    val esAccIndex = bufferSet.acceptations findIndexWhere(_.word == esWord)

    val concept = bufferSet.acceptations(esAccIndex).concept
    bufferSet.acceptations(jaAccIndex1).concept shouldBe concept
    bufferSet.acceptations(jaAccIndex2).concept shouldBe concept

    val daijiKanjiSymbolArray1 = bufferSet.symbolArrays findIndexOf "大"
    val daijiKanjiSymbolArray2 = bufferSet.symbolArrays findIndexOf "事"
    val taisetsuKanjiSymbolArray1 = daijiKanjiSymbolArray1
    val taisetsuKanjiSymbolArray2 = bufferSet.symbolArrays findIndexOf "切"
    val daijiKanaSymbolArray1 = bufferSet.symbolArrays findIndexOf "だい"
    val daijiKanaSymbolArray2 = bufferSet.symbolArrays findIndexOf "じ"
    val taisetsuKanaSymbolArray1 = bufferSet.symbolArrays findIndexOf "たい"
    val taisetsuKanaSymbolArray2 = bufferSet.symbolArrays findIndexOf "せつ"

    val daijiCorrelationIndex1 = bufferSet.kanjiKanaCorrelations findIndexOf (daijiKanjiSymbolArray1, daijiKanaSymbolArray1)
    val daijiCorrelationIndex2 = bufferSet.kanjiKanaCorrelations findIndexOf (daijiKanjiSymbolArray2, daijiKanaSymbolArray2)
    val taisetsuCorrelationIndex1 = bufferSet.kanjiKanaCorrelations findIndexOf (taisetsuKanjiSymbolArray1, taisetsuKanaSymbolArray1)
    val taisetsuCorrelationIndex2 = bufferSet.kanjiKanaCorrelations findIndexOf (taisetsuKanjiSymbolArray2, taisetsuKanaSymbolArray2)

    val daijiCorrSet = bufferSet.jaWordCorrelations(jaWord1)
    daijiCorrSet.size shouldBe 1
    daijiCorrSet.head._1 shouldBe Set(concept)
    daijiCorrSet.head._2 shouldBe Vector(daijiCorrelationIndex1, daijiCorrelationIndex2)

    val taisetsuCorrSet = bufferSet.jaWordCorrelations(jaWord2)
    taisetsuCorrSet.size shouldBe 1
    taisetsuCorrSet.head._1 shouldBe Set(concept)
    taisetsuCorrSet.head._2 shouldBe Vector(taisetsuCorrelationIndex1, taisetsuCorrelationIndex2)
  }

  it should "include 2 accRepresentations when only matching its pronunciation" in {
    implicit val bufferSet = new BufferSet()

    val kanjiArray1 = "早い"
    val kanjiArray2 = "速い"
    val kanaArray = "はやい"
    val esArray1 = "temprano"
    val esArray2 = "rápido"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation("早", "はや"),
        OldPronunciation("い", "い")
      ),
      2 -> IndexedSeq(
        OldPronunciation("速", "はや"),
        OldPronunciation("い", "い")
      )
    )

    val oldWords = Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex1 = bufferSet.symbolArrays findIndexOf kanjiArray1
    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArray2

    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word

    bufferSet.accRepresentations.length shouldBe 2
    val kanjiReprIndex1 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex1)
    val kanjiReprIndex2 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex2)

    val jaAccIndex1 = bufferSet.accRepresentations(kanjiReprIndex1).acc
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.accRepresentations(kanjiReprIndex2).acc
    jaAccIndex2 should be >= 0

    bufferSet.acceptations(jaAccIndex1).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)

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

    val kanjiSymbolArrayA1 = bufferSet.symbolArrays findIndexOf "早"
    val kanjiSymbolArrayA2 = bufferSet.symbolArrays findIndexOf "い"
    val kanaSymbolArrayA1 = bufferSet.symbolArrays findIndexOf "はや"
    val kanaSymbolArrayA2 = kanjiSymbolArrayA2
    val kanjiSymbolArrayB1 = bufferSet.symbolArrays findIndexOf "速"
    val kanaSymbolArrayB1 = kanaSymbolArrayA1

    val correlationA1 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiSymbolArrayA1, kanaSymbolArrayA1)
    val correlationA2 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiSymbolArrayA2, kanaSymbolArrayA2)
    val correlationB1 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiSymbolArrayB1, kanaSymbolArrayB1)
    val correlationB2 = correlationA2

    val corrSeq = bufferSet.jaWordCorrelations(jaWord).toSeq
    corrSeq.size shouldBe 2
    if (corrSeq.head._1 == Set(jaConcept1)) {
      corrSeq.head._2 shouldBe Vector(correlationA1, correlationA2)
      corrSeq(1)._1 shouldBe Set(jaConcept2)
      corrSeq(1)._2 shouldBe Vector(correlationB1, correlationB2)
    }
    else {
      corrSeq.head._1 shouldBe Set(jaConcept2)
      corrSeq.head._2 shouldBe Vector(correlationB1, correlationB2)
      corrSeq(1)._1 shouldBe Set(jaConcept1)
      corrSeq(1)._2 shouldBe Vector(correlationA1, correlationA2)
    }
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

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(OldPronunciation(kanjiArray1, kanaArray)),
      2 -> IndexedSeq(OldPronunciation(kanjiArray2, kanaArray)),
      3 -> IndexedSeq(OldPronunciation(kanjiArray3, kanaArray))
    )

    val oldWords = Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2),
      OldWord(3, kanjiArray3, kanaArray, esArray3)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex1 = bufferSet.symbolArrays findIndexOf kanjiArray1
    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    val kanjiIndex3 = bufferSet.symbolArrays findIndexOf kanjiArray3
    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArray2
    val esIndex3 = bufferSet.symbolArrays findIndexOf esArray3

    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word

    val kanjiReprIndex1 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex1)
    val kanjiReprIndex2 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex2)
    val kanjiReprIndex3 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex3)

    val jaAccIndex1 = bufferSet.accRepresentations(kanjiReprIndex1).acc
    jaAccIndex1 should be >= 0

    val jaAccIndex2 = bufferSet.accRepresentations(kanjiReprIndex2).acc
    jaAccIndex2 should be >= 0

    val jaAccIndex3 = bufferSet.accRepresentations(kanjiReprIndex3).acc
    jaAccIndex3 should be >= 0

    bufferSet.acceptations(jaAccIndex1).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord
    bufferSet.acceptations(jaAccIndex3).word shouldBe jaWord

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
    esWord3 should be >= 0

    jaWord should not be esWord1
    jaWord should not be esWord2
    jaWord should not be esWord3

    val concept1 = bufferSet.acceptations.find(_.word == esWord1).get.concept
    val concept2 = bufferSet.acceptations.find(_.word == esWord2).get.concept
    val concept3 = bufferSet.acceptations.find(_.word == esWord3).get.concept
    bufferSet.acceptations(jaAccIndex1).concept shouldBe concept1
    bufferSet.acceptations(jaAccIndex2).concept shouldBe concept2
    bufferSet.acceptations(jaAccIndex3).concept shouldBe concept3

    concept1 should not be concept2
    concept1 should not be concept3
    concept2 should not be concept3

    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex1) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex2) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex3) shouldBe false

    val correlation1 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex1, kanaIndex)
    val correlation2 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex2, kanaIndex)
    val correlation3 = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex3, kanaIndex)

    val corrSeq = bufferSet.jaWordCorrelations(jaWord).toSeq.sortWith(_._1.head < _._1.head)
    corrSeq.size shouldBe 3

    corrSeq.head._1 shouldBe Set(concept1)
    corrSeq.head._2 shouldBe Vector(correlation1)
    corrSeq(1)._1 shouldBe Set(concept2)
    corrSeq(1)._2 shouldBe Vector(correlation2)
    corrSeq(2)._1 shouldBe Set(concept3)
    corrSeq(2)._2 shouldBe Vector(correlation3)
  }

  private def checkSameJapaneseWordWithMoreThan2Concepts(shifter: Iterable[OldWord] => Iterable[OldWord]) = {
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

    val kanjiArray1a = "訪"
    val kanjiArray2a = "尋"
    val kanaArrayA = "たず"
    val kanaArrayB = "ねる"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanjiArray1a, kanaArrayA),
        OldPronunciation(kanaArrayB, kanaArrayB)
      ),
      2 -> IndexedSeq(
        OldPronunciation(kanjiArray2a, kanaArrayA),
        OldPronunciation(kanaArrayB, kanaArrayB)
      )
    )

    val oldWords = shifter(Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2)
    ))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanjiIndex1 = bufferSet.symbolArrays findIndexOf kanjiArray1
    bufferSet.symbolArrays.indexOf(kanjiArray1, kanjiIndex1 + 1) should be < 0

    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    bufferSet.symbolArrays.indexOf(kanjiArray2, kanjiIndex2 + 1) should be < 0

    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    bufferSet.symbolArrays.indexOf(esArray1, esIndex1 + 1) should be < 0

    val esIndexes2 = esArrays2.map(_.map(bufferSet.symbolArrays.findIndexOf))

    val kanjiIndex1a = bufferSet.symbolArrays findIndexOf kanjiArray1a
    bufferSet.symbolArrays.indexOf(kanjiArray1a, kanjiIndex1a + 1) should be < 0

    val kanjiIndex2a = bufferSet.symbolArrays findIndexOf kanjiArray2a
    bufferSet.symbolArrays.indexOf(kanjiArray2a, kanjiIndex2a + 1) should be < 0

    val kanaIndexA = bufferSet.symbolArrays findIndexOf kanaArrayA
    bufferSet.symbolArrays.indexOf(kanaArrayA, kanaIndexA + 1) should be < 0

    val kanaIndexB = bufferSet.symbolArrays findIndexOf kanaArrayB
    bufferSet.symbolArrays.indexOf(kanaArrayB, kanaIndexB + 1) should be < 0

    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex1) shouldBe false
    bufferSet.wordRepresentations.exists(_.symbolArray == kanjiIndex2) shouldBe false

    val kanjiAccReprIndex1 = bufferSet.accRepresentations findIndexWhere(_.symbolArray == kanjiIndex1)
    val kanjiAccReprIndex2a = bufferSet.accRepresentations findIndexWhere(_.symbolArray == kanjiIndex2)
    val kanjiAccReprIndex2b = bufferSet.accRepresentations findIndexWhere(_.symbolArray == kanjiIndex2, kanjiAccReprIndex2a + 1)
    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndexes2 = esIndexes2.map(_.map(index => bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == index && repr.alphabet == Main.esAlphabet)))

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

    val correlation1a = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex1a, kanaIndexA)
    val correlation2a = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex2a, kanaIndexA)
    val correlationB = bufferSet.kanjiKanaCorrelations findIndexOf (kanaIndexB, kanaIndexB)

    val corrSeq = bufferSet.jaWordCorrelations(jaWord).toSeq
    corrSeq.size shouldBe 2

    if (corrSeq.head._1 == Set(concept1)) {
      corrSeq.head._2 shouldBe Vector(correlation1a, correlationB)
      corrSeq(1)._1 shouldBe Set(concept2a, concept2b)
      corrSeq(1)._2 shouldBe Vector(correlation2a, correlationB)
    }
    else {
      corrSeq.head._1 shouldBe Set(concept2a, concept2b)
      corrSeq.head._2 shouldBe Vector(correlation2a, correlationB)
      corrSeq(1)._1 shouldBe Set(concept1)
      corrSeq(1)._2 shouldBe Vector(correlation1a, correlationB)
    }
  }

  it should "include same Japanese word with more than 2 concepts" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a)
  }

  it should "include same Japanese word with more than 2 concepts (order reversed)" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a.foldLeft(List[OldWord]())((list, e) => e :: list))
  }

  it should "not include the kanji if it matches the kana" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanaArray = "だから"
    val esArray = "por esa razón"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(OldPronunciation(kanaArray, kanaArray))
    )

    val oldWords = Iterable(OldWord(1, kanaArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val esIndex = bufferSet.symbolArrays findIndexOf esArray

    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex)
    bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex, kanaReprIndex + 1) should be < 0
    bufferSet.wordRepresentations(kanaReprIndex).alphabet shouldBe Main.kanaAlphabet

    val esReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val jaWord = bufferSet.wordRepresentations(kanaReprIndex).word
    jaWord should be >= 0

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
    esWord should be >= 0
    jaWord should not be esWord

    val jaAccIndex = bufferSet.acceptations findIndexWhere(_.word == jaWord)
    val esAccIndex = bufferSet.acceptations findIndexWhere(_.word == esWord)

    val concept = bufferSet.acceptations(jaAccIndex).concept
    bufferSet.acceptations(esAccIndex).concept shouldBe concept

    bufferSet.kanjiKanaCorrelations.indexOf((kanaIndex, kanaIndex)) should be < 0
  }

  it should "include a word with kanji representation even if the word is included without kanji for other concept" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanaArray = "ただ"
    val kanjiArray2 = "只"
    val esArray1 = "gratis"
    val esArrays2 = Array("ordinario", "común")
    val esArray2 = esArrays2.mkString(", ")

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(OldPronunciation(kanaArray, kanaArray)),
      2 -> IndexedSeq(OldPronunciation(kanjiArray2, kanaArray))
    )

    val oldWords = Iterable(
      OldWord(1, kanaArray, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanaIndex = bufferSet.symbolArrays findIndexOf kanaArray
    bufferSet.symbolArrays.indexOf(kanaArray, kanaIndex + 1) should be < 0

    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = esArrays2.map(bufferSet.symbolArrays.findIndexOf)

    val kanaReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == kanaIndex)
    bufferSet.wordRepresentations.indexWhere(repr => repr.symbolArray == kanaIndex, kanaReprIndex + 1) should be < 0
    bufferSet.wordRepresentations(kanaReprIndex).alphabet shouldBe Main.kanaAlphabet

    val kanjiAccReprIndex2 = bufferSet.accRepresentations findIndexWhere(repr => repr.symbolArray == kanjiIndex2)
    bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanjiIndex2, kanjiAccReprIndex2 + 1) should be < 0
    bufferSet.accRepresentations.indexWhere(repr => repr.symbolArray == kanaReprIndex) should be < 0

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = esIndex2.map(index => bufferSet.wordRepresentations.findIndexWhere(repr => repr.symbolArray == index && repr.alphabet == Main.esAlphabet))

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

    val esAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == esWord1)
    val esAccIndex2 = esWord2.map(index => bufferSet.acceptations.findIndexWhere(_.word == index))
    for (index <- esAccIndex2) {
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

    bufferSet.kanjiKanaCorrelations.indexOf((kanaIndex, kanaIndex)) should be < 0
    val correlation = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex2, kanaIndex)

    val corrSet = bufferSet.jaWordCorrelations(jaWord)
    corrSet.size shouldBe 1
    corrSet.head._1 shouldBe Set(concept2)
    corrSet.head._2 shouldBe Vector(correlation)
  }

  it should "ignore any duplicated word within the database" in {
    val kanjiArray = "言語"
    val kanaArray = "げんご"
    val esArray = "idioma"

    val oldWordPronunciations1 = Map(
      1 -> IndexedSeq(OldPronunciation(kanjiArray, kanaArray))
    )

    val oldWordPronunciations2 = Map(
      1 -> IndexedSeq(OldPronunciation(kanjiArray, kanaArray)),
      2 -> IndexedSeq(OldPronunciation(kanjiArray, kanaArray))
    )

    val oldWords1 = Iterable(
      OldWord(1, kanjiArray, kanaArray, esArray)
    )

    val oldWords2 = Iterable(
      OldWord(1, kanjiArray, kanaArray, esArray),
      OldWord(2, kanjiArray, kanaArray, esArray)
    )

    val bufferSet1 = Main.initialiseDatabase()
    Main.convertWords(oldWords1, oldWordPronunciations1)(bufferSet1)

    val bufferSet2 = Main.initialiseDatabase()
    Main.convertWords(oldWords2, oldWordPronunciations2)(bufferSet2)

    bufferSet1 shouldBe bufferSet2
  }

  it should "ignore any duplicated word within the database (with 2 words with same kana)" in {

    val kanjiArray1a = "漢"
    val kanjiArray1b = "字"
    val kanjiArray2a = "感"
    val kanjiArray2b = "じ"
    val kanaArrayA = "かん"
    val kanaArrayB = kanjiArray2b

    val kanjiArray1 = kanjiArray1a + kanjiArray1b
    val kanjiArray2 = kanjiArray2a + kanjiArray2b
    val kanaArray = kanaArrayA + kanaArrayB
    val esArray1 = "kanji"
    val esArray2 = "sensación"

    val oldWordPronunciations1 = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanjiArray1a, kanaArrayA),
        OldPronunciation(kanjiArray1b, kanaArrayB)
      ),
      2 -> IndexedSeq(
        OldPronunciation(kanjiArray2a, kanaArrayA),
        OldPronunciation(kanjiArray2b, kanaArrayB)
      )
    )

    val oldWordPronunciations2 = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanjiArray1a, kanaArrayA),
        OldPronunciation(kanjiArray1b, kanaArrayB)
      ),
      2 -> IndexedSeq(
        OldPronunciation(kanjiArray2a, kanaArrayA),
        OldPronunciation(kanjiArray2b, kanaArrayB)
      ),
      3 -> IndexedSeq(
        OldPronunciation(kanjiArray1a, kanaArrayA),
        OldPronunciation(kanjiArray1b, kanaArrayB)
      )
    )

    val oldWords1 = Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2)
    )

    val oldWords2 = Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2),
      OldWord(3, kanjiArray1, kanaArray, esArray1)
    )

    val bufferSet1 = new BufferSet()
    Main.convertWords(oldWords1, oldWordPronunciations1)(bufferSet1)

    val bufferSet2 = new BufferSet()
    Main.convertWords(oldWords2, oldWordPronunciations2)(bufferSet2)

    bufferSet1 shouldBe bufferSet2
  }
}
