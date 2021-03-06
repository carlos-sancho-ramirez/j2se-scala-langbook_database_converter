import org.scalatest.{Matchers, FlatSpec}

class MainTest extends FlatSpec with Matchers {

  case class TestableIndexedSeq[U](seq: IndexedSeq[U]) {
    def findIndexOf(element: U): Int = {
      val index = seq.indexOf(element)
      if (index < 0) {
        throw new AssertionError(s"Element $element not found in collection")
      }

      index
    }

    def findUniqueIndexOf(element: U): Int = {
      val index = findIndexOf(element)

      val nextIndex = seq.indexOf(element, index + 1)
      if (nextIndex >= 0) {
        throw new AssertionError(s"Element $element found multiple times in collection")
      }
      index
    }

    def findIndexWhere(predicate: U => Boolean): Int = {
      val index = seq.indexWhere(predicate)
      if (index < 0) {
        throw new AssertionError(s"Element not found in collection for the given predicate")
      }
      index
    }

    def findIndexWhere(predicate: U => Boolean, from: Int): Int = {
      val index = seq.indexWhere(predicate, from)
      if (index < 0) {
        throw new AssertionError(s"Element not found in collection from index $from for the given predicate")
      }
      index
    }

    def findUniqueIndexWhere(predicate: U => Boolean): Int = {
      val index = findIndexWhere(predicate)

      val nextIndex = seq.indexWhere(predicate, index + 1)
      if (nextIndex >= 0) {
        throw new AssertionError(s"Element found multiple times in collection for the given predicate")
      }
      index
    }
  }

  implicit def indexedSeq2TestableIndexedSeq[U](seq: IndexedSeq[U]):TestableIndexedSeq[U] = {
    TestableIndexedSeq(seq)
  }

  behavior of "Main.convertCollections"
  it should "include a word and its acceptation properly (old)" in {
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

    bufferSet.wordRepresentations indexWhere (repr => repr.symbolArray == kanjiIndex) should be < 0
    bufferSet.wordRepresentations indexWhere (repr => repr.symbolArray == kanaIndex) should be < 0
    val esReprIndex = bufferSet.wordRepresentations findIndexWhere (repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
    esWord should be >= 0

    val esAccIndex = bufferSet.acceptations findUniqueIndexWhere (_.word == esWord)

    val concept = bufferSet.acceptations(esAccIndex).concept

    val jaAccIndex = bufferSet.acceptations findUniqueIndexWhere(acc => acc.concept == concept && acc.word != esWord)
    val jaWord = bufferSet.acceptations(jaAccIndex).word

    val kanjiKanaCorrelationIndex = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex, kanaIndex)
    val corrSet = bufferSet.jaWordCorrelations(jaWord)
    corrSet.size shouldBe 1
    corrSet.head._1 shouldBe Set(concept)
    corrSet.head._2 shouldBe Vector(kanjiKanaCorrelationIndex)
  }

  private def findUniqueSpanishCorrelationArrayIndexOf(str: String)(implicit bufferSet: BufferSet): Int = {
    val index = bufferSet.symbolArrays findUniqueIndexOf str
    val correlationIndex = bufferSet.correlations findUniqueIndexOf Map(Main.esAlphabet -> index)
    bufferSet.correlationArrays findUniqueIndexOf Vector(correlationIndex)
  }

  private def findUniqueSpanishAcceptationIndex(str: String)(implicit bufferSet: BufferSet): Int = {
    val corrArrayIndex = findUniqueSpanishCorrelationArrayIndexOf(str)
    val results = bufferSet.acceptationCorrelations.collect {
      case (accId, corrArraySet) if corrArraySet.contains(corrArrayIndex) => accId
    }

    if (results.size != 1) {
      throw new AssertionError()
    }

    results.head
  }

  private def findUniqueJapaneseCorrelationArrayIndexOf(corr: (String, String)*)(implicit bufferSet: BufferSet): Int = {
    val corrArray = corr.map { case (kanji, kana) =>
      val kanjiIndex = bufferSet.symbolArrays findUniqueIndexOf kanji
      val kanaIndex = bufferSet.symbolArrays findUniqueIndexOf kana
      bufferSet.correlations findUniqueIndexOf Map(Main.kanjiAlphabet -> kanjiIndex, Main.kanaAlphabet -> kanaIndex)
    }
    bufferSet.correlationArrays findUniqueIndexOf corrArray
  }

  private def findUniqueJapaneseAcceptationIndex(corr: (String, String)*)(implicit bufferSet: BufferSet): Int = {
    val corrArrayIndex = findUniqueJapaneseCorrelationArrayIndexOf(corr: _*)
    val results = bufferSet.acceptationCorrelations.collect {
      case (accId, corrArraySet) if corrArraySet.contains(corrArrayIndex) => accId
    }

    if (results.size != 1) {
      throw new AssertionError()
    }

    results.head
  }

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

    val jaAccIndex = findUniqueJapaneseAcceptationIndex(kanjiArray -> kanaArray)
    val esAccIndex = findUniqueSpanishAcceptationIndex(esArray)
    bufferSet.acceptations(jaAccIndex).concept shouldBe bufferSet.acceptations(esAccIndex).concept
    bufferSet.acceptations(jaAccIndex).word should not be bufferSet.acceptations(esAccIndex).word
  }

  it should "include a word with multiple Spanish words (old)" in {
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

    bufferSet.symbolArrays indexOf kanjiArray should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArrays(0)
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArrays(1)
    val esIndex3 = bufferSet.symbolArrays findIndexOf esArrays(2)

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0
    esWord1 should not be esWord2

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
    esWord3 should be >= 0
    esWord1 should not be esWord3
    esWord2 should not be esWord3

    val esAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == esWord1)
    val esAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == esWord2)
    val esAccIndex3 = bufferSet.acceptations findIndexWhere(_.word == esWord3)

    val concept = bufferSet.acceptations(esAccIndex1).concept
    bufferSet.acceptations(esAccIndex2).concept shouldBe concept
    bufferSet.acceptations(esAccIndex3).concept shouldBe concept

    val allWordsSeq = bufferSet.acceptations collect { case acc if acc.concept == concept => acc.word }
    allWordsSeq.size shouldBe 4

    val jaWordSet = allWordsSeq.toSet - esWord1 - esWord2 - esWord3
    jaWordSet.size shouldBe 1

    val jaWord = jaWordSet.head

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

  it should "include a word with multiple Spanish words" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArrays = Array("recibir", "realizar (un exámen, classes...)", "aceptar (un reto)")
    val kanjiArray = "受ける"
    val kanaArray = "うける"
    val esArray = esArrays.mkString(", ")

    val kanji1 = "受"
    val kana1 = "う"
    val str2 = "け"
    val str3 = "る"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanji1, kana1),
        OldPronunciation(str2, str2),
        OldPronunciation(str3, str3)
      )
    )

    val oldWords = Iterable(OldWord(1, kanjiArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    bufferSet.symbolArrays indexOf kanjiArray should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0

    val jaAcc = findUniqueJapaneseAcceptationIndex(kanji1 -> kana1, str2 -> str2, str3 -> str3)
    val concept = bufferSet.acceptations(jaAcc).concept

    val esAcc1 = findUniqueSpanishAcceptationIndex(esArrays(0))
    val esAcc2 = findUniqueSpanishAcceptationIndex(esArrays(1))
    val esAcc3 = findUniqueSpanishAcceptationIndex(esArrays(2))

    bufferSet.acceptations(esAcc1).concept shouldBe concept
    bufferSet.acceptations(esAcc2).concept shouldBe concept
    bufferSet.acceptations(esAcc3).concept shouldBe concept
  }

  it should "include a word with multiple Spanish concepts (old)" in {
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

    bufferSet.symbolArrays indexOf kanjiArray should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArray2
    val esIndex3 = bufferSet.symbolArrays findIndexOf esArray3

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0
    esWord1 should not be esWord2

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
    esWord3 should be >= 0
    esWord1 should not be esWord3
    esWord2 should not be esWord3

    val esAccIndex1 = bufferSet.acceptations findIndexWhere(_.word == esWord1)
    val esAccIndex2 = bufferSet.acceptations findIndexWhere(_.word == esWord2)
    val esAccIndex3 = bufferSet.acceptations findIndexWhere(_.word == esWord3)

    val concept1 = bufferSet.acceptations(esAccIndex1).concept
    val concept2 = bufferSet.acceptations(esAccIndex2).concept
    concept2 should not be concept1
    bufferSet.acceptations(esAccIndex3).concept shouldBe concept2

    val jaConcepts = Set(concept1, concept2)
    val allWordsSeq = bufferSet.acceptations collect { case acc if jaConcepts(acc.concept) => acc.word }
    allWordsSeq.size shouldBe 5

    val jaWordSet = allWordsSeq.toSet - esWord1 - esWord2 - esWord3
    jaWordSet.size shouldBe 1

    val jaWord = jaWordSet.head

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

  it should "include a word with multiple Spanish concepts" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val esArray1 = "caramelo"
    val esArray2 = "pastel"
    val esArray3 = "pasta"
    val kanjiArray = "菓子"
    val kanaArray = "かし"
    val esArray = esArray1 + "; " + esArray2 + ", " + esArray3

    val kanji1 = "菓"
    val kana1 = "か"
    val kanji2 = "子"
    val kana2 = "し"
    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanji1, kana1),
        OldPronunciation(kanji2, kana2)
      )
    )

    val oldWords = Iterable(OldWord(1, kanjiArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    bufferSet.symbolArrays indexOf kanjiArray should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0

    val jaCorrelationArrayIndex = findUniqueJapaneseCorrelationArrayIndexOf(kanji1 -> kana1, kanji2 -> kana2)
    val jaAccIndexes = bufferSet.acceptationCorrelations.collect {
      case (accId, corrArraySet) if corrArraySet.contains(jaCorrelationArrayIndex) => accId
    }.toSet
    jaAccIndexes.size shouldBe 2
    val jaAccIndex1 = jaAccIndexes.head
    val jaAccIndex2 = (jaAccIndexes - jaAccIndex1).head

    val esAccIndex1 = findUniqueSpanishAcceptationIndex(esArray1)
    val esAccIndex2 = findUniqueSpanishAcceptationIndex(esArray2)
    val esAccIndex3 = findUniqueSpanishAcceptationIndex(esArray3)

    val concept1 = bufferSet.acceptations(esAccIndex1).concept
    val concept2 = bufferSet.acceptations(esAccIndex2).concept
    bufferSet.acceptations(esAccIndex3).concept shouldBe concept2

    if (bufferSet.acceptations(jaAccIndex1).concept == concept1) {
      bufferSet.acceptations(jaAccIndex2).concept shouldBe concept2
    }
    else {
      bufferSet.acceptations(jaAccIndex2).concept shouldBe concept1
      bufferSet.acceptations(jaAccIndex1).concept shouldBe concept2
    }
  }

  it should "reuse Spanish words already included (old)" in {
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

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanaArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray2 should be < 0
    val esIndex = bufferSet.symbolArrays findIndexOf esArray

    val esReprIndex = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex && repr.alphabet == Main.esAlphabet)

    val esWord = bufferSet.wordRepresentations(esReprIndex).word
    esWord should be >= 0

    val esAccIndex = bufferSet.acceptations findIndexWhere(_.word == esWord)

    val concept = bufferSet.acceptations(esAccIndex).concept

    val jaWordsSeq = bufferSet.acceptations collect { case acc if acc.concept == concept && acc.word != esWord => acc.word }
    jaWordsSeq.size shouldBe 2

    val jaWordA = jaWordsSeq.head
    val jaWordB = jaWordsSeq(1)

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

    val corrSetA = bufferSet.jaWordCorrelations(jaWordA)
    corrSetA.size shouldBe 1
    corrSetA.head._1 shouldBe Set(concept)

    val corrSetB = bufferSet.jaWordCorrelations(jaWordB)
    corrSetB.size shouldBe 1
    corrSetB.head._1 shouldBe Set(concept)

    if (corrSetA.head._2 == Vector(daijiCorrelationIndex1, daijiCorrelationIndex2))
      corrSetB.head._2 shouldBe Vector(taisetsuCorrelationIndex1, taisetsuCorrelationIndex2)
    else {
      corrSetA.head._2 shouldBe Vector(taisetsuCorrelationIndex1, taisetsuCorrelationIndex2)
      corrSetB.head._2 shouldBe Vector(daijiCorrelationIndex1, daijiCorrelationIndex2)
    }
  }

  it should "reuse Spanish words already included" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1 = "大事"
    val kanaArray1 = "だいじ"
    val kanjiArray2 = "大切"
    val kanaArray2 = "たいせつ"
    val esArray = "importante"

    val kanji11 = "大"
    val kana11 = "だい"
    val kanji12 = "事"
    val kana12 = "じ"

    val kanji21 = "大"
    val kana21 = "たい"
    val kanji22 = "切"
    val kana22 = "せつ"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanji11, kana11),
        OldPronunciation(kanji12, kana12)
      ),
      2 -> IndexedSeq(
        OldPronunciation(kanji21, kana21),
        OldPronunciation(kanji22, kana22)
      )
    )

    val oldWords = Iterable(
      OldWord(1, kanjiArray1, kanaArray1, esArray),
      OldWord(2, kanjiArray2, kanaArray2, esArray)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanaArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray2 should be < 0

    val esAccIndex = findUniqueSpanishAcceptationIndex(esArray)
    val concept = bufferSet.acceptations(esAccIndex).concept

    val jaAccIndex1 = findUniqueJapaneseAcceptationIndex(kanji11 -> kana11, kanji12 -> kana12)
    val jaAccIndex2 = findUniqueJapaneseAcceptationIndex(kanji21 -> kana21, kanji22 -> kana22)
    bufferSet.acceptations(jaAccIndex1).concept shouldBe concept
    bufferSet.acceptations(jaAccIndex2).concept shouldBe concept
  }

  it should "include 2 accRepresentations when only matching its pronunciation (old)" in {
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

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = bufferSet.symbolArrays findIndexOf esArray2

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0

    val concept1 = bufferSet.acceptations.find(_.word == esWord1).get.concept
    val concept2 = bufferSet.acceptations.find(_.word == esWord2).get.concept
    concept1 should not be concept2

    val jaConcepts = Set(concept1, concept2)
    val esWords = Set(esWord1, esWord2)
    val jaAcceptations = bufferSet.acceptations.filter(acc => jaConcepts(acc.concept) && !esWords(acc.word))
    jaAcceptations.length shouldBe 2

    val jaWord = jaAcceptations.head.word
    jaAcceptations(1).word shouldBe jaWord

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
    if (corrSeq.head._1 == Set(concept1)) {
      corrSeq.head._2 shouldBe Vector(correlationA1, correlationA2)
      corrSeq(1)._1 shouldBe Set(concept2)
      corrSeq(1)._2 shouldBe Vector(correlationB1, correlationB2)
    }
    else {
      corrSeq.head._1 shouldBe Set(concept2)
      corrSeq.head._2 shouldBe Vector(correlationB1, correlationB2)
      corrSeq(1)._1 shouldBe Set(concept1)
      corrSeq(1)._2 shouldBe Vector(correlationA1, correlationA2)
    }
  }

  it should "include 2 acceptations for same Japanese word" in {
    implicit val bufferSet = new BufferSet()

    val kanji1A = "早"
    val kanji1B = "速"
    val kana1 = "はや"
    val kana2 = "い"

    val kanjiArray1 = kanji1A + kana2
    val kanjiArray2 = kanji1B + kana2
    val kanaArray = kana1 + kana2
    val esArray1 = "temprano"
    val esArray2 = "rápido"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(
        OldPronunciation(kanji1A, kana1),
        OldPronunciation(kana2, kana2)
      ),
      2 -> IndexedSeq(
        OldPronunciation(kanji1B, kana1),
        OldPronunciation(kana2, kana2)
      )
    )

    val oldWords = Iterable(
      OldWord(1, kanjiArray1, kanaArray, esArray1),
      OldWord(2, kanjiArray2, kanaArray, esArray2)
    )
    Main.convertWords(oldWords, oldWordPronunciations)

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0

    val jaAccIndex1 = findUniqueJapaneseAcceptationIndex(kanji1A -> kana1, kana2 -> kana2)
    val jaAccIndex2 = findUniqueJapaneseAcceptationIndex(kanji1B -> kana1, kana2 -> kana2)
    val esAccIndex1 = findUniqueSpanishAcceptationIndex(esArray1)
    val esAccIndex2 = findUniqueSpanishAcceptationIndex(esArray2)

    val jaWord = bufferSet.acceptations(jaAccIndex1).word
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord

    val concept1 = bufferSet.acceptations(jaAccIndex1).concept
    val concept2 = bufferSet.acceptations(jaAccIndex2).concept
    concept1 should not be concept2

    bufferSet.acceptations(esAccIndex1).concept shouldBe concept1
    bufferSet.acceptations(esAccIndex2).concept shouldBe concept2
  }

  it should "include 3 accRepresentations when 3 words match its pronunciation (old)" in {
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

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndex2 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex2 && repr.alphabet == Main.esAlphabet)
    val esReprIndex3 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex3 && repr.alphabet == Main.esAlphabet)

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esWord2 = bufferSet.wordRepresentations(esReprIndex2).word
    esWord2 should be >= 0
    esWord2 should not be esWord1

    val esWord3 = bufferSet.wordRepresentations(esReprIndex3).word
    esWord3 should be >= 0
    esWord3 should not be esWord1
    esWord3 should not be esWord2

    val concept1 = bufferSet.acceptations.find(_.word == esWord1).get.concept
    val concept2 = bufferSet.acceptations.find(_.word == esWord2).get.concept
    val concept3 = bufferSet.acceptations.find(_.word == esWord3).get.concept
    concept1 should not be concept2
    concept1 should not be concept3
    concept2 should not be concept3

    val esWords = Set(esWord1, esWord2, esWord3)
    val concepts = Set(concept1, concept2, concept3)
    val jaAcceptations = bufferSet.acceptations.filter(acc => !esWords(acc.word) && concepts(acc.concept)).toSet
    jaAcceptations.size shouldBe 3

    val jaWord = jaAcceptations.head.word
    for (acc <- jaAcceptations) acc.word shouldBe jaWord

    jaAcceptations.map(_.concept) shouldBe concepts

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

  it should "include 3 acceptations for the same Japanese word" in {
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

    val jaAccIndex1 = findUniqueJapaneseAcceptationIndex(kanjiArray1 -> kanaArray)
    val jaAccIndex2 = findUniqueJapaneseAcceptationIndex(kanjiArray2 -> kanaArray)
    val jaAccIndex3 = findUniqueJapaneseAcceptationIndex(kanjiArray3 -> kanaArray)
    val esAccIndex1 = findUniqueSpanishAcceptationIndex(esArray1)
    val esAccIndex2 = findUniqueSpanishAcceptationIndex(esArray2)
    val esAccIndex3 = findUniqueSpanishAcceptationIndex(esArray3)

    val jaWord = bufferSet.acceptations(jaAccIndex1).word
    val concept1 = bufferSet.acceptations(jaAccIndex1).concept

    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord
    val concept2 = bufferSet.acceptations(jaAccIndex2).concept
    concept1 should not be concept2

    bufferSet.acceptations(jaAccIndex3).word shouldBe jaWord
    val concept3 = bufferSet.acceptations(jaAccIndex3).concept
    concept1 should not be concept3
    concept2 should not be concept3

    val esWord1 = bufferSet.acceptations(esAccIndex1).word
    esWord1 should not be jaWord
    bufferSet.acceptations(esAccIndex1).concept shouldBe concept1

    val esWord2 = bufferSet.acceptations(esAccIndex2).word
    esWord2 should not be jaWord
    esWord2 should not be esWord1
    bufferSet.acceptations(esAccIndex2).concept shouldBe concept2

    val esWord3 = bufferSet.acceptations(esAccIndex3).word
    esWord3 should not be jaWord
    esWord3 should not be esWord1
    esWord3 should not be esWord2
    bufferSet.acceptations(esAccIndex3).concept shouldBe concept3
  }

  private def checkSameJapaneseWordWithMoreThan2ConceptsOld(shifter: Iterable[OldWord] => Iterable[OldWord]) = {
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

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0
    val esIndex1 = bufferSet.symbolArrays findUniqueIndexOf esArray1
    val esIndexes2 = esArrays2.map(_.map(bufferSet.symbolArrays.findUniqueIndexOf))

    val kanjiIndex1a = bufferSet.symbolArrays findUniqueIndexOf kanjiArray1a
    val kanjiIndex2a = bufferSet.symbolArrays findUniqueIndexOf kanjiArray2a
    val kanaIndexA = bufferSet.symbolArrays findUniqueIndexOf kanaArrayA
    val kanaIndexB = bufferSet.symbolArrays findUniqueIndexOf kanaArrayB

    val esReprIndex1 = bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == esIndex1 && repr.alphabet == Main.esAlphabet)
    val esReprIndexes2 = esIndexes2.map(_.map(index => bufferSet.wordRepresentations findIndexWhere(repr => repr.symbolArray == index && repr.alphabet == Main.esAlphabet)))

    val esWord1 = bufferSet.wordRepresentations(esReprIndex1).word
    esWord1 should be >= 0

    val esAccIndex1 = bufferSet.acceptations findUniqueIndexWhere(_.word == esWord1)
    val concept1 = bufferSet.acceptations(esAccIndex1).concept

    val esWords2 = esReprIndexes2.map(_.map(index => bufferSet.wordRepresentations(index).word))
    for (words <- esWords2; word <- words) {
      word should be >= 0
      word should not be esWord1
    }
    val allWords2Set = esWords2.foldLeft(Set[Int]())((acc,elem) => elem.foldLeft(acc)((acc, elem) => acc + elem))

    val esConcepts2 = esWords2.map(_.map { word =>
      val index = bufferSet.acceptations.findUniqueIndexWhere(_.word == word)
      bufferSet.acceptations(index).concept
    })

    val concepts2 = esConcepts2.map(_.reduce { (a,b) =>
      if (a != b) throw new AssertionError()
      a
    }).toSet

    val jaWords1 = bufferSet.acceptations.collect { case acc if acc.concept == concept1 && acc.word != esWord1 => acc.word }
    jaWords1.size shouldBe 1
    val jaWord = jaWords1.head

    val jaWords2 = bufferSet.acceptations.collect { case acc if concepts2(acc.concept) && !allWords2Set(acc.word) => acc.word }
    jaWords2.size shouldBe 2
    jaWords2.head shouldBe jaWords2(1)
    jaWords2.head shouldBe jaWord

    val correlation1a = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex1a, kanaIndexA)
    val correlation2a = bufferSet.kanjiKanaCorrelations findIndexOf (kanjiIndex2a, kanaIndexA)
    val correlationB = bufferSet.kanjiKanaCorrelations findIndexOf (kanaIndexB, kanaIndexB)

    val corrSeq = bufferSet.jaWordCorrelations(jaWord).toSeq
    corrSeq.size shouldBe 2

    if (corrSeq.head._1 == Set(concept1)) {
      corrSeq.head._2 shouldBe Vector(correlation1a, correlationB)
      corrSeq(1)._1 shouldBe concepts2
      corrSeq(1)._2 shouldBe Vector(correlation2a, correlationB)
    }
    else {
      corrSeq.head._1 shouldBe concepts2
      corrSeq.head._2 shouldBe Vector(correlation2a, correlationB)
      corrSeq(1)._1 shouldBe Set(concept1)
      corrSeq(1)._2 shouldBe Vector(correlation1a, correlationB)
    }
  }

  it should "include same Japanese word with more than 2 concepts (old)" in {
    checkSameJapaneseWordWithMoreThan2ConceptsOld(a => a)
  }

  it should "include same Japanese word with more than 2 concepts (order reversed) (old)" in {
    checkSameJapaneseWordWithMoreThan2ConceptsOld(a => a.foldLeft(List[OldWord]())((list, e) => e :: list))
  }

  private def checkSameJapaneseWordWithMoreThan2Concepts(shifter: Iterable[OldWord] => Iterable[OldWord]) = {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanjiArray1a = "訪"
    val kanjiArray2a = "尋"
    val kanaArrayA = "たず"
    val kanaArrayB = "ねる"

    val kanjiArray1 = kanjiArray1a + kanaArrayB
    val kanjiArray2 = kanjiArray2a + kanaArrayB
    val kanaArray = kanaArrayA + kanaArrayB
    val esArray1 = "visitar"
    val esArrays2 = Array(
      Array("preguntar", "indagar"),
      Array("buscar", "investigar")
    )
    val esArray2 = esArrays2.map(_.mkString(", ")).mkString("; ")

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

    bufferSet.symbolArrays indexOf kanjiArray1 should be < 0
    bufferSet.symbolArrays indexOf kanjiArray2 should be < 0
    bufferSet.symbolArrays indexOf kanaArray should be < 0

    val jaAccIndex1 = findUniqueJapaneseAcceptationIndex(kanjiArray1a -> kanaArrayA, kanaArrayB -> kanaArrayB)
    val jaCorrIndex2 = findUniqueJapaneseCorrelationArrayIndexOf(kanjiArray2a -> kanaArrayA, kanaArrayB -> kanaArrayB)

    val jaAccIndexes2 = bufferSet.acceptationCorrelations.collect {
      case (accId, corrArraySet) if corrArraySet.contains(jaCorrIndex2) => accId
    }.toSet
    jaAccIndexes2.size shouldBe 2
    val jaAccIndex2a = jaAccIndexes2.head
    val jaAccIndex2b = (jaAccIndexes2 - jaAccIndex2a).head

    val esAccIndex1 = findUniqueSpanishAcceptationIndex(esArray1)
    val esAccIndex2a1 = findUniqueSpanishAcceptationIndex(esArrays2(0)(0))
    val esAccIndex2a2 = findUniqueSpanishAcceptationIndex(esArrays2(0)(1))
    val esAccIndex2b1 = findUniqueSpanishAcceptationIndex(esArrays2(1)(0))
    val esAccIndex2b2 = findUniqueSpanishAcceptationIndex(esArrays2(1)(1))

    val concept1 = bufferSet.acceptations(jaAccIndex1).concept
    bufferSet.acceptations(esAccIndex1).concept shouldBe concept1

    val concept2a = bufferSet.acceptations(esAccIndex2a1).concept
    bufferSet.acceptations(esAccIndex2a2).concept shouldBe concept2a

    val concept2b = bufferSet.acceptations(esAccIndex2b1).concept
    bufferSet.acceptations(esAccIndex2b2).concept shouldBe concept2b

    concept1 should not be concept2a
    concept1 should not be concept2b
    concept2a should not be concept2b

    if (bufferSet.acceptations(jaAccIndex2a).concept == concept2a) {
      bufferSet.acceptations(jaAccIndex2b).concept shouldBe concept2b
    }
    else {
      bufferSet.acceptations(jaAccIndex2b).concept shouldBe concept2a
      bufferSet.acceptations(jaAccIndex2a).concept shouldBe concept2b
    }
  }

  it should "include same Japanese word with more than 2 concepts" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a)
  }

  it should "include same Japanese word with more than 2 concepts (order reversed)" in {
    checkSameJapaneseWordWithMoreThan2Concepts(a => a.foldLeft(List[OldWord]())((list, e) => e :: list))
  }

  it should "not include the kanji if it matches the kana (old)" in {
    implicit val bufferSet = Main.initialiseDatabase()

    val kanaArray = "だから"
    val esArray = "por esa razón"

    val oldWordPronunciations = Map(
      1 -> IndexedSeq(OldPronunciation(kanaArray, kanaArray))
    )

    val oldWords = Iterable(OldWord(1, kanaArray, kanaArray, esArray))
    Main.convertWords(oldWords, oldWordPronunciations)

    val kanaIndex = bufferSet.symbolArrays findUniqueIndexOf kanaArray
    val esIndex = bufferSet.symbolArrays findIndexOf esArray

    val kanaReprIndex = bufferSet.wordRepresentations findUniqueIndexWhere(repr => repr.symbolArray == kanaIndex && repr.alphabet == Main.kanaAlphabet)
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

  it should "include a word with kanji representation even if the word is included without kanji for other concept (old)" in {
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

    val kanaIndex = bufferSet.symbolArrays findUniqueIndexOf kanaArray
    val kanjiIndex2 = bufferSet.symbolArrays findIndexOf kanjiArray2
    val esIndex1 = bufferSet.symbolArrays findIndexOf esArray1
    val esIndex2 = esArrays2.map(bufferSet.symbolArrays.findIndexOf)

    val kanaReprIndex = bufferSet.wordRepresentations findUniqueIndexWhere(repr => repr.symbolArray == kanaIndex)
    bufferSet.wordRepresentations(kanaReprIndex).alphabet shouldBe Main.kanaAlphabet

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

    val jaAccIndex1 = findUniqueJapaneseAcceptationIndex(kanaArray -> kanaArray)
    val jaAccIndex2 = findUniqueJapaneseAcceptationIndex(kanjiArray2 -> kanaArray)
    val esAccIndex1 = findUniqueSpanishAcceptationIndex(esArray1)
    val esAccIndex2a = findUniqueSpanishAcceptationIndex(esArrays2(0))
    val esAccIndex2b = findUniqueSpanishAcceptationIndex(esArrays2(1))

    val jaWord = bufferSet.acceptations(jaAccIndex1).word
    bufferSet.acceptations(jaAccIndex2).word shouldBe jaWord

    val concept1 = bufferSet.acceptations(jaAccIndex1).concept
    val concept2 = bufferSet.acceptations(jaAccIndex2).concept
    concept1 should not be concept2

    val esAcc1 = bufferSet.acceptations(esAccIndex1)
    esAcc1.word should not be jaWord
    esAcc1.concept shouldBe concept1

    val esAcc2a = bufferSet.acceptations(esAccIndex2a)
    esAcc2a.word should not be jaWord
    esAcc2a.word should not be esAcc1.word
    esAcc2a.concept shouldBe concept2

    val esAcc2b = bufferSet.acceptations(esAccIndex2b)
    esAcc2b.word should not be jaWord
    esAcc2b.word should not be esAcc1.word
    esAcc2b.word should not be esAcc2a.word
    esAcc2b.concept shouldBe concept2
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

  it should "include all list names as Spanish words" in {
    implicit val bufferSet = new BufferSet()

    val listId = 1
    val listName = "animal"
    val oldLists = Map[Int, String](listId -> listName)
    val listChildRegisters = Vector()
    val grammarConstraints = Map[Int, String]()
    val grammarRules = Map[Int, GrammarRuleRegister]()
    val oldWordAccMap = Map[Int, Set[Int]]()

    Main.convertBunches(oldLists, listChildRegisters, grammarConstraints, grammarRules, oldWordAccMap)
    findUniqueSpanishAcceptationIndex(listName)
  }

  it should "reuse concepts for Spanish words matching list names" in {
    implicit val bufferSet = new BufferSet()

    val wordId = 4
    val listId = 1
    val kanji = "動物"
    val kana = "どうぶつ"
    val listName = "animal"
    val oldLists = Map[Int, String](listId -> listName)
    val listChildRegisters = Vector()
    val grammarConstraints = Map[Int, String]()
    val grammarRules = Map[Int, GrammarRuleRegister]()
    val oldWordAccMap = Map[Int, Set[Int]]()

    val oldWords = Vector(
      OldWord(wordId, kanji, kana, listName)
    )

    val oldWordPronunciations = Map(
      wordId -> IndexedSeq(OldPronunciation(kanji, kana))
    )

    Main.convertWords(oldWords, oldWordPronunciations)
    Main.convertBunches(oldLists, listChildRegisters, grammarConstraints, grammarRules, oldWordAccMap)
    findUniqueSpanishAcceptationIndex(listName)
  }

  it should "include all GrammarRule names as Spanish words" in {
    implicit val bufferSet = new BufferSet()

    val listId = 1
    val constraintId = 14
    val ruleId = 15
    val listName = "adjetivo"
    val ruleName = "pasado"
    val oldLists = Map[Int, String](listId -> listName)

    val listChildRegisters = Vector(
      ListChildRegister(listId, constraintId, Main.listChildTypes.constraint),
      ListChildRegister(listId, ruleId, Main.listChildTypes.rule)
    )

    val grammarConstraints = Map(
      constraintId -> "い"
    )

    val grammarRules = Map(
      ruleId -> GrammarRuleRegister(ruleName, 0, "かった")
    )

    val oldWordAccMap = Map[Int, Set[Int]]()

    Main.convertBunches(oldLists, listChildRegisters, grammarConstraints, grammarRules, oldWordAccMap)
    findUniqueSpanishAcceptationIndex(ruleName)
  }

  it should "include agents including all list children" in {
    val substantiveListId = 54
    val verbListId = 55
    val transitiveListId = 56
    val intransitiveListId = 57

    val oldLists = Map(
      substantiveListId -> "substantive",
      verbListId -> "verb",
      transitiveListId -> "transitive",
      intransitiveListId -> "intransitive"
    )

    val listChildRegisters = Vector(
      ListChildRegister(verbListId, transitiveListId, Main.listChildTypes.list),
      ListChildRegister(verbListId, intransitiveListId, Main.listChildTypes.list)
    )

    val oldWordAccMap = Map[Int, Set[Int]]()
    val bufferSet = new BufferSet()

    Main.convertBunches(oldLists, listChildRegisters, Map(), Map(), oldWordAccMap)(bufferSet)

    val verbSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf oldLists(verbListId)
    val transitiveSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf oldLists(transitiveListId)
    val intransitiveSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf oldLists(intransitiveListId)

    val verbReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.symbolArray == verbSymbolArrayIndex && repr.alphabet == Main.esAlphabet)
    val transitiveReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.symbolArray == transitiveSymbolArrayIndex && repr.alphabet == Main.esAlphabet)
    val intransitiveReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.symbolArray == intransitiveSymbolArrayIndex && repr.alphabet == Main.esAlphabet)

    val verbWord = bufferSet.wordRepresentations(verbReprIndex).word
    val transitiveWord = bufferSet.wordRepresentations(transitiveReprIndex).word
    val intransitiveWord = bufferSet.wordRepresentations(intransitiveReprIndex).word

    val verbAccIndex = bufferSet.acceptations.findUniqueIndexWhere(_.word == verbWord)
    val transitiveAccIndex = bufferSet.acceptations.findUniqueIndexWhere(_.word == transitiveWord)
    val intransitiveAccIndex = bufferSet.acceptations.findUniqueIndexWhere(_.word == intransitiveWord)

    val verbConcept = bufferSet.acceptations(verbAccIndex).concept
    val transitiveConcept = bufferSet.acceptations(transitiveAccIndex).concept
    val intransitiveConcept = bufferSet.acceptations(intransitiveAccIndex).concept

    val sourceBunches = Set(transitiveConcept, intransitiveConcept)
    val nullCorrelation = bufferSet.addCorrelation(Map())
    bufferSet.agents.toSet shouldBe Set(Agent(verbConcept, sourceBunches, nullCorrelation, nullCorrelation, StreamedDatabaseConstants.nullBunchId, fromStart = false))
  }

  it should "include agents for lists with suffix grammar rules" in {
    val substantiveListId = 134
    val adjListId = 135
    val iAdjListId = 13
    val naAdjListId = 1101

    val iAdjListName = "adjective with i"

    val oldLists = Map(
      substantiveListId -> "substantive",
      adjListId -> "adjective",
      iAdjListId -> iAdjListName,
      naAdjListId -> "adjective with na"
    )

    val iConstraintId = 24
    val naConstraintId = 25
    val iStr = "い"
    val naStr = "な"
    val grammarConstraints = Map(
      iConstraintId -> iStr,
      naConstraintId -> naStr
    )

    val ruleName = "pasado"
    val pastRuleId = 8
    val kattaStr = "かった"
    val grammarRules = Map(
      pastRuleId -> GrammarRuleRegister(ruleName, 0, kattaStr)
    )

    val listChildRegisters = Vector(
      ListChildRegister(adjListId, iAdjListId, Main.listChildTypes.list),
      ListChildRegister(adjListId, naAdjListId, Main.listChildTypes.list),
      ListChildRegister(iAdjListId, iConstraintId, Main.listChildTypes.constraint),
      ListChildRegister(naAdjListId, naConstraintId, Main.listChildTypes.constraint),
      ListChildRegister(iAdjListId, pastRuleId, Main.listChildTypes.rule)
    )

    val oldWordAccMap = Map[Int, Set[Int]]()
    val bufferSet = new BufferSet()

    Main.convertBunches(oldLists, listChildRegisters, grammarConstraints, grammarRules, oldWordAccMap)(bufferSet)

    val iSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf iStr
    val kattaSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf kattaStr
    bufferSet.symbolArrays indexOf naStr should be < 0

    val ruleSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf ruleName
    val ruleWordReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.alphabet == Main.esAlphabet && repr.symbolArray == ruleSymbolArrayIndex)
    val ruleWord = bufferSet.wordRepresentations(ruleWordReprIndex).word
    val ruleAccIndex = bufferSet.acceptations.findUniqueIndexWhere(acc => acc.word == ruleWord)
    val ruleConcept = bufferSet.acceptations(ruleAccIndex).concept

    val iAdjSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf iAdjListName
    val iAdjReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.symbolArray == iAdjSymbolArrayIndex && repr.alphabet == Main.esAlphabet)
    val iAdjWord = bufferSet.wordRepresentations(iAdjReprIndex).word
    val iAdjAccIndex = bufferSet.acceptations.findUniqueIndexWhere(_.word == iAdjWord)
    val iAdjConcept = bufferSet.acceptations(iAdjAccIndex).concept

    val expectedAgent = Agent(
      StreamedDatabaseConstants.nullBunchId,
      Set(iAdjConcept),
      bufferSet.addCorrelation(Map(
        Main.kanjiAlphabet -> iSymbolArrayIndex,
        Main.kanaAlphabet -> iSymbolArrayIndex
      )),
      bufferSet.addCorrelation(Map(
        Main.kanjiAlphabet -> kattaSymbolArrayIndex,
        Main.kanaAlphabet -> kattaSymbolArrayIndex
      )),
      ruleConcept,
      fromStart = false
    )

    bufferSet.agents.contains(expectedAgent) shouldBe true
  }

  it should "include agents for lists with prefix grammar rules" in {
    val substantiveListId = 134
    val listName = "substantive"

    val oldLists = Map(
      substantiveListId -> listName
    )

    val ruleName = "honorífico"
    val honorificRuleId = 8
    val prefixStr = "お"
    val grammarRules = Map(
      honorificRuleId -> GrammarRuleRegister(ruleName, 1, prefixStr)
    )

    val listChildRegisters = Vector(
      ListChildRegister(substantiveListId, honorificRuleId, Main.listChildTypes.rule),
    )

    val oldWordAccMap = Map[Int, Set[Int]]()
    val bufferSet = new BufferSet()

    Main.convertBunches(oldLists, listChildRegisters, Map(), grammarRules, oldWordAccMap)(bufferSet)

    val prefixSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf prefixStr

    val ruleSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf ruleName
    val ruleWordReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.alphabet == Main.esAlphabet && repr.symbolArray == ruleSymbolArrayIndex)
    val ruleWord = bufferSet.wordRepresentations(ruleWordReprIndex).word
    val ruleAccIndex = bufferSet.acceptations.findUniqueIndexWhere(acc => acc.word == ruleWord)
    val ruleConcept = bufferSet.acceptations(ruleAccIndex).concept

    val substantiveSymbolArrayIndex = bufferSet.symbolArrays findUniqueIndexOf listName
    val substantiveReprIndex = bufferSet.wordRepresentations.findUniqueIndexWhere(repr => repr.symbolArray == substantiveSymbolArrayIndex && repr.alphabet == Main.esAlphabet)
    val substantiveWord = bufferSet.wordRepresentations(substantiveReprIndex).word
    val substantiveAccIndex = bufferSet.acceptations.findUniqueIndexWhere(_.word == substantiveWord)
    val substantiveConcept = bufferSet.acceptations(substantiveAccIndex).concept

    val expectedAgent = Agent(
      StreamedDatabaseConstants.nullBunchId,
      Set(substantiveConcept),
      bufferSet.addCorrelation(Map()),
      bufferSet.addCorrelation(Map(
        Main.kanjiAlphabet -> prefixSymbolArrayIndex,
        Main.kanaAlphabet -> prefixSymbolArrayIndex
      )),
      ruleConcept,
      fromStart = true
    )

    bufferSet.agents.contains(expectedAgent) shouldBe true
  }

  it should "recycle concepts" in {
    val hirouWordId = 1
    val mitsukeruWordId = 2
    val mitsukaruWordId = 3

    val oldWords = Vector(
      OldWord(hirouWordId, "拾う", "ひろう", "coger, encontrar, recolectar"),
      OldWord(mitsukeruWordId, "見つける", "みつける", "encontrar, hallar"),
      OldWord(mitsukaruWordId, "見つかる", "みつかる", "encontrar, hallar")
    )

    val oldWordPronunciations = Map(
      hirouWordId -> Vector(OldPronunciation("拾", "ひろ"), OldPronunciation("う", "う")),
      mitsukeruWordId -> Vector(OldPronunciation("見", "み"), OldPronunciation("つける", "つける")),
      mitsukaruWordId -> Vector(OldPronunciation("見", "み"), OldPronunciation("つかる", "つかる"))
    )

    implicit val bufferSet = new BufferSet()
    val wordAcceptationMap: Map[Int, Set[Int]] = Main.convertWords(oldWords, oldWordPronunciations)
    wordAcceptationMap.size shouldBe 3
    wordAcceptationMap(hirouWordId).size shouldBe 1
    val concept1 = bufferSet.acceptations(wordAcceptationMap(hirouWordId).head).concept

    wordAcceptationMap(mitsukeruWordId).size shouldBe 1
    val concept2 = bufferSet.acceptations(wordAcceptationMap(mitsukeruWordId).head).concept
    concept1 should not be concept2

    wordAcceptationMap(mitsukaruWordId).size shouldBe 1
    bufferSet.acceptations(wordAcceptationMap(mitsukaruWordId).head).concept shouldBe concept2
  }

  it should "not mix concepts when at least one Spanish word is common" in {
    val kippuWordId = 1
    val iriguchiWordId = 2
    val oldWords = Vector(
      OldWord(kippuWordId, "切符", "きっぷ", "billete, ticket, entrada"),
      OldWord(iriguchiWordId, "入口", "いりぐち", "entrada")
    )

    val oldWordPronunciations = Map(
      kippuWordId -> Vector(OldPronunciation("切", "きっ"), OldPronunciation("符", "ぷ")),
      iriguchiWordId -> Vector(OldPronunciation("入", "いり"), OldPronunciation("口", "ぐち"))
    )

    implicit val bufferSet = new BufferSet()
    val wordAcceptationMap: Map[Int, Set[Int]] = Main.convertWords(oldWords, oldWordPronunciations)
    wordAcceptationMap.size shouldBe 2
    wordAcceptationMap(kippuWordId).size shouldBe 1
    wordAcceptationMap(iriguchiWordId).size shouldBe 1

    val kippuConcept = bufferSet.acceptations(wordAcceptationMap(kippuWordId).head).concept
    val iriguchiConcept = bufferSet.acceptations(wordAcceptationMap(iriguchiWordId).head).concept
    kippuConcept should not be iriguchiConcept
  }
}
