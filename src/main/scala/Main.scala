import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object Main {

  val enAlphabet = StreamedDatabaseConstants.minValidAlphabet
  val esAlphabet = StreamedDatabaseConstants.minValidAlphabet + 1
  val kanjiAlphabet = StreamedDatabaseConstants.minValidAlphabet + 2
  val kanaAlphabet = StreamedDatabaseConstants.minValidAlphabet + 3
  val roumajiAlphabet = StreamedDatabaseConstants.minValidAlphabet + 4

  val alphabets = Vector(
    enAlphabet, // English
    esAlphabet, // Spanish
    kanjiAlphabet, // Japanese kanji
    kanaAlphabet, // Japanese kana
    roumajiAlphabet // Japanese roumaji
  )

  case class Language(concept: Int, code: String, alphabets: Set[Int])

  val minValidLanguage = StreamedDatabaseConstants.minValidAlphabet + alphabets.size

  val enLanguage = minValidLanguage
  val esLanguage = minValidLanguage + 1
  val jaLanguage = minValidLanguage + 2

  val languages = Vector(
    Language(enLanguage, "en", Set(enAlphabet)), // English
    Language(esLanguage, "es", Set(esAlphabet)), // Spanish
    Language(jaLanguage, "ja", Set(kanjiAlphabet, kanaAlphabet, roumajiAlphabet)) // Japanese
  )

  val maxValidLanguage = minValidLanguage + languages.size - 1

  val dbWordBase = StreamedDatabaseConstants.minValidWord
  val dbConceptBase = maxValidLanguage + 1

  val hiragana2RoumajiConversionPairs = List(
    "あ" -> "a",
    "い" -> "i",
    "う" -> "u",
    "え" -> "e",
    "お" -> "o",
    "きゃ" -> "kya",
    "きゅ" -> "kyu",
    "きょ" -> "kyo",
    "ぎゃ" -> "gya",
    "ぎゅ" -> "gyu",
    "ぎょ" -> "gyo",
    "か" -> "ka",
    "っか" -> "kka",
    "き" -> "ki",
    "っき" -> "kki",
    "く" -> "ku",
    "っく" -> "kku",
    "け" -> "ke",
    "っけ" -> "kke",
    "こ" -> "ko",
    "っこ" -> "kko",
    "が" -> "ga",
    "ぎ" -> "gi",
    "ぐ" -> "gu",
    "げ" -> "ge",
    "ご" -> "go",
    "しゃ" -> "sha",
    "しゅ" -> "shu",
    "しょ" -> "sho",
    "じゃ" -> "ja",
    "じゅ" -> "ju",
    "じょ" -> "jo",
    "さ" -> "sa",
    "っさ" -> "ssa",
    "し" -> "shi",
    "っし" -> "sshi",
    "す" -> "su",
    "っす" -> "ssu",
    "せ" -> "se",
    "っせ" -> "sse",
    "そ" -> "so",
    "っそ" -> "sso",
    "ざ" -> "za",
    "じ" -> "ji",
    "ず" -> "zu",
    "ぜ" -> "ze",
    "ぞ" -> "zo",
    "ちゃ" -> "cha",
    "っちゃ" -> "tcha",
    "ちゅ" -> "chu",
    "っちゅ" -> "tchu",
    "ちょ" -> "cho",
    "っちょ" -> "tcho",
    "た" -> "ta",
    "った" -> "tta",
    "ち" -> "chi",
    "つ" -> "tsu",
    "て" -> "te",
    "って" -> "tte",
    "と" -> "to",
    "っと" -> "tto",
    "だ" -> "da",
    "ぢ" -> "di",
    "づ" -> "du",
    "で" -> "de",
    "ど" -> "do",
    "にゃ" -> "nya",
    "にゅ" -> "nyu",
    "にょ" -> "nyo",
    "な" -> "na",
    "に" -> "ni",
    "ぬ" -> "nu",
    "ね" -> "ne",
    "の" -> "no",
    "ひゃ" -> "hya",
    "ひゅ" -> "hyu",
    "ひょ" -> "hyo",
    "びゃ" -> "bya",
    "びゅ" -> "byu",
    "びょ" -> "byo",
    "ぴゃ" -> "pya",
    "ぴゅ" -> "pyu",
    "ぴょ" -> "pyo",
    "は" -> "ha",
    "ひ" -> "hi",
    "ふ" -> "fu",
    "へ" -> "he",
    "ほ" -> "ho",
    "ば" -> "ba",
    "び" -> "bi",
    "ぶ" -> "bu",
    "べ" -> "be",
    "ぼ" -> "bo",
    "ぱ" -> "pa",
    "っぱ" -> "ppa",
    "ぴ" -> "pi",
    "っぴ" -> "ppi",
    "ぷ" -> "pu",
    "っぷ" -> "ppu",
    "ぺ" -> "pe",
    "っぺ" -> "ppe",
    "ぽ" -> "po",
    "っぽ" -> "ppo",
    "みゃ" -> "mya",
    "みゅ" -> "myu",
    "みょ" -> "myo",
    "ま" -> "ma",
    "み" -> "mi",
    "む" -> "mu",
    "め" -> "me",
    "も" -> "mo",
    "や" -> "ya",
    "ゆ" -> "yu",
    "よ" -> "yo",
    "りゃ" -> "rya",
    "りゅ" -> "ryu",
    "りょ" -> "ryo",
    "ら" -> "ra",
    "り" -> "ri",
    "る" -> "ru",
    "れ" -> "re",
    "ろ" -> "ro",
    "わ" -> "wa",
    "を" -> "wo",
    "ん" -> "n"
  )

  val katakana2RoumajiConversionPairs = List(
    "ア" -> "a",
    "イ" -> "i",
    "ウ" -> "u",
    "エ" -> "e",
    "オ" -> "o",
    "キャ" -> "kya",
    "キュ" -> "kyu",
    "キョ" -> "kyo",
    "ギャ" -> "gya",
    "ギュ" -> "gyu",
    "ギョ" -> "gyo",
    "カ" -> "ka",
    "ッカ" -> "kka",
    "キ" -> "ki",
    "ッキ" -> "kki",
    "ク" -> "ku",
    "ック" -> "kku",
    "ケ" -> "ke",
    "ッケ" -> "kke",
    "コ" -> "ko",
    "ッコ" -> "kko",
    "ガ" -> "ga",
    "ギ" -> "gi",
    "グ" -> "gu",
    "ゲ" -> "ge",
    "ゴ" -> "go",
    "シャ" -> "sha",
    "シュ" -> "shu",
    "ショ" -> "sho",
    "ジャ" -> "ja",
    "ジュ" -> "ju",
    "ジェ" -> "je",
    "ジョ" -> "jo",
    "サ" -> "sa",
    "ッサ" -> "ssa",
    "シ" -> "shi",
    "ッシ" -> "sshi",
    "ス" -> "su",
    "ッス" -> "ssu",
    "セ" -> "se",
    "ッセ" -> "sse",
    "ソ" -> "so",
    "ッソ" -> "sso",
    "ザ" -> "za",
    "ジ" -> "ji",
    "ズ" -> "zu",
    "ゼ" -> "ze",
    "ゾ" -> "zo",
    "チャ" -> "cha",
    "ッチャ" -> "tcha",
    "チュ" -> "chu",
    "ッチュ" -> "tchu",
    "チェ" -> "che",
    "ッチェ" -> "tche",
    "チョ" -> "cho",
    "ッチョ" -> "tcho",
    "ティ" -> "ti",
    "タ" -> "ta",
    "ッタ" -> "tta",
    "チ" -> "chi",
    "ツ" -> "tsu",
    "テ" -> "te",
    "ッテ" -> "tte",
    "ト" -> "to",
    "ット" -> "tto",
    "ディ" -> "di",
    "ダ" -> "da",
    "ヂ" -> "di",
    "ヅ" -> "du",
    "デ" -> "de",
    "ド" -> "do",
    "ニャ" -> "nya",
    "ニュ" -> "nyu",
    "ニョ" -> "nyo",
    "ナ" -> "na",
    "ニ" -> "ni",
    "ヌ" -> "nu",
    "ネ" -> "ne",
    "ノ" -> "no",
    "ヒャ" -> "hya",
    "ヒュ" -> "hyu",
    "ヒョ" -> "hyo",
    "ビャ" -> "bya",
    "ビュ" -> "byu",
    "ビョ" -> "byo",
    "ピャ" -> "pya",
    "ピュ" -> "pyu",
    "ピョ" -> "pyo",
    "ファ" -> "fa",
    "フィ" -> "fi",
    "フェ" -> "fe",
    "フォ" -> "fo",
    "ハ" -> "ha",
    "ヒ" -> "hi",
    "フ" -> "fu",
    "ヘ" -> "he",
    "ホ" -> "ho",
    "バ" -> "ba",
    "ビ" -> "bi",
    "ブ" -> "bu",
    "ベ" -> "be",
    "ボ" -> "bo",
    "パ" -> "pa",
    "ッパ" -> "ppa",
    "ピ" -> "pi",
    "ッピ" -> "ppi",
    "プ" -> "pu",
    "ップ" -> "ppu",
    "ペ" -> "pe",
    "ッペ" -> "ppe",
    "ポ" -> "po",
    "ッポ" -> "ppo",
    "ミャ" -> "mya",
    "ミュ" -> "myu",
    "ミョ" -> "myo",
    "マ" -> "ma",
    "ミ" -> "mi",
    "ム" -> "mu",
    "メ" -> "me",
    "モ" -> "mo",
    "ヤ" -> "ya",
    "ユ" -> "yu",
    "ヨ" -> "yo",
    "リャ" -> "rya",
    "リュ" -> "ryu",
    "リョ" -> "ryo",
    "ラ" -> "ra",
    "リ" -> "ri",
    "ル" -> "ru",
    "レ" -> "re",
    "ロ" -> "ro",
    "ワ" -> "wa",
    "ヲ" -> "wo",
    "ン" -> "n",
    "ー" -> "-"
  )

  val kana2RoumajiConversionPairs = hiragana2RoumajiConversionPairs ++ katakana2RoumajiConversionPairs

  def initialiseDatabase(): BufferSet = {
    implicit val bufferSet = new BufferSet()
    var wordCount = dbWordBase
    var conceptCount = dbConceptBase

    def appendEnglishAcceptation(symbolArray: String, concept: Int)(implicit bufferSet: BufferSet): Unit = {
      val newWord = wordCount

      bufferSet.wordRepresentations.append(WordRepresentation(newWord, enAlphabet, bufferSet.addSymbolArray(symbolArray)))
      bufferSet.acceptations += Acceptation(newWord, concept)

      val correlationId = bufferSet.addCorrelationArray(Vector(Map(enAlphabet -> symbolArray)))
      val accId = bufferSet.addAcceptation(Acceptation(newWord, concept))
      val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
      bufferSet.acceptationCorrelations(accId) = corrArraySet + correlationId

      wordCount += 1
      bufferSet.enWords += newWord
    }

    /**
      * Add a new Spanish word in the list and returns its id
      */
    def appendSpanishAcceptation(symbolArray: String, concept: Int)(implicit bufferSet: BufferSet): Unit = {
      val newWord = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newWord, esAlphabet, bufferSet.addSymbolArray(symbolArray)))
      bufferSet.acceptations += Acceptation(newWord, concept)

      val correlationId = bufferSet.addCorrelationArray(Vector(Map(esAlphabet -> symbolArray)))
      val accId = bufferSet.addAcceptation(Acceptation(newWord, concept))
      val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
      bufferSet.acceptationCorrelations(accId) = corrArraySet + correlationId

      wordCount += 1
      bufferSet.esWords += newWord
    }

    def appendJapaneseAcceptation(concept: Int, correlation: Vector[(String, String)]): Unit = {
      val kanjiKanaDiffer = correlation.exists { case (str1, str2) => str1 != str2 }

      val wordIndex = wordCount
      if (kanjiKanaDiffer) {
        val vector = correlation.map { case (kanjiStr, kanaStr) =>
          val kanjiIndex = bufferSet.addSymbolArray(kanjiStr)
          val kanaIndex = bufferSet.addSymbolArray(kanaStr)
          val pair = (kanjiIndex, kanaIndex)

          val foundIndex = bufferSet.kanjiKanaCorrelations.indexOf(pair)
          if (foundIndex >= 0) foundIndex
          else {
            val newIndex = bufferSet.kanjiKanaCorrelations.size
            bufferSet.kanjiKanaCorrelations += pair
            newIndex
          }
        }

        bufferSet.jaWordCorrelations(wordIndex) = Set((Set(concept), vector))
      }
      else {
        val str = correlation.map(_._1).mkString("")
        val index = bufferSet.addSymbolArray(str)
        bufferSet.wordRepresentations += WordRepresentation(wordIndex, kanaAlphabet, index)
      }
      bufferSet.acceptations += Acceptation(wordIndex, concept)

      val array = correlation.map { case (kanji, kana) =>
          Map(kanjiAlphabet -> kanji, kanaAlphabet -> kana)
      }
      val accId = bufferSet.addAcceptation(Acceptation(wordIndex, concept))
      val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
      bufferSet.acceptationCorrelations(accId) = corrArraySet + bufferSet.addCorrelationArray(array)

      wordCount += 1
      bufferSet.jaWords += wordIndex
    }

    def registerWordWithConcept(concept: Int, en: String, es: String, correlation: Vector[(String, String)]): Unit = {
      if (en != null) {
        appendEnglishAcceptation(en, concept)
      }

      if (es != null) {
        appendSpanishAcceptation(es, concept)
      }

      if (correlation != null) {
        appendJapaneseAcceptation(concept, correlation)
      }
    }

    def registerWord(en: String, es: String, correlation: Vector[(String, String)]): Int = {
      val concept = conceptCount
      conceptCount += 1

      registerWordWithConcept(concept, en, es, correlation)
      concept
    }

    registerWordWithConcept(StreamedDatabaseConstants.languageConcept, "language", "idioma", Vector("言" -> "げん", "語" -> "ご"))
    registerWordWithConcept(enLanguage, "English", "inglés", Vector("英" -> "えい", "語" -> "ご"))
    registerWordWithConcept(esLanguage, "Spanish", "español", Vector("スペイン" -> "スペイン", "語" -> "ご"))
    registerWordWithConcept(jaLanguage, "Japanese", "japonés", Vector("日本" -> "にほん", "語" -> "ご"))

    registerWordWithConcept(StreamedDatabaseConstants.alphabetConcept, "alphabet", "alfabeto", Vector("アルファベット" -> "アルファベット"))
    registerWordWithConcept(enAlphabet, "English alphabet", "alfabeto inglés", Vector("英" -> "えい", "語" -> "ご", "の" -> "の", "アルファベット" -> "アルファベット"))
    registerWordWithConcept(esAlphabet, "Spanish alphabet", "alfabeto español", Vector("スペイン" -> "スペイン", "語" -> "ご", "の" -> "の", "アルファベット" -> "アルファベット"))
    registerWordWithConcept(kanjiAlphabet, "kanji", "kanji", Vector("漢" -> "かん", "字" -> "じ"))
    registerWordWithConcept(kanaAlphabet, "kana", "kana", Vector("仮" -> "か", "名" -> "な"))
    registerWordWithConcept(roumajiAlphabet, "roumaji", "roumaji", Vector("ローマ" -> "ローマ", "字" -> "じ"))

    // Link concepts
    bufferSet.bunchConcepts(StreamedDatabaseConstants.languageConcept) =
      Set(enLanguage, esLanguage, jaLanguage)

    bufferSet.bunchConcepts(StreamedDatabaseConstants.alphabetConcept) =
      Set(enAlphabet, esAlphabet, kanjiAlphabet, kanaAlphabet, roumajiAlphabet)

    // Add conversions
    val conversionPairs = for ((kanaText, roumajiText) <- kana2RoumajiConversionPairs) yield {
      val kana = bufferSet.addSymbolArray(kanaText)
      val roumaji = bufferSet.addSymbolArray(roumajiText)
      (kana, roumaji)
    }
    bufferSet.conversions += Conversion(kanaAlphabet, roumajiAlphabet, conversionPairs)

    bufferSet
  }

  /**
    * Returns the language index that the given word belongs to.
    * An illegal argument exception is thrown if the word provided
    * is not registered.
    */
  def languageIndex(word: Int)(implicit bufferSet: BufferSet): Int = {
    val result = languages.indexWhere { lang =>
      lang.code match {
        case "en" => bufferSet.enWords(word)
        case "es" => bufferSet.esWords(word)
        case "ja" => bufferSet.jaWords(word)
      }
    }

    if (result < 0) {
      throw new IllegalArgumentException(s"Word with id $word does not belong to any language")
    }

    result
  }

  /**
    * Take meaning field from old database base and parses it for semicolon and commas to get all
    * Spanish words related.
    */
  def parseOldMeaning(spSymbolArray: String): Array[Array[String]] = {
    val semicolonSeparated = {
      if (spSymbolArray.contains(';')) {
        spSymbolArray.split(";").map(_.trim).filter(s => s != null && s.length > 0)
      }
      else {
        Array(spSymbolArray)
      }
    }

    for (part <- semicolonSeparated) yield {
      val (arr, _, _, _) = part.foldLeft((ArrayBuffer[Int](), 0, false, 0)) { case ((arr, level, wrong, i), char) =>
        char match {
          case '(' => (arr, level + 1, wrong, i + 1)
          case ')' => if (level > 0) (arr, level - 1, wrong, i + 1) else (arr, 0, true, i + 1)
          case ',' => if (level > 0) (arr, level, wrong, i + 1) else (arr += i, 0, wrong, i + 1)
          case _ => (arr, level, wrong, i + 1)
        }
      }

      if (arr.isEmpty) {
        Array(part)
      }
      else {
        val startArr = ArrayBuffer(0) ++ arr.map(_ + 1)
        val endArr = arr ++ ArrayBuffer(part.length)
        val r = startArr zip endArr map { case (start, end) =>
          part.substring(start, end)
        }
        r.map(_.trim).toArray
      }
    }
  }

  /**
    * Convert from old database schema to the new database schema
    */
  def convertWords(oldWords: Iterable[OldWord], oldWordPronunciations: Map[Int /* old word id */, IndexedSeq[OldPronunciation]])(implicit bufferSet: BufferSet): Map[Int /* old word id */, Set[Int /* New Acceptation index */]] = {
    var (wordCount, conceptCount) = bufferSet.maxWordAndConceptIndexes
    wordCount += 1
    conceptCount += 1

    val oldNewMap = scala.collection.mutable.Map[Int, Int]()
    val oldWordAccMap = scala.collection.mutable.Map[Int /* old word id */, Set[Int] /* Accs */]()
    val kanaWordMap = scala.collection.mutable.Map[String, Int]()

    for (repr <- bufferSet.wordRepresentations if repr.alphabet == Main.kanaAlphabet) {
      val str = bufferSet.symbolArrays(repr.symbolArray)
      if (kanaWordMap.contains(str)) {
        throw new AssertionError()
      }

      kanaWordMap(str) = repr.word
    }

    for ((wordId, set) <- bufferSet.jaWordCorrelations) {
      val strSet = set.map(_._2.map(index => bufferSet.kanjiKanaCorrelations(index)._2).map(bufferSet.symbolArrays).mkString(""))
      if (strSet.size != 1) {
        throw new AssertionError()
      }

      val str = strSet.head
      if (kanaWordMap.contains(str) && kanaWordMap(str) != wordId) {
        throw new AssertionError()
      }

      kanaWordMap(strSet.head) = wordId
    }

    def appendSpanishWordWithRegisteredSymbolArray(symbolArray: Int): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newIndex, esAlphabet, symbolArray))
      wordCount += 1
      bufferSet.esWords += newIndex
      newIndex
    }

    for (oldWord <- oldWords) {
      // Retrieve strings from old word
      val kanjiSymbolArray = oldWord.kanjiSymbolArray
      val kanaSymbolArray = oldWord.kanaSymbolArray
      val spSymbolArrays = parseOldMeaning(oldWord.spSymbolArray)

      // Register all strings for Spanish in the database if they are still not present and retrieve its ids
      val spSymbolArrayIndexes = spSymbolArrays.map(_.map(bufferSet.addSymbolArray))

      // It is assumed that there cannot be more than one word with the same kana
      val knownJaWord = kanaWordMap.get(kanaSymbolArray)

      val knownConcepts: Array[Int] = for (arrayIndexes <- spSymbolArrayIndexes) yield {
        val wordIndexes: Array[Int] = arrayIndexes.map { index =>
          bufferSet.wordRepresentations.collectFirst {
            case repr if repr.symbolArray == index => repr.word
          }.getOrElse(-1)
        }

        val conceptIndexes: Array[Set[Int]] = wordIndexes.map { index =>
          bufferSet.acceptations.collect {
            case acc if acc.word == index => acc.concept
          }.toSet
        }

        val commonConcepts = conceptIndexes.reduce((a,b) => a.intersect(b))
        val wordIndexesSet = wordIndexes.toSet

        commonConcepts.collectFirst {
          case concept if bufferSet.acceptations.collect {
            case acc if acc.concept == concept && bufferSet.esWords(acc.word) => acc.word
          }.toSet == wordIndexesSet => concept
        }.getOrElse(-1)
      }

      // If another word with the same kana is found we reuse it as they are considered to be the
      // same word, if not we create the representation for it
      val jaWord = knownJaWord.getOrElse {
        val jaWord = wordCount
        wordCount += 1
        bufferSet.jaWords += jaWord

        kanaWordMap(kanaSymbolArray) = jaWord
        jaWord
      }

      if (kanjiSymbolArray == kanaSymbolArray) {
        val kanaSymbolArrayIndex = bufferSet.addSymbolArray(kanaSymbolArray)
        bufferSet.wordRepresentations += WordRepresentation(jaWord, kanaAlphabet, kanaSymbolArrayIndex)
      }

      oldNewMap(oldWord.wordId) = jaWord

      val accArray = ArrayBuffer[Int]()
      val accInsertedJustNow = mutable.Set[Int]()
      val thisAccIndexes: Array[Int] = for (knownConcept <- knownConcepts) yield {
        val concept = {
          if (knownConcept >= 0) {
            knownConcept
          }
          else {
            val r = conceptCount
            conceptCount += 1
            r
          }
        }

        val acc = Acceptation(jaWord, concept)
        val alreadyIncludedAccIndex = bufferSet.acceptations.indexOf(acc)
        val accIndex = if (alreadyIncludedAccIndex >= 0) alreadyIncludedAccIndex else {
          val index = bufferSet.acceptations.length
          bufferSet.acceptations += acc
          accInsertedJustNow += index
          index
        }

        accArray += accIndex
        accIndex
      }
      oldWordAccMap(oldWord.wordId) = accArray.toSet

      for (((meanings, knownConcept), accIndex) <- spSymbolArrayIndexes zip knownConcepts zip thisAccIndexes) {
        if (knownConcept < 0) {
          for (meaning <- meanings) {
            val esWordOpt = bufferSet.wordRepresentations.collectFirst {
              case repr if repr.symbolArray == meaning => repr.word
            }
            val esWord = esWordOpt.getOrElse(appendSpanishWordWithRegisteredSymbolArray(meaning))
            val concept = bufferSet.acceptations(accIndex).concept
            val acc = Acceptation(esWord, concept)
            bufferSet.acceptations += acc
          }
        }
      }
    }

    // Exclude all correlations whose kanji and kana matches
    val filteredOldWordPronunciations = oldWordPronunciations.filter { case (_ ,seq) =>
      seq.exists(pronunciation => pronunciation.kanji != pronunciation.kana)
    }

    // Include correlations
    for ((oldWordId, seq) <- filteredOldWordPronunciations) {
      val newSeq = mutable.ArrayBuffer[Int /* conversion index */]()
      for (pronunciation <- seq) {
        val kanjiArray = bufferSet.addSymbolArray(pronunciation.kanji)
        val kanaArray = bufferSet.addSymbolArray(pronunciation.kana)
        val prevIndex = bufferSet.kanjiKanaCorrelations.indexWhere(c => c._1 == kanjiArray && c._2 == kanaArray)
        val conversionIndex = if (prevIndex >= 0) prevIndex else {
          val newIndex = bufferSet.kanjiKanaCorrelations.length
          bufferSet.kanjiKanaCorrelations += ((kanjiArray, kanaArray))
          newIndex
        }

        newSeq += conversionIndex
      }

      val correlationArray = bufferSet.addCorrelationArray(seq.map(pron => Map(Main.kanjiAlphabet -> pron.kanji, Main.kanaAlphabet -> pron.kana)))
      val wordId = oldNewMap(oldWordId)
      val set = bufferSet.jaWordCorrelations.getOrElse(wordId, Set())

      val conceptSet = for (accIndex <- oldWordAccMap(oldWordId)) yield {
        val acc = bufferSet.acceptations(accIndex)
        if (acc.word != wordId) {
          throw new AssertionError()
        }

        acc.concept
      }

      bufferSet.jaWordCorrelations(oldNewMap(oldWordId)) = set + ((conceptSet, newSeq.toVector))
    }

    for (jaWordCorr <- bufferSet.jaWordCorrelations) {
      val wordId = jaWordCorr._1
      for ((concepts, vector) <- jaWordCorr._2) {
        val arrayRef = bufferSet.addCorrelationArrayForIndex(vector.map { kanjiKanaCorr =>
          val (kanji, kana) = bufferSet.kanjiKanaCorrelations(kanjiKanaCorr)
          Map(Main.kanjiAlphabet -> kanji, Main.kanaAlphabet -> kana)
        })

        for (concept <- concepts) {
          val accId = bufferSet.addAcceptation(Acceptation(wordId, concept))
          val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
          bufferSet.acceptationCorrelations(accId) = corrArraySet + arrayRef
        }
      }
    }

    for (WordRepresentation(word, alphabet, symbolArray) <- bufferSet.wordRepresentations) {
      lazy val correlationArray = {
        if (alphabet == Main.kanaAlphabet) {
          bufferSet.addCorrelationArrayForIndex(Vector(Map(Main.kanaAlphabet -> symbolArray, Main.kanjiAlphabet -> symbolArray)))
        }
        else {
          bufferSet.addCorrelationArrayForIndex(Vector(Map(alphabet -> symbolArray)))
        }
      }
      for (acc <- bufferSet.acceptations if acc.word == word) {
        val conceptsInJaCorrelations = bufferSet.jaWordCorrelations.get(word).map(_.foldLeft(Set[Int]())((set, r) => set ++ r._1)).getOrElse(Set[Int]())
        if (!conceptsInJaCorrelations(acc.concept)) {
          val accId = bufferSet.addAcceptation(Acceptation(word, acc.concept))
          val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
          bufferSet.acceptationCorrelations(accId) = corrArraySet + correlationArray
        }
      }
    }

    oldWordAccMap.toMap
  }

  object listChildTypes {
    val word = 0
    val list = 5
    val constraint = 8
    val rule = 9
  }

  def convertBunches(
      oldLists: Map[Int, String],
      listChildRegisters: Iterable[ListChildRegister],
      grammarConstraints: Map[Int, String],
      grammarRules: Map[Int, GrammarRuleRegister],
      oldWordAccMap: Map[Int, Set[Int]])(implicit bufferSet: BufferSet): Unit = {

    val lists = new ArrayBuffer[(Int /* Concept */, Int /* old list id */, Boolean /* new word */, String /* name */)]
    var (wordCount, conceptCount) = bufferSet.maxWordAndConceptIndexes
    wordCount += 1
    conceptCount += 1

    def appendSpanishAcceptation(symbolArray: String, concept: Int): Unit = {
      val newWord = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newWord, esAlphabet, bufferSet.addSymbolArray(symbolArray)))
      bufferSet.acceptations += Acceptation(newWord, concept)

      val correlationId = bufferSet.addCorrelationArray(Vector(Map(esAlphabet -> symbolArray)))
      val accId = bufferSet.addAcceptation(Acceptation(newWord, concept))
      val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
      bufferSet.acceptationCorrelations(accId) = corrArraySet + correlationId

      wordCount += 1
      bufferSet.esWords += newWord
    }

    for ((listId, name) <- oldLists) {
      val index = bufferSet.addSymbolArray(name)
      val wordOption = bufferSet.wordRepresentations.collectFirst {
        case repr if repr.symbolArray == index && repr.alphabet == esAlphabet => repr.word
      }
      val concepts = wordOption.toArray.flatMap(word => bufferSet.acceptations.collect {
        case acc if acc.word == word => acc.concept
      })

      val uniqueWordConcepts: Array[Int] = {
        if (concepts.length > 1) {
          concepts.filter { concept =>
            bufferSet.acceptations.count {
              acc => acc.concept == concept && bufferSet.wordRepresentations.exists {
                repr => repr.word == acc.word && repr.alphabet == esAlphabet
              }
            } == 1
          }
        }
        else Array()
      }

      if (concepts.length == 1) {
        lists += ((concepts.head, listId, false, name))
      }
      else if (uniqueWordConcepts.length == 1) {
        lists += ((uniqueWordConcepts.head, listId, false, name))
      }
      else {
        val concept = conceptCount
        conceptCount += 1

        if (wordOption.isEmpty) {
          appendSpanishAcceptation(name, concept)
        }

        lists += ((concept, listId, wordOption.isEmpty, name))
      }
    }

    // This is here just for testing purposes and should be removed
    do {
      val newWords = lists.collect { case (_, _, newWord, name) if newWord => "\n  " + name }
      val reusedWords = lists.collect { case (_, _, newWord, name) if !newWord => "\n  " + name }
      println(s"Included new ${newWords.length} words as bunch names out of ${lists.length} bunches")
      println(s"New words: ${newWords.toList}")
      println(s"Reused words: ${reusedWords.toList}")
    } while (false)

    val listsToIgnore = mutable.Set[Int]()

    // Look for the list with title "listas conceptuales". This must be treated in an special way assuming that sublists are generic concepts including more concrete ones.
    // The content of the sublists will be registered by their concept and not by their word or acceptation.
    val rootConceptLists = oldLists.collect { case (listId, name) if name == "listas conceptuales" => listId }
    if (rootConceptLists.size == 1) {
      val rootConceptListId = rootConceptLists.head
      listsToIgnore += rootConceptListId

      val conceptLists = for (listChild <- listChildRegisters if listChild.listId == rootConceptListId && listChild.childType == listChildTypes.list) yield {
        listChild.childId
      }
      listsToIgnore ++= conceptLists

      for {
        listId <- conceptLists
        listChild <- listChildRegisters if listChild.listId == listId && listChild.childType == listChildTypes.word
        (bunch, oldListId, _, _) <- lists if oldListId == listId
        accIndex <- oldWordAccMap(listChild.childId)
      } {
        val concept = bufferSet.acceptations(accIndex).concept
        bufferSet.bunchConcepts(bunch) = bufferSet.bunchConcepts.getOrElse(bunch, Set[Int]()) + concept
      }
    }

    for {
      listChildRegister <- listChildRegisters if listChildRegister.childType == 0 && !listsToIgnore(listChildRegister.listId)
      accIndex <- oldWordAccMap(listChildRegister.childId)
    } {
      val listConcept = lists.collectFirst {
        case (concept, listId, _, _) if listId == listChildRegister.listId => concept
      }.get

      bufferSet.bunchAcceptations(listConcept) = bufferSet.bunchAcceptations.getOrElse(listConcept, Set[Int]()) + accIndex
    }

    val nullCorrelation = bufferSet.addCorrelation(Map())
    for {
      (listId,_) <- oldLists
      (targetBunch, oldTargetListId, _, _) <- lists if oldTargetListId == listId
    } {
      val sources = (for {
        listChildReg <- listChildRegisters if listChildReg.listId == listId && listChildReg.childType == listChildTypes.list
        (bunch, oldListId, _, _) <- lists if oldListId == listChildReg.childId
      } yield {
        bunch
      }).toSet

      if (sources.nonEmpty) {
        bufferSet.agents += Agent(targetBunch, sources, nullCorrelation, nullCorrelation, StreamedDatabaseConstants.nullBunchId, fromStart = false)
      }
    }

    for {
      (ruleId, rule) <- grammarRules
      ListChildRegister(listId, childId, childType) <- listChildRegisters if childId == ruleId && childType == listChildTypes.rule
    } {
      val matchers = listChildRegisters.collect {
        case reg if reg.listId == listId && reg.childType == listChildTypes.constraint => reg.childId
      }

      val bunchId = lists.collectFirst { case list if list._2 == listId => list._1 }.get
      val agentMatcher: Int = {
        if (matchers.size == 1) {
          val constraintSymbolArrayIndex = bufferSet.addSymbolArray(grammarConstraints(matchers.head))
          bufferSet.addCorrelation(Map(
            kanjiAlphabet -> constraintSymbolArrayIndex,
            kanaAlphabet -> constraintSymbolArrayIndex
          ))
        }
        else nullCorrelation
      }

      if (matchers.size < 2) {
        val ruleNameSymbolArrayIndex = bufferSet.addSymbolArray(rule.form)
        val ruleWordIdOpt = bufferSet.wordRepresentations.collectFirst { case repr if repr.symbolArray == ruleNameSymbolArrayIndex && repr.alphabet == esAlphabet => repr.word }
        val ruleWordId = ruleWordIdOpt.getOrElse {
          val newWordId = wordCount
          wordCount += 1
          bufferSet.wordRepresentations += WordRepresentation(newWordId, esAlphabet, ruleNameSymbolArrayIndex)
          bufferSet.esWords += newWordId
          newWordId
        }

        val ruleConcept = bufferSet.acceptations.collectFirst { case acc if acc.word == ruleWordId => acc.concept }.getOrElse {
          val newConcept = conceptCount
          conceptCount += 1
          bufferSet.acceptations += Acceptation(ruleWordId, newConcept)
          val corrArray = bufferSet.addCorrelationArrayForIndex(Vector(Map(esAlphabet -> ruleNameSymbolArrayIndex)))
          val accId = bufferSet.addAcceptation(Acceptation(ruleWordId, newConcept))
          val corrArraySet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
          bufferSet.acceptationCorrelations(accId) = corrArraySet + corrArray
          newConcept
        }

        val ruleSymbolArrayIndex = bufferSet.addSymbolArray(rule.pattern)
        val agentAdder = bufferSet.addCorrelation(Map(
          kanjiAlphabet -> ruleSymbolArrayIndex,
          kanaAlphabet -> ruleSymbolArrayIndex
        ))

        bufferSet.agents += Agent(StreamedDatabaseConstants.nullBunchId, Set(bunchId), agentMatcher, agentAdder, ruleConcept, rule.fromStart)
      }
    }
  }

  case class WordEntry(concepts: Set[Int], word: Int, lang: String, alphabet: Int, str: String) {
    override def toString(): String = {
      val conceptsStr = concepts.toList.sortWith(_ < _).mkString(" ")
      s"$conceptsStr,$word,$lang,$alphabet,$str"
    }
  }

  private def composeWordEntrySet(bufferSet: BufferSet): Set[WordEntry] = {
    val array = mutable.ArrayBuffer[WordEntry]()

    // Dump all words in wordRepresentations
    for (repr <- bufferSet.wordRepresentations) {
      if (repr != InvalidRegister.wordRepresentation) {
        val concepts = bufferSet.acceptations.collect { case p if p.word == repr.word => p.concept }
        val lang = languages(languageIndex(repr.word)(bufferSet)).code
        val str = bufferSet.symbolArrays(repr.symbolArray)
        array += WordEntry(concepts.toSet, repr.word, lang, repr.alphabet, str)
      }
    }

    for ((wordId, set) <- bufferSet.jaWordCorrelations) {
      val accConcepts = bufferSet.acceptations.collect { case acc if acc.word == wordId => acc.concept }.toSet
      val allConcepts = set.foldLeft(accConcepts)((set, elem) => set ++ elem._1)
      val lang = "ja"
      val kanaStr = set.head._2.map(index => bufferSet.kanjiKanaCorrelations(index)._2).map(bufferSet.symbolArrays).mkString("")
      array += WordEntry(allConcepts, wordId, lang, kanaAlphabet, kanaStr)

      for ((concepts, vector) <- set) {
        val kanjiStr = vector.map(index => bufferSet.kanjiKanaCorrelations(index)._1).map(bufferSet.symbolArrays).mkString("")
        array += WordEntry(concepts, wordId, lang, kanjiAlphabet, kanjiStr)
      }
    }
    array.toSet
  }

  object FileNames {
    val resourceDatabase = "langbook.db"

    val basicDatabase = "basic.sdb"
    val exportDatabase = "export.sdb"

    val acceptationBunchesCsv = "AcceptationBunches.csv"
    val conceptualBunchesCsv = "ConceptualBunches.csv"
    val wordsCsv = "Words.csv"

    val wordRegisterCsv = "WordRegister.csv"
    val listRegisterCsv = "ListRegister.csv"
    val listChildRegisterCsv = "ListChildRegister.csv"
    val pronunciationRegisterCsv = "PronunciationRegister.csv"
    val wordPronunciationRegisterCsv = "WordPronunciationRegister.csv"
    val grammarConstraintRegisterCsv = "GrammarConstraintRegister.csv"
    val grammarRuleRegisterCsv = "GrammarRuleRegister.csv"
  }

  private def processDatabase(filePath: String): Unit = {
    implicit val bufferSet = initialiseDatabase()

    val sqliteDatabaseReader = new SQLiteDatabaseReader(filePath)
    val oldWordAccMap = convertWords(
      sqliteDatabaseReader.readOldWords,
      sqliteDatabaseReader.readOldWordPronunciations
    )
    convertBunches(
      sqliteDatabaseReader.readOldLists,
      sqliteDatabaseReader.readOldListChildren,
      sqliteDatabaseReader.readOldGrammarConstraints,
      sqliteDatabaseReader.readOldGrammarRules,
      oldWordAccMap
    )

    StreamedDatabaseWriter.write(bufferSet, FileNames.exportDatabase)

    val outStream2 = new PrintWriter(new FileOutputStream(FileNames.wordsCsv))
    try {
      var i = 0
      for (wordEntry <- composeWordEntrySet(bufferSet).toVector.sortWith { case (a, b) =>
        a.word < b.word || a.word == b.word && {
          a.alphabet < b.alphabet || a.alphabet == b.alphabet && {
            a.concepts.min < b.concepts.min
          }
        }
      }) {
        outStream2.println(s"$i,$wordEntry")
        i += 1
      }
    }
    finally {
      outStream2.close()
    }

    def sortedSpanishWordsFromConcepts(concepts: Set[Int]) = {
      val bunchAcceptations = concepts.flatMap(bunch => bufferSet.acceptations.filter(_.concept == bunch))

      val accStrings = bunchAcceptations.foldLeft(Set[(Acceptation, Vector[String])]()) { (set, acc) =>
        val strs = bufferSet.wordRepresentations.filter(repr => repr.word == acc.word && repr.alphabet == esAlphabet).map(repr => bufferSet.symbolArrays(repr.symbolArray)).sorted.toVector
        if (strs.isEmpty) set
        else set + ((acc, strs))
      }

      accStrings.toVector.sortWith { case ((_, strs1), (_, strs2)) => scala.math.Ordering.String.lt(strs1.head, strs2.head) }
    }

    val outStream3 = new PrintWriter(new FileOutputStream(FileNames.conceptualBunchesCsv))
    try {
      val bunches = bufferSet.bunchConcepts.keySet.toSet

      val titles = sortedSpanishWordsFromConcepts(bunches)

      for ((acc, title) <- titles) {
        outStream3.println(title.mkString(", "))

        val concepts = bufferSet.bunchConcepts.getOrElse(acc.concept, Set[Int]())
        val words = concepts.flatMap(bunch =>
          bufferSet.acceptations.collect { case acc if acc.concept == bunch => acc.word }.toSet
        )

        val wordStrings = words.foldLeft(Set[String]()) { (set, word) =>
          val strOpt = bufferSet.wordRepresentations.collectFirst {
            case repr if repr.word == word && repr.alphabet == esAlphabet =>
              bufferSet.symbolArrays(repr.symbolArray)
          }

          set ++ strOpt
        }

        for (str <- wordStrings.toVector.sorted) {
          outStream3.println(s"  $str")
        }
      }
    }
    finally {
      outStream3.close()
    }

    val outStream4 = new PrintWriter(new FileOutputStream(FileNames.acceptationBunchesCsv))
    try {
      val bunches = bufferSet.bunchAcceptations.keySet.toSet
      val titles = sortedSpanishWordsFromConcepts(bunches)

      for ((bunchAcc, title) <- titles) {
        outStream4.println(title.mkString(", "))

        val acceptations = bufferSet.bunchAcceptations.getOrElse(bunchAcc.concept, Set[Int]())
        val wordStrings = acceptations.foldLeft(Set[String]()) { (words, accIndex) =>
          val acc = bufferSet.acceptations(accIndex)
          val kanjiSetOpt = bufferSet.jaWordCorrelations.get(acc.word).flatMap { set =>
            val kanjiStrs = set.collect { case (conceptSet, vector) if conceptSet(acc.concept) =>
              vector.map(index => bufferSet.symbolArrays(bufferSet.kanjiKanaCorrelations(index)._1)).mkString("")
            }

            if (kanjiStrs.isEmpty) None
            else Some(kanjiStrs)
          }

          val strSet = kanjiSetOpt.getOrElse {
            bufferSet.wordRepresentations.collect { case repr if repr.word == acc.word => bufferSet.symbolArrays(repr.symbolArray)}.toSet
          }

          words ++ strSet
        }

        for (str <- wordStrings.toVector.sorted) {
          outStream4.println(s"  $str")
        }
      }
    }
    finally {
      outStream4.close()
    }
  }

  def main(args: Array[String]): Unit = {
    StreamedDatabaseWriter.write(initialiseDatabase(), FileNames.basicDatabase)

    val resource = getClass.getClassLoader.getResource(FileNames.resourceDatabase)
    if (resource != null) {
      processDatabase(new File(resource.toURI).getPath)
    }
  }
}
