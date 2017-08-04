import java.io._

import scala.collection.mutable.ArrayBuffer
import sword.bitstream.OutputBitStream

import scala.collection.mutable

object Main {

  val enAlphabet = StreamedDatabaseConstants.minValidConcept
  val esAlphabet = StreamedDatabaseConstants.minValidConcept + 1
  val kanjiAlphabet = StreamedDatabaseConstants.minValidConcept + 2
  val kanaAlphabet = StreamedDatabaseConstants.minValidConcept + 3
  val roumajiAlphabet = StreamedDatabaseConstants.minValidConcept + 4

  val alphabets = Vector(
    enAlphabet, // English
    esAlphabet, // Spanish
    kanjiAlphabet, // Japanese kanji
    kanaAlphabet, // Japanese kana
    roumajiAlphabet // Japanese roumaji
  )

  case class Language(concept: Int, code: String)

  val minValidLanguage = 0
  val maxValidLanguage = 2

  val languageConceptBase = alphabets.max + 1
  val enLanguage = languageConceptBase
  val esLanguage = languageConceptBase + 1
  val jaLanguage = languageConceptBase + 2

  val languages = Vector(
    Language(enLanguage, "en"), // English
    Language(esLanguage, "es"), // Spanish
    Language(jaLanguage, "ja") // Japanese
  )

  val dbWordBase = StreamedDatabaseConstants.minValidWord
  val dbConceptBase = languageConceptBase + languages.size

  // TODO: This should not be static
  val enWords = scala.collection.mutable.BitSet()
  val esWords = scala.collection.mutable.BitSet()
  val jaWords = scala.collection.mutable.BitSet()

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
    "し" -> "shi",
    "す" -> "su",
    "せ" -> "se",
    "そ" -> "so",
    "ざ" -> "za",
    "じ" -> "ji",
    "ず" -> "zu",
    "ぜ" -> "ze",
    "ぞ" -> "zo",
    "ちゃ" -> "cha",
    "ちゅ" -> "chu",
    "ちょ" -> "cho",
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

  def initialiseDatabase(): BufferSet = {
    implicit val bufferSet = new BufferSet()
    var wordCount = dbWordBase
    var conceptCount = dbConceptBase

    def appendEnglishWord(symbolArray: String)(implicit bufferSet: BufferSet): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newIndex, enAlphabet, bufferSet.addSymbolArray(symbolArray)))
      wordCount += 1
      enWords += newIndex
      newIndex
    }

    /**
      * Add a new Spanish word in the list and returns its id
      */
    def appendSpanishWord(symbolArray: String)(implicit bufferSet: BufferSet): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newIndex, esAlphabet, bufferSet.addSymbolArray(symbolArray)))
      wordCount += 1
      esWords += newIndex
      newIndex
    }

    def appendJapaneseWord(kanjiSymbolArray: String, kanaSymbolArray:String): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(wordCount, kanjiAlphabet, bufferSet.addSymbolArray(kanjiSymbolArray)))
      bufferSet.wordRepresentations.append(WordRepresentation(wordCount, kanaAlphabet, bufferSet.addSymbolArray(kanaSymbolArray)))

      wordCount += 1
      jaWords += newIndex
      newIndex
    }

    def registerWordWithConcept(concept: Int, en: String, es: String, kanji: String, kana: String): Unit = {
      if (en != null) {
        val word = appendEnglishWord(en)
        bufferSet.acceptations += Acceptation(word, concept)
      }

      if (es != null) {
        val word = appendSpanishWord(es)
        bufferSet.acceptations += Acceptation(word, concept)
      }

      if (kana != null) {
        val word = appendJapaneseWord(kanji, kana)
        bufferSet.acceptations += Acceptation(word, concept)
      }
    }

    def registerWord(en: String, es: String, kanji: String, kana: String): Int = {
      val concept = conceptCount
      conceptCount += 1

      registerWordWithConcept(concept, en, es, kanji, kana)
      concept
    }

    registerWord("Language", "idioma", "言語", "げんご")
    registerWordWithConcept(enLanguage, "English", "inglés", "英語", "えいご")
    registerWordWithConcept(esLanguage, "Spanish", "español", "スペイン語", "スペインご")
    registerWordWithConcept(jaLanguage, "Japanese", "japonés", "日本語", "にほんご")
    registerWord("kanji", "kanji", "漢字", "かんじ")
    registerWord("kana", "kana", "平仮名", "かな")

    // Add conversions
    val conversionPairs = for ((kanaText, roumajiText) <- hiragana2RoumajiConversionPairs) yield {
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
  def languageIndex(word: Int): Int = {
    val result = languages.indexWhere { lang =>
      lang.code match {
        case "en" => enWords(word)
        case "es" => esWords(word)
        case "ja" => jaWords(word)
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
  def convertWords(oldWords: Iterable[OldWord], oldWordPronunciations: Map[Int /* old word id */, IndexedSeq[OldPronunciation]])(implicit bufferSet: BufferSet): Map[Int /* old word id */, Int /* New word id */] = {
    var (wordCount, conceptCount) = bufferSet.maxWordAndConceptIndexes
    wordCount += 1
    conceptCount += 1

    def appendSpanishWord(symbolArray: String): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newIndex, esAlphabet, bufferSet.addSymbolArray(symbolArray)))
      wordCount += 1
      esWords += newIndex
      newIndex
    }

    val oldNewMap = scala.collection.mutable.Map[Int, Int]()
    val oldWordAccMap = scala.collection.mutable.Map[Int /* old word id */, Set[Int] /* Accs */]()

    for (oldWord <- oldWords) {
      // Retrieve strings from old word
      val kanjiSymbolArray = oldWord.kanjiSymbolArray
      val kanaSymbolArray = oldWord.kanaSymbolArray
      val spSymbolArrays = parseOldMeaning(oldWord.spSymbolArray)

      // Register all strings in the database if still not present and retrieve its ids
      val kanjiSymbolArrayIndex = bufferSet.addSymbolArray(kanjiSymbolArray)
      val kanaSymbolArrayIndex = bufferSet.addSymbolArray(kanaSymbolArray)
      val spSymbolArrayIndexes = spSymbolArrays.map(_.map(bufferSet.addSymbolArray))

      // It is assumed that there cannot be more than one word with the same kana
      val knownJaWord: Option[Int] = bufferSet.wordRepresentations.collectFirst {
        case repr if repr.symbolArray == kanaSymbolArrayIndex && repr.alphabet == kanaAlphabet =>
          repr.word
      }

      val knownConcepts: Array[Int] = for (arrayIndexes <- spSymbolArrayIndexes) yield {
        val wordIndexes: Array[Int] = arrayIndexes.map { index =>
          bufferSet.wordRepresentations.collectFirst {
            case repr if repr.symbolArray == index => repr.word
          }.getOrElse(-1)
        }

        val conceptIndexes: Array[Int] = wordIndexes.map { index =>
          bufferSet.acceptations.collectFirst {
            case acc if acc.word == index => acc.concept
          }.getOrElse(-1)
        }

        conceptIndexes.reduce((a,b) => if (a == b && a >= 0) a else -1)
        // TODO: Here we should check if the already registered one does not include any extra word, which would mean that are not synonyms.
      }

      // If another word with the same kana is found we reuse it as they are considered to be the
      // same word, if not we create the representation for it
      val jaWord = knownJaWord.getOrElse {
        val jaWord = wordCount
        wordCount += 1
        jaWords += jaWord

        bufferSet.wordRepresentations.append(WordRepresentation(jaWord, kanaAlphabet, kanaSymbolArrayIndex))
        jaWord
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

      if (knownJaWord.isEmpty /* New word */) {
        if (kanjiSymbolArrayIndex != kanaSymbolArrayIndex) {
          bufferSet.wordRepresentations.append(WordRepresentation(jaWord, kanjiAlphabet, kanjiSymbolArrayIndex))
        }
      }
      else {
        // we assume here that the concepts are different between the existing word and the new included word
        // TODO: Check if this assumption is true and implement code to execute when false

        val previousWordReprIndex = bufferSet.wordRepresentations.indexWhere( repr =>
          repr.alphabet == kanjiAlphabet && repr.word == jaWord
        )

        val matchesKanjiWithPrevious = previousWordReprIndex >= 0 &&
          bufferSet.wordRepresentations(previousWordReprIndex).symbolArray == kanjiSymbolArrayIndex

        if (!matchesKanjiWithPrevious) {
          if (previousWordReprIndex >= 0) {
            val otherSymbolArray = bufferSet.wordRepresentations(previousWordReprIndex).symbolArray

            val (_, otherAccs) = bufferSet.acceptations.foldLeft((0, Set[Int]())) { case ((i, set), acc) =>
              if (acc.word == jaWord && !accInsertedJustNow(i)) (i + 1, set + i)
              else (i + 1, set)
            }
            for (otherAcc <- otherAccs) {
              bufferSet.accRepresentations += AccRepresentation(otherAcc, otherSymbolArray)
            }

            bufferSet.wordRepresentations(previousWordReprIndex) = InvalidRegister.wordRepresentation
          }

          for (acc <- thisAccIndexes) {
            val accRepr = AccRepresentation(acc, kanjiSymbolArrayIndex)
            if (!bufferSet.accRepresentations.contains(accRepr)) {
              bufferSet.accRepresentations += accRepr
            }
          }
        }
      }

      for (((meanings, knownConcept), accIndex) <- spSymbolArrays zip knownConcepts zip thisAccIndexes) {
        if (knownConcept < 0) {
          for (meaning <- meanings) {
            val esWord = appendSpanishWord(meaning)
            val concept = bufferSet.acceptations(accIndex).concept
            val acc = Acceptation(esWord, concept)
            bufferSet.acceptations += acc
          }
        }
      }
    }

    // Include correlations
    for ((oldWordId, seq) <- oldWordPronunciations) {
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

    oldNewMap.toMap
  }

  def convertBunches(oldLists: Map[Int, String], listChildRegisters: Iterable[ListChildRegister], oldNewWordIdMap: Map[Int, Int])(implicit bufferSet: BufferSet): Unit = {
    val lists = new ArrayBuffer[(Int /* Concept */, Int /* old list id */, Boolean /* new word */, String /* name */)]
    var (wordCount, conceptCount) = bufferSet.maxWordAndConceptIndexes
    wordCount += 1
    conceptCount += 1

    def appendSpanishWord(symbolArray: String): Int = {
      val newIndex = wordCount
      bufferSet.wordRepresentations.append(WordRepresentation(newIndex, esAlphabet, bufferSet.addSymbolArray(symbolArray)))
      wordCount += 1
      esWords += newIndex
      newIndex
    }

    for ((listId, name) <- oldLists) {
      val index = bufferSet.addSymbolArray(name)
      val wordOption = bufferSet.wordRepresentations.collectFirst {
        case repr if repr.symbolArray == index && repr.alphabet == esAlphabet => repr.word
      }
      val concepts = wordOption.toArray.flatMap(word => bufferSet.acceptations.collect {
        case acc if acc.word == word => acc.concept
      })

      if (concepts.length == 1) {
        lists += ((concepts.head, listId, false, name))
      }
      else {
        val word = wordOption.getOrElse(appendSpanishWord(name))
        val concept = conceptCount
        conceptCount += 1

        bufferSet.acceptations += Acceptation(word, concept)
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

    for (listChildRegister <- listChildRegisters if listChildRegister.childType == 0) {
      val jaWord = oldNewWordIdMap(listChildRegister.childId)
      val listConcept = lists.collectFirst {
        case (concept, listId, _, _) if listId == listChildRegister.listId => concept
      }.get

      bufferSet.bunchWords += BunchWord(listConcept, jaWord)
    }

    // This is here just for testing purposes and should be removed
    do {
      val transitiveVerbSymbolArray = bufferSet.symbolArrays.indexOf("verbo transitivo")
      val transitiveVerbWord = bufferSet.wordRepresentations.collectFirst {
        case repr if repr.symbolArray == transitiveVerbSymbolArray => repr.word
      }.get
      val transitiveVerbConcept = bufferSet.acceptations.collectFirst {
        case acc if acc.word == transitiveVerbWord => acc.concept
      }.get
      val r = bufferSet.bunchWords.collect {
        case bunchWord if bunchWord.bunchConcept == transitiveVerbConcept => bunchWord.word
      }

      val wordsInList = r.map(wordId => bufferSet.wordRepresentations.collectFirst {
        case repr if repr.alphabet == kanaAlphabet && repr.word == wordId => bufferSet.symbolArrays(repr.symbolArray)
      }.get).toList

      println(s"Encontradas ${r.length} palabras en la lista 'verbo transitivo': $wordsInList")
    } while(false)
  }

  def main(args: Array[String]): Unit = {
    implicit val bufferSet = initialiseDatabase()

    val filePath = {
      val resource = getClass.getClassLoader.getResource("langbook.db")
      if (resource == null) sys.error("Expected file src/main/resources/langbook.db not present")
      new File(resource.toURI).getPath
    }

    val sqliteDatabaseReader = new SQLiteDatabaseReader(filePath)
    val oldNewWordIdMap = convertWords(
      sqliteDatabaseReader.readOldWords,
      sqliteDatabaseReader.readOldWordPronunciations
    )
    convertBunches(
      sqliteDatabaseReader.readOldLists,
      sqliteDatabaseReader.readOldListChildren,
      oldNewWordIdMap
    )

    // Added temporal for debugging purposes
    val kakuKanji = "書"
    val kakuKanjiIndex = bufferSet.symbolArrays.indexOf(kakuKanji)
    val kakuKanas = bufferSet.kanjiKanaCorrelations.collect { case conv if conv._1 == kakuKanjiIndex => bufferSet.symbolArrays(conv._2) }
    println(s"For kanji $kakuKanji following pronunciations are found: $kakuKanas")

    val fileName = "export.sdb"
    val obs = new OutputBitStream(new FileOutputStream(fileName))
    try {
      StreamedDatabaseWriter.write(bufferSet, obs)
    }
    catch {
      case _: IOException => System.err.println(s"Unable to write $fileName")
    }

    val outStream2 = new PrintWriter(new FileOutputStream("Words.csv"))
    try {
      case class WordEntry(concepts: Set[Int], word: Int, lang: String, alphabet: Int, str: String) {
        override def toString(): String = {
          val conceptsStr = concepts.toList.sortWith(_ < _).mkString(" ")
          s"$conceptsStr,$word,$lang,$alphabet,$str"
        }
      }

      val array = mutable.ArrayBuffer[WordEntry]()

      // Dump all words in wordRepresentations
      for (repr <- bufferSet.wordRepresentations) {
        if (repr != InvalidRegister.wordRepresentation) {
          val concepts = bufferSet.acceptations.collect { case p if p.word == repr.word => p.concept }
          val lang = languages(languageIndex(repr.word)).code
          val str = bufferSet.symbolArrays(repr.symbolArray)
          array += WordEntry(concepts.toSet, repr.word, lang, repr.alphabet, str)
        }
      }

      // Dump all words in accRepresentations
      val accRepresentationsMap = bufferSet.accRepresentationsMap.toList.sortWith {
        case (((wordId1, _), _), ((wordId2, _), _)) => wordId1 < wordId2
      }

      for (((wordId, str), concepts) <- accRepresentationsMap) {
        val lang = "ja"
        val alphabet = kanjiAlphabet
        array += WordEntry(concepts, wordId, lang, alphabet, str)
      }

      var i = 0
      for (wordEntry <- array.toSet[WordEntry].toVector.sortWith { case (a, b) =>
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
  }
}
