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

    def appendJapaneseWord(concepts: Set[Int], correlation: Vector[(String, String)]): Int = {
      val vector = correlation.map {
        case (kanjiStr, kanaStr) =>
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

      val wordIndex = wordCount
      bufferSet.jaWordCorrelations(wordIndex) = Set((concepts, vector))

      wordCount += 1
      jaWords += wordIndex
      wordIndex
    }

    def registerWordWithConcept(concept: Int, en: String, es: String, correlation: Vector[(String, String)]): Unit = {
      if (en != null) {
        val word = appendEnglishWord(en)
        bufferSet.acceptations += Acceptation(word, concept)
      }

      if (es != null) {
        val word = appendSpanishWord(es)
        bufferSet.acceptations += Acceptation(word, concept)
      }

      if (correlation != null) {
        val word = appendJapaneseWord(Set(concept), correlation)
        bufferSet.acceptations += Acceptation(word, concept)
      }
    }

    def registerWord(en: String, es: String, correlation: Vector[(String, String)]): Int = {
      val concept = conceptCount
      conceptCount += 1

      registerWordWithConcept(concept, en, es, correlation)
      concept
    }

    registerWord("Language", "idioma", Vector("言" -> "げん", "語" -> "ご"))
    registerWordWithConcept(enLanguage, "English", "inglés", Vector("英" -> "えい", "語" -> "ご"))
    registerWordWithConcept(esLanguage, "Spanish", "español", Vector("スペイン" -> "スペイン", "語" -> "ご"))
    registerWordWithConcept(jaLanguage, "Japanese", "japonés", Vector("日本" -> "にほん", "語" -> "ご"))
    registerWord("kanji", "kanji", Vector("漢" -> "かん", "字" -> "じ"))
    registerWord("kana", "kana", Vector("仮" -> "か", "名" -> "な"))

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
  def convertWords(oldWords: Iterable[OldWord], oldWordPronunciations: Map[Int /* old word id */, IndexedSeq[OldPronunciation]])(implicit bufferSet: BufferSet): Map[Int /* old word id */, Set[Int /* New Acceptation index */]] = {
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

    oldWordAccMap.toMap
  }

  def convertBunches(oldLists: Map[Int, String], listChildRegisters: Iterable[ListChildRegister], oldWordAccMap: Map[Int, Set[Int]])(implicit bufferSet: BufferSet): Unit = {

    object listChildTypes {
      val word = 0
      val list = 5
    }

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

    val listsToIgnore = mutable.Set[Int]()

    // Look for the list with title "lists conceptuales". This must be treated in an special way assuming that sublists are generic concepts including more concrete ones.
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
        val lang = languages(languageIndex(repr.word)).code
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

  def main(args: Array[String]): Unit = {
    implicit val bufferSet = initialiseDatabase()

    val filePath = {
      val resource = getClass.getClassLoader.getResource("langbook.db")
      if (resource == null) sys.error("Expected file src/main/resources/langbook.db not present")
      new File(resource.toURI).getPath
    }

    val sqliteDatabaseReader = new SQLiteDatabaseReader(filePath)
    val oldWordAccMap = convertWords(
      sqliteDatabaseReader.readOldWords,
      sqliteDatabaseReader.readOldWordPronunciations
    )
    convertBunches(
      sqliteDatabaseReader.readOldLists,
      sqliteDatabaseReader.readOldListChildren,
      oldWordAccMap
    )

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

    val outStream3 = new PrintWriter(new FileOutputStream("ConceptualBunches.csv"))
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

    val outStream4 = new PrintWriter(new FileOutputStream("AcceptationBunches.csv"))
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
}
