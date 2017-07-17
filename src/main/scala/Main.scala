import java.io._
import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer

object Main {

  def filePath = {
    val resource = getClass.getClassLoader.getResource("langbook.db")
    if (resource == null) sys.error("Expected file src/main/resources/langbook.db not present")
    new File(resource.toURI).getPath
  }

  var conceptCount = 0

  val enAlphabet = conceptCount
  val esAlphabet = conceptCount + 1
  val kanjiAlphabet = conceptCount + 2
  val kanaAlphabet = conceptCount + 3

  val alphabets: Vector[Int] = {
    val i = conceptCount
    val r = Vector(
      i, // English
      i + 1, // Spanish
      i + 2, // Japanese kanji
      i + 3 // Japanese kana
    )

    conceptCount += r.size
    r
  }

  case class Language(concept: Int, code: String)

  val enLanguage = conceptCount
  val esLanguage = conceptCount + 1
  val jaLanguage = conceptCount + 2

  val languages: Vector[Language] = {
    val i = conceptCount
    val r = Vector(
      Language(i, "en"), // English
      Language(i + 1, "es"), // Spanish
      Language(i + 2, "ja") // Japanese
    )

    conceptCount += r.size
    r
  }

  val enWords = scala.collection.mutable.BitSet()
  val esWords = scala.collection.mutable.BitSet()
  val jaWords = scala.collection.mutable.BitSet()
  var wordCount = 0

  /**
    * Add a new English word in the list and returns its id
    */
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

  /**
    * Add a new Japanese word in the list and returns its id
    */
  def appendJapaneseWord(kanjiSymbolArray: String, kanaSymbolArray:String)(implicit bufferSet: BufferSet): Int = {
    val newIndex = wordCount
    bufferSet.wordRepresentations.append(WordRepresentation(wordCount, kanjiAlphabet, bufferSet.addSymbolArray(kanjiSymbolArray)))
    bufferSet.wordRepresentations.append(WordRepresentation(wordCount, kanaAlphabet, bufferSet.addSymbolArray(kanaSymbolArray)))

    wordCount += 1
    jaWords += newIndex
    newIndex
  }

  def registerWord(concept: Int, en: String, es: String, kanji: String, kana: String)(implicit bufferSet: BufferSet): Unit = {
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

  def registerWord(en: String, es: String, kanji: String, kana: String)(implicit bufferSet: BufferSet): Int = {
    val concept = conceptCount
    conceptCount += 1

    registerWord(concept, en, es, kanji, kana)
    concept
  }

  def initialiseDatabase(): BufferSet = {
    implicit val bufferSet = new BufferSet()
    registerWord("Language", "Idioma", "言語", "げんご")
    registerWord(enLanguage, "English", "Inglés", "英語", "えいご")
    registerWord(esLanguage, "Spanish", "Español", "スペイン語", "スペイン")
    registerWord(jaLanguage, "Japanese", "Japonés", "日本語", "にほんご")
    registerWord(null, null, "漢字", "かんじ")
    registerWord(null, null, "平仮名", "かな")

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

  case class OldWord(kanjiSymbolArray: String, kanaSymbolArray: String, spSymbolArray: String)

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
  def convertCollections(oldWords: Iterable[OldWord])(implicit bufferSet: BufferSet) = {
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

        bufferSet.acceptations += Acceptation(jaWord, concept)
        bufferSet.acceptations.length - 1
      }

      if (knownJaWord.isEmpty /* New word */ && kanjiSymbolArrayIndex != kanaSymbolArrayIndex) {
        bufferSet.wordRepresentations.append(WordRepresentation(jaWord, kanjiAlphabet, kanjiSymbolArrayIndex))
      }
      else {
        // we assume here that the concepts are different between the existing word and the new included word
        // TODO: Check if this assumption is true and implement code to execute when false

        val previousWordReprIndex = bufferSet.wordRepresentations.indexWhere( repr =>
          repr.alphabet == kanjiAlphabet && repr.word == jaWord
        )

        val (_, accIndexes) = bufferSet.acceptations.foldLeft((0, Set[Int]())) {
          case ((i, result), acc) =>
            if (acc.word == jaWord) (i + 1, result + i)
            else (i + 1, result)
        }

        val existsPreviousAccRepr = bufferSet.accRepresentations.exists(repr => accIndexes(repr.acc))

        // We are taking actions here assuming that there is always a previous word with kanji
        // representation. It is false if kanji and kana matches
        // TODO: Handle the situation where the previous word has no kanji representation

        if (previousWordReprIndex >= 0) {
          val otherSymbolArray = bufferSet.wordRepresentations(previousWordReprIndex).symbolArray

          val (_, otherAccs) = bufferSet.acceptations.foldLeft((0, Set[Int]())) { case ((i, set), acc) =>
            if (acc.word == jaWord) (i + 1, set + i)
            else (i + 1, set)
          }
          for (otherAcc <- otherAccs) {
            bufferSet.accRepresentations += AccRepresentation(otherAcc, otherSymbolArray)
          }

          for (acc <- thisAccIndexes) {
            bufferSet.accRepresentations += AccRepresentation(acc, kanjiSymbolArrayIndex)
          }

          bufferSet.wordRepresentations(previousWordReprIndex) = InvalidRegister.wordRepresentation
        }
        else if (existsPreviousAccRepr) {
          for (acc <- thisAccIndexes) {
            bufferSet.accRepresentations += AccRepresentation(acc, kanjiSymbolArrayIndex)
          }
        }
      }

      for (((meanings, knownConcept), accIndex) <- spSymbolArrays zip knownConcepts zip thisAccIndexes) {
        if (knownConcept < 0) {
          for (meaning <- meanings) {
            val esWord = appendSpanishWord(meaning)
            val concept = bufferSet.acceptations(accIndex).concept
            bufferSet.acceptations += Acceptation(esWord, concept)
          }
        }
      }
    }
  }

  def readOldWordsFromDatabase: Iterable[OldWord] = {
    val oldWords = ArrayBuffer[OldWord]()

    val outStream = new PrintWriter(new FileOutputStream("WordRegister.csv"))
    try {
      val path = filePath
      println("Connecting to database at " + path)
      val connection = DriverManager.getConnection("jdbc:sqlite:" + path)
      try {
        val statement = connection.createStatement()
        statement.setQueryTimeout(10) // 10 seconds
        val resultSet = statement.executeQuery("SELECT * FROM WordRegister")
        var limit = 10000
        while (limit > 0 && resultSet.next()) {
          val kanjiSymbolArray = resultSet.getString("mWrittenWord")
          val kanaSymbolArray = resultSet.getString("mPronunciation")
          val spSymbolArray = resultSet.getString("meaning")
          val rowValues = List(
            resultSet.getInt("id"),
            kanjiSymbolArray,
            kanaSymbolArray,
            spSymbolArray
          )

          oldWords += OldWord(kanjiSymbolArray, kanaSymbolArray, spSymbolArray)

          outStream.println(rowValues.mkString(","))
          limit -= 1
        }
      }
      finally {
        connection.close()
      }
    }
    finally {
      outStream.close()
    }

    oldWords.toArray[OldWord]
  }

  def main(args: Array[String]): Unit = {
    implicit val bufferSet = initialiseDatabase()

    val oldWords = readOldWordsFromDatabase
    convertCollections(oldWords)

    val outStream2 = new PrintWriter(new FileOutputStream("Words.csv"))
    try {
      var i = 0
      for (repr <- bufferSet.wordRepresentations) {
        if (repr != InvalidRegister.wordRepresentation) {
          val concepts = bufferSet.acceptations.collect { case p if p.word == repr.word => p.concept }
          val conceptsStr = concepts.mkString(" ")
          val lang = languages(languageIndex(repr.word)).code
          outStream2.println(s"$i,$conceptsStr,${repr.word},$lang,${repr.alphabet},${bufferSet.symbolArrays(repr.symbolArray)}")
        }

        i += 1
      }
    }
    finally {
      outStream2.close()
    }
  }
}
