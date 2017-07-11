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

  val symbolArrays = ArrayBuffer[String]()

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

  val enWords = scala.collection.mutable.BitSet()
  val esWords = scala.collection.mutable.BitSet()
  val jaWords = scala.collection.mutable.BitSet()
  var wordCount = 0

  case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)
  val representations = ArrayBuffer[WordRepresentation]()

  case class Acceptation(word: Int, concept: Int)
  val acceptations = ArrayBuffer[Acceptation]()

  /**
    * Add a new English word in the list and returns its id
    */
  def appendEnglishWord(symbolArray: String): Int = {
    val newIndex = wordCount
    representations.append(WordRepresentation(newIndex, enAlphabet, addSymbolArray(symbolArray)))
    wordCount += 1
    enWords += newIndex
    newIndex
  }

  /**
    * Add a new Spanish word in the list and returns its id
    */
  def appendSpanishWord(symbolArray: String): Int = {
    val newIndex = wordCount
    representations.append(WordRepresentation(newIndex, esAlphabet, addSymbolArray(symbolArray)))
    wordCount += 1
    esWords += newIndex
    newIndex
  }

  /**
    * Add a new Japanese word in the list and returns its id
    */
  def appendJapaneseWord(kanjiSymbolArray: String, kanaSymbolArray:String): Int = {
    val newIndex = wordCount
    representations.append(WordRepresentation(wordCount, kanjiAlphabet, addSymbolArray(kanjiSymbolArray)))
    representations.append(WordRepresentation(wordCount, kanaAlphabet, addSymbolArray(kanaSymbolArray)))

    wordCount += 1
    jaWords += newIndex
    newIndex
  }

  def registerWord(concept: Int, en: String, es: String, kanji: String, kana: String): Unit = {
    if (en != null) {
      val word = appendEnglishWord(en)
      acceptations += Acceptation(word, concept)
    }

    if (es != null) {
      val word = appendSpanishWord(es)
      acceptations += Acceptation(word, concept)
    }

    val jaWord = appendJapaneseWord(kanji, kana)
    acceptations += Acceptation(jaWord, concept)
  }

  def registerWord(en: String, es: String, kanji: String, kana: String): Unit = {
    val concept = conceptCount
    conceptCount += 1

    registerWord(concept, en, es, kanji, kana)
  }

  def initialiseDatabase(): Unit = {
    registerWord("Language", "Idioma", "言語", "げんご")
    registerWord(enLanguage, "English", "Inglés", "英語", "えいご")
    registerWord(esLanguage, "Spanish", "Español", "スペイン語", "スペイン")
    registerWord(jaLanguage, "Japanese", "Japonés", "日本語", "にほんご")
    registerWord(null, null, "漢字", "かんじ")
    registerWord(null, null, "平仮名", "かな")
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

  def main(args: Array[String]): Unit = {
    initialiseDatabase()

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

          registerWord(null, spSymbolArray, kanjiSymbolArray, kanaSymbolArray)

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

    val outStream2 = new PrintWriter(new FileOutputStream("Words.csv"))
    try {
      var i = 0
      for (repr <- representations) {
        val concepts = acceptations.collect { case p if p.word == repr.word => p.concept}
        val conceptsStr = concepts.mkString(" ")
        val lang = languages(languageIndex(repr.word)).code
        outStream2.println(s"$i,$conceptsStr,${repr.word},$lang,${repr.alphabet},${symbolArrays(repr.symbolArray)}")
        i += 1
      }
    }
    finally {
      outStream2.close()
    }
  }
}
