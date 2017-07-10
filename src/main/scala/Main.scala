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

  case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)

  val representations = ArrayBuffer[WordRepresentation]()

  var wordCount = 0

  def appendEnglishWord(symbolArray: String): Unit = {
    representations.append(WordRepresentation(wordCount, enAlphabet, addSymbolArray(symbolArray)))
    wordCount += 1
  }

  def appendSpanishWord(symbolArray: String): Unit = {
    representations.append(WordRepresentation(wordCount, esAlphabet, addSymbolArray(symbolArray)))
    wordCount += 1
  }

  def appendJapaneseWord(kanjiSymbolArray: String, kanaSymbolArray:String): Unit = {
    representations.append(WordRepresentation(wordCount, kanjiAlphabet, addSymbolArray(kanjiSymbolArray)))
    representations.append(WordRepresentation(wordCount, kanaAlphabet, addSymbolArray(kanaSymbolArray)))
    wordCount += 1
  }

  def initialiseDatabase(): Unit = {
    appendEnglishWord("Language")
    appendSpanishWord("Idioma")
    appendJapaneseWord("言語", "げんご")

    appendEnglishWord("English")
    appendSpanishWord("Inglés")
    appendJapaneseWord("英語", "えいご")

    appendEnglishWord("Spanish")
    appendSpanishWord("Español")
    appendJapaneseWord("スペイン語", "スペイン")

    appendEnglishWord("Japanese")
    appendSpanishWord("Japonés")
    appendJapaneseWord("日本語", "にほんご")

    appendJapaneseWord("漢字", "かんじ")
    appendJapaneseWord("平仮名", "かな")
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

          appendJapaneseWord(kanjiSymbolArray, kanaSymbolArray)
          appendSpanishWord(spSymbolArray)

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

    val outStream2 = new PrintWriter(new FileOutputStream("WordRepr.csv"))
    try {
      var i = 0
      for (repr <- representations) {
        outStream2.println(s"$i,${repr.word},${repr.alphabet},${symbolArrays(repr.symbolArray)}")
        i += 1
      }
    }
    finally {
      outStream2.close()
    }
  }
}
