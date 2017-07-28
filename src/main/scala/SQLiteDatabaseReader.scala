import java.io.{FileOutputStream, PrintWriter}
import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer

case class OldWord(wordId: Int, kanjiSymbolArray: String, kanaSymbolArray: String, spSymbolArray: String)

/**
  * Register representation for list-child relationship in old databases
  * @param listId Identifier for the list
  * @param childId Identifier for the word, list, grammar constraint, grammar rule or quiz.
  * @param childType Determines the children type (word = 0)
  */
case class ListChildRegister(listId: Int, childId: Int, childType: Int)

class SQLiteDatabaseReader(val filePath: String) {

  def readOldWords: Iterable[OldWord] = {
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
          val wordId = resultSet.getInt("id")
          val kanjiSymbolArray = resultSet.getString("mWrittenWord")
          val kanaSymbolArray = resultSet.getString("mPronunciation")
          val spSymbolArray = resultSet.getString("meaning")
          val rowValues = List(
            wordId,
            kanjiSymbolArray,
            kanaSymbolArray,
            spSymbolArray
          )

          oldWords += OldWord(wordId, kanjiSymbolArray, kanaSymbolArray, spSymbolArray)

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

  def readOldLists: Map[Int,String] = {
    val oldLists = scala.collection.mutable.Map[Int,String]()

    val outStream = new PrintWriter(new FileOutputStream("ListRegister.csv"))
    try {
      val path = filePath
      println("Connecting to database at " + path)
      val connection = DriverManager.getConnection("jdbc:sqlite:" + path)
      try {
        val statement = connection.createStatement()
        statement.setQueryTimeout(10) // 10 seconds
        val resultSet = statement.executeQuery("SELECT * FROM ListRegister")
        var limit = 10000
        while (limit > 0 && resultSet.next()) {
          val id = resultSet.getInt("id")
          val name = resultSet.getString("name")
          val rowValues = s"$id,$name"
          oldLists(id) = name

          outStream.println(rowValues)
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

    oldLists.toMap
  }

  def readOldListChildren: Iterable[ListChildRegister] = {
    val listChildren = ArrayBuffer[ListChildRegister]()

    val outStream = new PrintWriter(new FileOutputStream("ListChildRegister.csv"))
    try {
      val path = filePath
      println("Connecting to database at " + path)
      val connection = DriverManager.getConnection("jdbc:sqlite:" + path)
      try {
        val statement = connection.createStatement()
        statement.setQueryTimeout(10) // 10 seconds
        val resultSet = statement.executeQuery("SELECT * FROM ListChildRegister")
        var limit = 10000
        while (limit > 0 && resultSet.next()) {
          val listId = resultSet.getInt("listId")
          val childId = resultSet.getInt("childId")
          val childType = resultSet.getInt("childRegisterIndex")
          val rowValues = s"$listId,$childId,$childType"
          listChildren += ListChildRegister(listId, childId, childType)

          outStream.println(rowValues)
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

    listChildren
  }
}
