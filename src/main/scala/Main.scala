import java.io._
import java.sql.DriverManager

object Main {

  def filePath = {
    val resource = getClass.getClassLoader.getResource("langbook.db")
    if (resource == null) sys.error("Expected file src/main/resources/langbook.db not present")
    new File(resource.toURI).getPath
  }

  def main(args: Array[String]): Unit = {
    val outStream = new PrintWriter(new FileOutputStream("WordRegister.csv"))
    try {
      val path = filePath
      println("Connecting to database at " + path)
      val connection = DriverManager.getConnection("jdbc:sqlite:" + path)
      try {
        val statement = connection.createStatement()
        statement.setQueryTimeout(10) // 10 seconds
        val resultSet = statement.executeQuery("SELECT * FROM WordRegister")
        var limit = 100000
        while (limit > 0 && resultSet.next()) {
          val rowValues = List(
            resultSet.getInt("id"),
            resultSet.getString("mWrittenWord"),
            resultSet.getString("mPronunciation"),
            resultSet.getString("meaning")
          )
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
  }
}
