import java.io._

object Main {

  def filePath = {
    val resource = getClass.getClassLoader.getResource("langbook.db")
    if (resource == null) sys.error("Expected file src/main/resources/langbook.db not present")
    new File(resource.toURI).getPath
  }

  def main(args: Array[String]): Unit = {
    val stream = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)))
    try {
      println(stream.readLine())
    }
    finally {
      stream.close()
    }
  }
}
