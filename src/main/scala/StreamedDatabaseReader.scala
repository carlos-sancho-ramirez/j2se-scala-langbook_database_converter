import sword.bitstream.InputBitStream

import scala.collection.mutable.ArrayBuffer

object StreamedDatabaseReader {

  def readSymbolArrays(symbolArrays: ArrayBuffer[String], ibs: InputBitStream): Unit = {

    // Read Huffman table for chars
    val huffmanTable = ibs.readHuffmanTable[Char](() => ibs.readChar())

    // Read all symbol arrays
    val symbolArraysLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until symbolArraysLength) {
      val length = ibs.readNaturalNumber().toInt
      val str = new StringBuilder()
      for (j <- 0 until length) {
        str.append(ibs.readHuffmanSymbol(huffmanTable))
      }

      println(s"SymbolArrays: $symbolArrays")
      symbolArrays += str.toString
    }
  }
}
