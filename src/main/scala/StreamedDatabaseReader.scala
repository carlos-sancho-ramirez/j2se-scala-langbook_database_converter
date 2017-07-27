import sword.bitstream.{InputBitStream, NaturalNumberHuffmanTable}

import scala.collection.mutable.ArrayBuffer

object StreamedDatabaseReader {

  def readSymbolArrays(symbolArrays: ArrayBuffer[String], ibs: InputBitStream): Unit = {

    // Read Huffman table for chars
    val huffmanTable = ibs.readHuffmanTable[Char](() => ibs.readChar())

    // Read Symbol array length Huffman table
    val symbolArraysLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt)

    // Read all symbol arrays
    val symbolArraysLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until symbolArraysLength) {
      val length = ibs.readHuffmanSymbol(symbolArraysLengthHuffmanTable)
      val str = new StringBuilder()
      for (j <- 0 until length) {
        str.append(ibs.readHuffmanSymbol(huffmanTable))
      }

      println(s"SymbolArrays: $symbolArrays")
      symbolArrays += str.toString
    }
  }
}
