import StreamedDatabaseConstants.{maxValidAlphabet, minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.InputBitStream

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

      symbolArrays += str.toString
    }
  }

  def read(bufferSet: BufferSet, ibs: InputBitStream): Unit = {
    readSymbolArrays(bufferSet.symbolArrays, ibs)

    // Export the amount of words and concepts in order to range integers
    val maxWord = ibs.readNaturalNumber().toInt - 1
    val maxConcept = ibs.readNaturalNumber().toInt - 1

    // Export acceptations
    val acceptationsLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until acceptationsLength) {
      val word = ibs.readRangedNumber(minValidWord, maxWord)
      val concept = ibs.readRangedNumber(minValidConcept, maxConcept)
      bufferSet.acceptations += Acceptation(word, concept)
    }

    // Export word representations
    val symbolArraysLength = bufferSet.symbolArrays.length
    val wordRepresentationLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until wordRepresentationLength) {
      val word = ibs.readRangedNumber(minValidWord, maxWord)
      val concept = ibs.readRangedNumber(minValidAlphabet, maxValidAlphabet)
      val symbolArray = ibs.readRangedNumber(0, symbolArraysLength - 1)
      bufferSet.wordRepresentations += WordRepresentation(word, concept, symbolArray)
    }

    // Export acceptation representations
    val accRepresentationLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until accRepresentationLength) {
      val acc = ibs.readRangedNumber(0, acceptationsLength - 1)
      val symbolArray = ibs.readRangedNumber(0, symbolArraysLength - 1)
      bufferSet.accRepresentations += AccRepresentation(acc, symbolArray)
    }

    ibs.close()
  }
}
