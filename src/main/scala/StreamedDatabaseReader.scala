import StreamedDatabaseConstants.{maxValidAlphabet, minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.{HuffmanTable, InputBitStream}

import scala.collection.mutable.ArrayBuffer

object StreamedDatabaseReader {

  def readSymbolArrays(symbolArrays: ArrayBuffer[String], ibs: InputBitStream): Unit = {

    // Read the number of symbol arrays
    val symbolArraysLength = ibs.readNaturalNumber().toInt

    if (symbolArraysLength > 0) {
      // Read Huffman table for chars
      val huffmanTable = ibs.readHuffmanTable[Char](() => ibs.readChar())

      // Read Symbol array length Huffman table
      val symbolArraysLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt)

      // Read all symbol arrays
      for (i <- 0 until symbolArraysLength) {
        val length = ibs.readHuffmanSymbol(symbolArraysLengthHuffmanTable)
        val str = new StringBuilder()
        for (j <- 0 until length) {
          str.append(ibs.readHuffmanSymbol(huffmanTable))
        }

        symbolArrays += str.toString
      }
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

    // Export kanji-kana correlations
    val kanjiKanaCorrelationsLength = ibs.readNaturalNumber().toInt
    for (i <- 0 until kanjiKanaCorrelationsLength) {
      val kanji = ibs.readRangedNumber(0, symbolArraysLength - 1)
      val kana = ibs.readRangedNumber(0, symbolArraysLength - 1)
      bufferSet.kanjiKanaCorrelations += ((kanji, kana))
    }

    // Export jaWordCorrelations
    val jaWordCorrelationsLength = ibs.readNaturalNumber().toInt
    if (jaWordCorrelationsLength > 0) {
      val correlationReprCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt)
      val correlationConceptCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt)
      val correlationVectorLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt)

      for (i <- 0 until jaWordCorrelationsLength) {
        val wordId = ibs.readRangedNumber(minValidWord, maxWord)
        val reprCount = ibs.readHuffmanSymbol(correlationReprCountHuffmanTable)

        val reprs = for (j <- 0 until reprCount) yield {
          val conceptSetLength = ibs.readHuffmanSymbol(correlationConceptCountHuffmanTable)
          val concepts = for (k <- 0 until conceptSetLength) yield {
            ibs.readRangedNumber(minValidConcept, maxConcept)
          }

          val vectorLength = ibs.readHuffmanSymbol(correlationVectorLengthHuffmanTable)
          val vector = for (k <- 0 until vectorLength) yield {
            ibs.readRangedNumber(0, kanjiKanaCorrelationsLength - 1)
          }

          (concepts.toSet, vector.toVector)
        }

        bufferSet.jaWordCorrelations(wordId) = reprs.toSet
      }
    }

    // Export bunchConcepts
    val bunchConceptsLength = ibs.readNaturalNumber().toInt
    val bunchConceptsLengthTable: HuffmanTable[Integer] = {
      if (bunchConceptsLength > 0) ibs.readHuffmanTable(() => ibs.readNaturalNumber().toInt)
      else null
    }

    for (i <- 0 until bunchConceptsLength) {
      val bunch = ibs.readRangedNumber(minValidConcept, maxConcept)
      val iterator = ibs.readRangedNumberSet(bunchConceptsLengthTable, minValidConcept, maxConcept).iterator
      val concepts = scala.collection.mutable.Set[Int]()
      while (iterator.hasNext) {
        concepts += iterator.next()
      }

      bufferSet.bunchConcepts(bunch) = concepts.toSet
    }

    // Export bunchAcceptations
    val bunchAcceptationsLength = ibs.readNaturalNumber().toInt
    val bunchAcceptationsLengthTable: HuffmanTable[Integer] = {
      if (bunchAcceptationsLength > 0) ibs.readHuffmanTable(() => Integer.valueOf(ibs.readNaturalNumber().toInt))
      else null
    }

    for (i <- 0 until bunchAcceptationsLength) {
      val bunch = ibs.readRangedNumber(minValidConcept, maxConcept)
      val iterator = ibs.readRangedNumberSet(bunchAcceptationsLengthTable, 0, acceptationsLength - 1).iterator
      val acceptations = scala.collection.mutable.Set[Int]()
      while (iterator.hasNext) {
        acceptations += iterator.next()
      }

      bufferSet.bunchAcceptations(bunch) = acceptations.toSet
    }

    ibs.close()
  }
}
