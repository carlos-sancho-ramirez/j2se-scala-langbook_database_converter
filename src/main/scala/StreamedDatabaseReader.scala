import BufferSet.Correlation
import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.{HuffmanTable, InputBitStream, NaturalNumberHuffmanTable}

import scala.collection.mutable.ArrayBuffer

object StreamedDatabaseReader {

  def readSymbolArrays(symbolArrays: ArrayBuffer[String], ibs: InputBitStream): Unit = {

    // Read the number of symbol arrays
    val symbolArraysInitLength = symbolArrays.size
    val symbolArraysLength = ibs.readNaturalNumber().toInt

    if (symbolArraysLength > 0) {

      val nat3Table = new NaturalNumberHuffmanTable(3)
      val nat4Table = new NaturalNumberHuffmanTable(4)

      // Read Huffman table for chars
      val huffmanTable = ibs.readHuffmanTable[Char](() => ibs.readChar(), prev => (ibs.readHuffmanSymbol(nat4Table) + prev + 1).toChar)

      // Read Symbol array length Huffman table
      val symbolArraysLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt, prev => ibs.readHuffmanSymbol(nat3Table).toInt + prev + 1)

      // Read all symbol arrays
      for (i <- 0 until symbolArraysLength) {
        val length = ibs.readHuffmanSymbol(symbolArraysLengthHuffmanTable)
        val strBuilder = new StringBuilder()
        for (j <- 0 until length) {
          strBuilder.append(ibs.readHuffmanSymbol(huffmanTable))
        }

        val str = strBuilder.toString
        if (i < symbolArraysInitLength) {
          if (symbolArrays(i) != str) {
            throw new AssertionError(s"Predefined symbol arrays are not respected. At index $i, the expectation was '${symbolArrays(i)}' but was '$str'")
          }
        }
        else {
          symbolArrays += str.toString
        }
      }
    }
  }

  def read(bufferSet: BufferSet, ibs: InputBitStream): Unit = {
    readSymbolArrays(bufferSet.symbolArrays, ibs)
    val symbolArraysLength = bufferSet.symbolArrays.length

    // Ensure same languages (as they are constant here)
    val nat2Table = new NaturalNumberHuffmanTable(2)
    if (ibs.readNaturalNumber() != bufferSet.languages.size) {
      throw new AssertionError("Number of languages does not match")
    }

    for (language <- bufferSet.languages) {
      if (bufferSet.symbolArrays(ibs.readRangedNumber(0, symbolArraysLength - 1)) != language.code) {
        throw new AssertionError(s"Language code for '${language.code}' does not match")
      }

      val alphabetCount = ibs.readHuffmanSymbol(nat2Table).intValue
      if (alphabetCount != language.alphabets.size) {
        throw new AssertionError(s"Number of alphabets does not match for language '${language.code}'. Read $alphabetCount")
      }
    }

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
    val maxValidAlphabet = minValidAlphabet + bufferSet.alphabets.size - 1
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
      val correlationReprCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt, prev => ibs.readNaturalNumber().toInt + prev + 1)
      val correlationConceptCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt, prev => ibs.readNaturalNumber().toInt + prev + 1)
      val correlationVectorLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber().toInt, prev => ibs.readNaturalNumber().toInt + prev + 1)

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
      if (bunchConceptsLength > 0) ibs.readHuffmanTable(() => ibs.readNaturalNumber().toInt, prev => ibs.readNaturalNumber().toInt + prev + 1)
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
      if (bunchAcceptationsLength > 0) ibs.readHuffmanTable(() => Integer.valueOf(ibs.readNaturalNumber().toInt), prev => ibs.readNaturalNumber().toInt + prev + 1)
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

    // Export agents
    val agentsLength = ibs.readNaturalNumber().toInt
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)
      val sourceSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)
      val matcherSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)

      var lastTarget = StreamedDatabaseConstants.nullBunchId
      var minSource = StreamedDatabaseConstants.minValidConcept
      for (i <- 0 until agentsLength) {
        val targetBunch = ibs.readRangedNumber(lastTarget, maxConcept)

        if (targetBunch != lastTarget) {
          minSource = StreamedDatabaseConstants.minValidConcept
        }

        val sourceJavaSetIterator = ibs.readRangedNumberSet(sourceSetLengthTable, minSource, maxConcept).iterator()
        val mutableSourceSet = scala.collection.mutable.Set[Int]()
        while (sourceJavaSetIterator.hasNext) {
          mutableSourceSet += sourceJavaSetIterator.next()
        }

        if (mutableSourceSet.nonEmpty) {
          minSource = mutableSourceSet.min
        }

        def readCorrelationMap(): Correlation = {
          val maxAlphabet = bufferSet.alphabets.size - 1
          val mapLength = ibs.readHuffmanSymbol(matcherSetLengthTable)
          val result = scala.collection.mutable.Map[Int, Int]()
          var minAlphabet = 0
          for (i <- 0 until mapLength) {
            val alphabet = ibs.readRangedNumber(minAlphabet, maxAlphabet)
            minAlphabet = alphabet + 1
            val symbolArrayIndex = ibs.readRangedNumber(0, symbolArraysLength - 1)
            result(alphabet) = symbolArrayIndex
          }

          result.toMap
        }

        val matcher = readCorrelationMap()
        val adder = readCorrelationMap()

        val rule = {
          if (adder.nonEmpty) {
            ibs.readRangedNumber(StreamedDatabaseConstants.minValidConcept, maxConcept)
          }
          else StreamedDatabaseConstants.nullBunchId
        }

        val fromStart = (matcher.nonEmpty || adder.nonEmpty) && ibs.readBoolean()

        bufferSet.agents += Agent(targetBunch, mutableSourceSet.toSet, matcher, adder, rule, fromStart)

        lastTarget = targetBunch
      }
    }

    // Export ruleConcepts
    if (ibs.readNaturalNumber() != 0L) {
      throw new AssertionError("Not expected any register within the ruleConcepts table")
    }

    ibs.close()
  }
}
