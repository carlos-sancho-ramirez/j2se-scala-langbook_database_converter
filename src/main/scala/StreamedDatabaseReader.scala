import BufferSet.Correlation
import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.InputBitStream
import sword.bitstream.huffman.{HuffmanTable, NaturalNumberHuffmanTable, RangedIntegerHuffmanTable}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object StreamedDatabaseReader {

  def readSymbolArrays(symbolArrays: ArrayBuffer[String], ibs: InputBitStream): Unit = {

    // Read the number of symbol arrays
    val symbolArraysInitLength = symbolArrays.size
    val symbolArraysLength = ibs.readNaturalNumber().toInt

    if (symbolArraysLength > 0) {

      val nat3Table = new NaturalNumberHuffmanTable(3)
      val nat4Table = new NaturalNumberHuffmanTable(4)

      // Read Huffman table for chars
      val huffmanTable = ibs.readHuffmanTable[Char](
        () => ibs.readNaturalNumber().toChar,
        prev => (ibs.readHuffmanSymbol(nat4Table) + prev + 1).toChar)

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

  def readRangedNumberSet(ibs: InputBitStream, lengthTable: HuffmanTable[Integer], min: Int, max:Int): Set[Int] = {
    val length = ibs.readHuffmanSymbol(lengthTable)
    val result = scala.collection.mutable.Set[Int]()
    var currentMin = min
    var currentMax = max - length + 1
    for (i <- 0 until length) {
      val table = new RangedIntegerHuffmanTable(currentMin, currentMax)
      val value = ibs.readHuffmanSymbol(table)
      result += value
      currentMin = value + 1
      currentMax += 1
    }

    result.toSet
  }

  def read(bufferSet: BufferSet, ibs: InputBitStream): Unit = {
    readSymbolArrays(bufferSet.symbolArrays, ibs)
    val symbolArraysLength = bufferSet.symbolArrays.length

    // Ensure same languages (as they are constant here)
    val nat2Table = new NaturalNumberHuffmanTable(2)
    if (ibs.readNaturalNumber() != bufferSet.languages.size) {
      throw new AssertionError("Number of languages does not match")
    }

    val symbolArrayTable = new RangedIntegerHuffmanTable(0, symbolArraysLength - 1)
    for (language <- bufferSet.languages) {
      if (bufferSet.symbolArrays(ibs.readHuffmanSymbol(symbolArrayTable)) != language.code) {
        throw new AssertionError(s"Language code for '${language.code}' does not match")
      }

      val alphabetCount = ibs.readHuffmanSymbol(nat2Table).intValue
      if (alphabetCount != language.alphabets.size) {
        throw new AssertionError(s"Number of alphabets does not match for language '${language.code}'. Read $alphabetCount")
      }
    }

    // Export conversions
    val maxValidAlphabet = minValidAlphabet + bufferSet.alphabets.size - 1
    val conversionsLength = ibs.readNaturalNumber()
    var minSourceAlphabet = minValidAlphabet
    var minTargetAlphabet = minValidAlphabet
    for (i <- 0 until conversionsLength) {
      val sourceTable = new RangedIntegerHuffmanTable(minSourceAlphabet, maxValidAlphabet)
      val sourceAlphabet = ibs.readHuffmanSymbol(sourceTable).toInt

      if (minSourceAlphabet != sourceAlphabet) {
        minTargetAlphabet = minValidAlphabet
        minSourceAlphabet = sourceAlphabet
      }

      val targetTable = new RangedIntegerHuffmanTable(minTargetAlphabet, maxValidAlphabet)
      val targetAlphabet = ibs.readHuffmanSymbol(targetTable).toInt
      minTargetAlphabet = targetAlphabet + 1

      val pairCount = ibs.readNaturalNumber()
      val pairs = new ListBuffer[(Int, Int)]()
      for (j <- 0 until pairCount) {
        val source = ibs.readHuffmanSymbol(symbolArrayTable)
        val target = ibs.readHuffmanSymbol(symbolArrayTable)
        pairs += ((source, target))
      }

      bufferSet.conversions += Conversion(sourceAlphabet, targetAlphabet, pairs)
    }

    // Export the amount of words and concepts in order to range integers
    val maxWord = ibs.readNaturalNumber() - 1
    val maxConcept = ibs.readNaturalNumber() - 1

    lazy val wordTable = new RangedIntegerHuffmanTable(minValidWord, maxWord)
    lazy val conceptTable = new RangedIntegerHuffmanTable(minValidConcept, maxConcept)

    // Export acceptations
    val acceptationsLength = ibs.readNaturalNumber()
    for (i <- 0 until acceptationsLength) {
      val word = ibs.readHuffmanSymbol(wordTable)
      val concept = ibs.readHuffmanSymbol(conceptTable)
      bufferSet.acceptations += Acceptation(word, concept)
    }

    // Export word representations
    val alphabetTable = new RangedIntegerHuffmanTable(minValidAlphabet, maxValidAlphabet)
    val wordRepresentationLength = ibs.readNaturalNumber()
    for (i <- 0 until wordRepresentationLength) {
      val word = ibs.readHuffmanSymbol(wordTable)
      val concept = ibs.readHuffmanSymbol(alphabetTable)
      val symbolArray = ibs.readHuffmanSymbol(symbolArrayTable)
      bufferSet.wordRepresentations += WordRepresentation(word, concept, symbolArray)
    }

    // Export kanji-kana correlations
    val kanjiKanaCorrelationsLength = ibs.readNaturalNumber()
    lazy val kanjiKanaCorrelationTable = new RangedIntegerHuffmanTable(0, kanjiKanaCorrelationsLength - 1)
    for (i <- 0 until kanjiKanaCorrelationsLength) {
      val kanji = ibs.readHuffmanSymbol(symbolArrayTable)
      val kana = ibs.readHuffmanSymbol(symbolArrayTable)
      bufferSet.kanjiKanaCorrelations += ((kanji, kana))
    }

    // Export jaWordCorrelations
    val jaWordCorrelationsLength = ibs.readNaturalNumber()
    if (jaWordCorrelationsLength > 0) {
      val correlationReprCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)
      val correlationConceptCountHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)
      val correlationVectorLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)

      for (i <- 0 until jaWordCorrelationsLength) {
        val wordId = ibs.readHuffmanSymbol(wordTable)
        val reprCount = ibs.readHuffmanSymbol(correlationReprCountHuffmanTable)

        val reprs = for (j <- 0 until reprCount) yield {
          val conceptSetLength = ibs.readHuffmanSymbol(correlationConceptCountHuffmanTable)
          val concepts = for (k <- 0 until conceptSetLength) yield {
            ibs.readHuffmanSymbol(conceptTable).toInt
          }

          val vectorLength = ibs.readHuffmanSymbol(correlationVectorLengthHuffmanTable)
          val vector = for (k <- 0 until vectorLength) yield {
            ibs.readHuffmanSymbol(kanjiKanaCorrelationTable).toInt
          }

          (concepts.toSet, vector.toVector)
        }

        bufferSet.jaWordCorrelations(wordId) = reprs.toSet
      }
    }

    // Export bunchConcepts
    val bunchConceptsLength = ibs.readNaturalNumber()
    val bunchConceptsLengthTable: HuffmanTable[Integer] = {
      if (bunchConceptsLength > 0) ibs.readHuffmanTable(() => ibs.readNaturalNumber().toInt, prev => ibs.readNaturalNumber().toInt + prev + 1)
      else null
    }

    for (i <- 0 until bunchConceptsLength) {
      val bunch = ibs.readHuffmanSymbol(conceptTable)
      val concepts = readRangedNumberSet(ibs, bunchConceptsLengthTable, minValidConcept, maxConcept)
      val previousOpt = bufferSet.bunchConcepts.get(bunch)
      if (previousOpt.nonEmpty) {
        if (concepts != previousOpt.get) {
          throw new AssertionError("Repeated key should match in value")
        }
      }
      else {
        bufferSet.bunchConcepts(bunch) = concepts
      }
    }

    // Export bunchAcceptations
    val bunchAcceptationsLength = ibs.readNaturalNumber()
    val bunchAcceptationsLengthTable: HuffmanTable[Integer] = {
      if (bunchAcceptationsLength > 0) ibs.readHuffmanTable(() => Integer.valueOf(ibs.readNaturalNumber().toInt), prev => ibs.readNaturalNumber().toInt + prev + 1)
      else null
    }

    for (i <- 0 until bunchAcceptationsLength) {
      val bunch = ibs.readHuffmanSymbol(conceptTable)
      val acceptations = readRangedNumberSet(ibs, bunchAcceptationsLengthTable, 0, acceptationsLength - 1)
      bufferSet.bunchAcceptations(bunch) = acceptations
    }

    // Export agents
    val agentsLength = ibs.readNaturalNumber()
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)
      val sourceSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)
      val matcherSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)

      var lastTarget = StreamedDatabaseConstants.nullBunchId
      var minSource = StreamedDatabaseConstants.minValidConcept
      for (i <- 0 until agentsLength) {
        val targetTable = new RangedIntegerHuffmanTable(lastTarget, maxConcept)
        val targetBunch = ibs.readHuffmanSymbol(targetTable).toInt

        if (targetBunch != lastTarget) {
          minSource = StreamedDatabaseConstants.minValidConcept
        }

        val sourceSet = readRangedNumberSet(ibs, sourceSetLengthTable, minSource, maxConcept)
        if (sourceSet.nonEmpty) {
          minSource = sourceSet.min
        }

        def readCorrelationMap(): Correlation = {
          val maxAlphabet = bufferSet.alphabets.max
          val mapLength = ibs.readHuffmanSymbol(matcherSetLengthTable)
          val result = scala.collection.mutable.Map[Int, Int]()
          var minAlphabet = bufferSet.alphabets.min
          for (i <- 0 until mapLength) {
            val table = new RangedIntegerHuffmanTable(minAlphabet, maxAlphabet)
            val alphabet = ibs.readHuffmanSymbol(table)
            minAlphabet = alphabet + 1
            val symbolArrayIndex = ibs.readHuffmanSymbol(symbolArrayTable)
            result(alphabet) = symbolArrayIndex
          }

          result.toMap
        }

        val matcher = readCorrelationMap()
        val adder = readCorrelationMap()

        val rule = {
          if (adder.nonEmpty) {
            ibs.readHuffmanSymbol(conceptTable).toInt
          }
          else StreamedDatabaseConstants.nullBunchId
        }

        val fromStart = (matcher.nonEmpty || adder.nonEmpty) && ibs.readBoolean()

        bufferSet.agents += Agent(targetBunch, sourceSet, matcher, adder, rule, fromStart)

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
