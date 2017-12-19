import BufferSet.Correlation
import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.{InputBitStream, RangedIntegerSetDecoder}
import sword.bitstream.huffman.{HuffmanTable, NaturalNumberHuffmanTable, RangedIntegerHuffmanTable}

import scala.collection.mutable.ListBuffer

object StreamedDatabaseReader {

  val naturalNumberTable = new NaturalNumberHuffmanTable(8)

  case class RichInputBitStream(ibs: InputBitStream) {
    def readNaturalNumber(): Int = {
      ibs.readHuffmanSymbol(naturalNumberTable).toInt
    }

    def readRangedNumberSet(lengthTable: HuffmanTable[Integer], min: Int, max:Int): Set[Int] = {
      val decoder = new RangedIntegerSetDecoder(ibs, lengthTable, min, max)
      val iterator = ibs.readSet[Integer](decoder, decoder, decoder).iterator
      val set = scala.collection.mutable.Set[Int]()
      while (iterator.hasNext) {
        set += iterator.next()
      }

      set.toSet
    }
  }

  implicit def inputBitStream2RichInputBitStream(ibs: InputBitStream) :RichInputBitStream = RichInputBitStream(ibs)

  def readSymbolArrays(bufferSet: BufferSet, ibs: InputBitStream): Unit = {

    // Read the number of symbol arrays
    val symbolArraysInitLength = bufferSet.symbolArrays.size
    val symbolArraysLength = ibs.readNaturalNumber()

    if (symbolArraysLength > 0) {

      val nat3Table = new NaturalNumberHuffmanTable(3)
      val nat4Table = new NaturalNumberHuffmanTable(4)

      // Read Huffman table for chars
      val huffmanTable = ibs.readHuffmanTable[Char](
        () => ibs.readNaturalNumber().toChar,
        prev => (ibs.readHuffmanSymbol(nat4Table) + prev + 1).toChar)

      // Read Symbol array length Huffman table
      val symbolArraysLengthHuffmanTable = ibs.readHuffmanTable[Int](() => ibs.readNaturalNumber(), prev => ibs.readHuffmanSymbol(nat3Table).toInt + prev + 1)

      // Read all symbol arrays
      for (i <- 0 until symbolArraysLength) {
        val length = ibs.readHuffmanSymbol(symbolArraysLengthHuffmanTable)
        val strBuilder = new StringBuilder()
        for (j <- 0 until length) {
          strBuilder.append(ibs.readHuffmanSymbol(huffmanTable))
        }

        val str = strBuilder.toString
        if (i < symbolArraysInitLength) {
          if (bufferSet.symbolArrays(i) != str) {
            throw new AssertionError(s"Predefined symbol arrays are not respected. At index $i, the expectation was '${bufferSet.symbolArrays(i)}' but was '$str'")
          }
        }
        else {
          bufferSet.addSymbolArray(str.toString)
        }
      }
    }
  }

  private def ensureSameLanguagesAndAlphabets(
      languages: scala.collection.Seq[Main.Language],
      alphabets: scala.collection.Seq[Int],
      symbolArrayTable: RangedIntegerHuffmanTable,
      symbolArrays: scala.collection.IndexedSeq[String],
      ibs: InputBitStream): Unit = {
    val nat2Table = new NaturalNumberHuffmanTable(2)
    if (ibs.readNaturalNumber() != languages.size) {
      throw new AssertionError("Number of languages does not match")
    }

    for (language <- languages) {
      if (symbolArrays(ibs.readHuffmanSymbol(symbolArrayTable)) != language.code) {
        throw new AssertionError(s"Language code for '${language.code}' does not match")
      }

      val alphabetCount = ibs.readHuffmanSymbol(nat2Table).intValue
      if (alphabetCount != language.alphabets.size) {
        throw new AssertionError(s"Number of alphabets does not match for language '${language.code}'. Read $alphabetCount")
      }
    }
  }

  private def readConversions(
      maxValidAlphabet: Int,
      symbolArrayTable: HuffmanTable[Integer],
      ibs: InputBitStream): Set[Conversion] = {
    val conversionsLength = ibs.readNaturalNumber()
    var minSourceAlphabet = minValidAlphabet
    var minTargetAlphabet = minValidAlphabet
    val result = scala.collection.mutable.Set[Conversion]()
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

      result += Conversion(sourceAlphabet, targetAlphabet, pairs)
    }

    result.toSet
  }

  private def readAcceptationsOld(
      bufferSet: BufferSet,
      acceptationsLength: Int,
      wordTable: => RangedIntegerHuffmanTable,
      conceptTable: => RangedIntegerHuffmanTable,
      symbolArrayTable: RangedIntegerHuffmanTable,
      alphabetTable: RangedIntegerHuffmanTable,
      ibs: InputBitStream): Unit = {
    for (i <- 0 until acceptationsLength) {
      val word = ibs.readHuffmanSymbol(wordTable)
      val concept = ibs.readHuffmanSymbol(conceptTable)
      bufferSet.acceptations += Acceptation(word, concept)
    }

    // Export word representations
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
  }

  private def readAcceptations(
      bufferSet: BufferSet,
      wordTable: => RangedIntegerHuffmanTable,
      conceptTable: => RangedIntegerHuffmanTable,
      symbolArrayTable: RangedIntegerHuffmanTable,
      minAlphabet: Int,
      maxAlphabet: Int,
      ibs: InputBitStream): Unit = {

    // Import correlations
    val correlationsLength = ibs.readNaturalNumber()
    if (correlationsLength > 0) {
      val correlationTable = new RangedIntegerHuffmanTable(0, correlationsLength - 1)
      val correlationLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)
      for (i <- 0 until correlationsLength) {
        val keyDecoder = new RangedIntegerSetDecoder(ibs, correlationLengthTable, minAlphabet, maxAlphabet)
        val iterator = ibs.readMap[Integer, Integer](keyDecoder, keyDecoder, keyDecoder, () => ibs.readHuffmanSymbol(symbolArrayTable)).entrySet.iterator
        val map = scala.collection.mutable.Map[Int, Int]()
        while (iterator.hasNext) {
          val pair = iterator.next()
          map(pair.getKey) = pair.getValue
        }
        bufferSet.addCorrelation(map.toMap)
      }

      // Import correlation arrays
      val correlationArraysLength = ibs.readNaturalNumber()
      if (correlationArraysLength > 0) {
        val correlationArrayTable = new RangedIntegerHuffmanTable(0, correlationArraysLength - 1)

        val corrArrayLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)
        for (i <- 0 until correlationArraysLength) {
          val length = ibs.readHuffmanSymbol(corrArrayLengthTable)
          bufferSet.addCorrelationArrayForIntArray(Array.ofDim[Int](length).map(_ => ibs.readHuffmanSymbol(correlationTable).toInt))
        }

        // Import acceptations
        val acceptationsLength = ibs.readNaturalNumber()
        for (i <- 0 until acceptationsLength) {
          bufferSet.addAcceptation(NewAcceptation(
            ibs.readHuffmanSymbol(wordTable),
            ibs.readHuffmanSymbol(conceptTable),
            ibs.readHuffmanSymbol(correlationArrayTable)))
        }
      }
    }
  }

  def read(bufferSet: BufferSet, ibs: InputBitStream): Unit = {
    readSymbolArrays(bufferSet, ibs)
    val symbolArraysLength = bufferSet.symbolArrays.length

    // Ensure same languages (as they are constant here)
    val symbolArrayTable = new RangedIntegerHuffmanTable(0, symbolArraysLength - 1)
    ensureSameLanguagesAndAlphabets(bufferSet.languages, bufferSet.alphabets, symbolArrayTable, bufferSet.symbolArrays, ibs)

    // Export conversions
    val maxValidAlphabet = minValidAlphabet + bufferSet.alphabets.size - 1
    bufferSet.conversions ++= readConversions(maxValidAlphabet, symbolArrayTable, ibs)

    // Export the amount of words and concepts in order to range integers
    val maxWord = ibs.readNaturalNumber() - 1
    val maxConcept = ibs.readNaturalNumber() - 1

    lazy val wordTable = new RangedIntegerHuffmanTable(minValidWord, maxWord)
    lazy val conceptTable = new RangedIntegerHuffmanTable(minValidConcept, maxConcept)

    // Export acceptations (old)
    val acceptationsLength = ibs.readNaturalNumber()
    val alphabetTable = new RangedIntegerHuffmanTable(minValidAlphabet, maxValidAlphabet)
    readAcceptationsOld(bufferSet, acceptationsLength, wordTable, conceptTable, symbolArrayTable, alphabetTable, ibs)

    // Import acceptations
    readAcceptations(bufferSet, wordTable, conceptTable, symbolArrayTable, minValidAlphabet, maxValidAlphabet, ibs)

    // Export bunchConcepts
    val bunchConceptsLength = ibs.readNaturalNumber()
    val bunchConceptsLengthTable: HuffmanTable[Integer] = {
      if (bunchConceptsLength > 0) ibs.readHuffmanTable(() => ibs.readNaturalNumber(), prev => ibs.readNaturalNumber() + prev + 1)
      else null
    }

    for (i <- 0 until bunchConceptsLength) {
      val bunch = ibs.readHuffmanSymbol(conceptTable)
      val concepts = ibs.readRangedNumberSet(bunchConceptsLengthTable, minValidConcept, maxConcept)
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
      val acceptations = ibs.readRangedNumberSet(bunchAcceptationsLengthTable, 0, acceptationsLength - 1)
      bufferSet.bunchAcceptations(bunch) = acceptations
    }

    // Export agents
    val agentsLength = ibs.readNaturalNumber()
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)
      val sourceSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)

      val correlationTable = new RangedIntegerHuffmanTable(0, bufferSet.correlations.size - 1)
      val nullCorrelation = bufferSet.addCorrelation(Map())

      var lastTarget = StreamedDatabaseConstants.nullBunchId
      var minSource = StreamedDatabaseConstants.minValidConcept
      for (i <- 0 until agentsLength) {
        val targetTable = new RangedIntegerHuffmanTable(lastTarget, maxConcept)
        val targetBunch = ibs.readHuffmanSymbol(targetTable).toInt

        if (targetBunch != lastTarget) {
          minSource = StreamedDatabaseConstants.minValidConcept
        }

        val sourceSet = ibs.readRangedNumberSet(sourceSetLengthTable, minSource, maxConcept)
        if (sourceSet.nonEmpty) {
          minSource = sourceSet.min
        }

        val matcher = ibs.readHuffmanSymbol(correlationTable)
        val adder = ibs.readHuffmanSymbol(correlationTable)

        val rule = {
          if (adder != nullCorrelation) {
            ibs.readHuffmanSymbol(conceptTable).toInt
          }
          else StreamedDatabaseConstants.nullBunchId
        }

        val fromStart = (matcher != nullCorrelation || adder != nullCorrelation) && ibs.readBoolean()

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
