import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.{InputBitStream, IntegerDecoder, NaturalDecoder, RangedIntegerSetDecoder}
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

  private def readAcceptations(
      bufferSet: BufferSet,
      conceptTable: => RangedIntegerHuffmanTable,
      symbolArrayTable: RangedIntegerHuffmanTable,
      minAlphabet: Int,
      maxAlphabet: Int,
      ibs: InputBitStream): Unit = {

    // Import correlations
    val correlationsLength = ibs.readNaturalNumber()
    if (correlationsLength > 0) {
      val correlationTable = new RangedIntegerHuffmanTable(0, correlationsLength - 1)
      val intDecoder = new IntegerDecoder(ibs)
      val correlationLengthTable = ibs.readHuffmanTable[Integer](intDecoder, intDecoder)
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
        val corrArrayLengthTable = ibs.readHuffmanTable(intDecoder, intDecoder)
        for (_ <- 0 until correlationArraysLength) {
          val length = ibs.readHuffmanSymbol(corrArrayLengthTable)
          bufferSet.addCorrelationArrayForIntArray(Array.ofDim[Int](length).map(_ => ibs.readHuffmanSymbol(correlationTable).toInt))
        }

        // Import acceptations
        val acceptationsLength = ibs.readNaturalNumber()
        val corrArraySetLengthTable = ibs.readHuffmanTable(intDecoder, intDecoder)
        for (_ <- 0 until acceptationsLength) {
          val accId = bufferSet.addAcceptation(Acceptation(
            StreamedDatabaseConstants.minValidWord, ibs.readHuffmanSymbol(conceptTable)))

          val corrArraySet = ibs.readRangedNumberSet(corrArraySetLengthTable, 0, correlationArraysLength - 1)
          val currentSet = bufferSet.acceptationCorrelations.getOrElse(accId, Set[Int]())
          bufferSet.acceptationCorrelations(accId) = currentSet ++ corrArraySet
        }
      }
    }
  }

  private def readBunchAcceptations(
      bufferSet: BufferSet,
      minValidConcept: Int,
      maxValidConcept: Int,
      minAcceptation: Int,
      maxAcceptation: Int,
      ibs: InputBitStream): Unit = {
    val bunchAcceptationsLength = ibs.readNaturalNumber()
    if (bunchAcceptationsLength > 0) {
      val bunchAcceptationsLengthTable: HuffmanTable[Integer] = {
        if (bunchAcceptationsLength > 0) {
          val natDecoder = new NaturalDecoder(ibs)
          ibs.readHuffmanTable(natDecoder, natDecoder)
        }
        else null
      }

      var remainingBunches = bunchAcceptationsLength
      var minBunchConcept = minValidConcept
      for (i <- 0 until bunchAcceptationsLength) {
        val table = new RangedIntegerHuffmanTable(minBunchConcept, maxValidConcept - remainingBunches + 1)
        val bunch = ibs.readHuffmanSymbol(table)
        minBunchConcept = bunch + 1
        remainingBunches -= 1

        val acceptations = ibs.readRangedNumberSet(bunchAcceptationsLengthTable, minAcceptation, maxAcceptation)
        val currentSet = bufferSet.bunchAcceptations.getOrElse(bunch, Set[Int]())
        bufferSet.bunchAcceptations(bunch) = currentSet ++ acceptations
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
    val maxConcept = ibs.readNaturalNumber() - 1

    lazy val conceptTable = new RangedIntegerHuffmanTable(minValidConcept, maxConcept)

    // Import acceptations
    readAcceptations(bufferSet, conceptTable, symbolArrayTable, minValidAlphabet, maxValidAlphabet, ibs)

    // Import bunchConcepts
    val bunchConceptsLength = ibs.readNaturalNumber()
    val bunchConceptsLengthTable: HuffmanTable[Integer] = {
      if (bunchConceptsLength > 0) {
        val natDecoder = new NaturalDecoder(ibs)
        ibs.readHuffmanTable(natDecoder, natDecoder)
      }
      else null
    }

    var remainingBunches = bunchConceptsLength
    var minBunchConcept = minValidConcept
    for (i <- 0 until bunchConceptsLength) {
      val table = new RangedIntegerHuffmanTable(minBunchConcept, maxConcept - remainingBunches + 1)
      val bunch = ibs.readHuffmanSymbol(table)
      minBunchConcept = bunch + 1
      remainingBunches -= 1

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

    // Import bunchAcceptations
    readBunchAcceptations(bufferSet, minValidConcept, maxConcept, 0, bufferSet.acceptations.size - 1, ibs)

    // Import agents
    val agentsLength = ibs.readNaturalNumber()
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)
      val sourceSetLengthTable = ibs.readHuffmanTable[Integer](() => ibs.readHuffmanSymbol(nat3Table).toInt, null)

      val nullCorrelation = bufferSet.addCorrelation(Map())
      val correlationTable = new RangedIntegerHuffmanTable(0, bufferSet.correlations.size - 1)

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

        val startMatcher = ibs.readHuffmanSymbol(correlationTable)
        val startAdder = ibs.readHuffmanSymbol(correlationTable)
        val endMatcher = ibs.readHuffmanSymbol(correlationTable)
        val endAdder = ibs.readHuffmanSymbol(correlationTable)

        val hasRule = startMatcher != startAdder || endMatcher != endAdder
        val fromStart = startMatcher != nullCorrelation || startAdder != nullCorrelation

        val matcher = if (fromStart) startMatcher else endMatcher
        val adder = if (fromStart) startAdder else endAdder
        val rule = {
          if (hasRule) ibs.readHuffmanSymbol(conceptTable).toInt
          else StreamedDatabaseConstants.nullBunchId
        }

        bufferSet.agents += Agent(targetBunch, sourceSet, matcher, adder, rule, fromStart)

        lastTarget = targetBunch
      }
    }

    // Import relevant dynamic acceptations - None to be added here
    if (ibs.readNaturalNumber() != 0) {
      throw new UnsupportedOperationException()
    }

    // Import sentence spans - None to be added here
    if (ibs.readNaturalNumber() != 0) {
      throw new UnsupportedOperationException()
    }

    // Import sentence meanings - None to be added here
    if (ibs.readNaturalNumber() != 0) {
      throw new UnsupportedOperationException()
    }

    ibs.close()
  }
}
