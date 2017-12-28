import java.io.{File, FileOutputStream, IOException, RandomAccessFile}
import java.security.{DigestOutputStream, MessageDigest}

import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream._
import sword.bitstream.huffman._

object StreamedDatabaseWriter {

  val naturalNumberTable = new NaturalNumberHuffmanTable(8)

  case class RichOutputBitStream(obs: OutputBitStream) {
    def writeNaturalNumber(value: Int) = {
      obs.writeHuffmanSymbol[Integer](naturalNumberTable, value)
    }

    def writeRangedNumberSet(lengthTable: HuffmanTable[Integer], min: Int, max: Int, set: Set[Int]): Unit = {
      val javaSet = new java.util.HashSet[Integer]()
      for (value <- set) {
        javaSet.add(value)
      }
      val encoder = new RangedIntegerSetEncoder(obs, lengthTable, min, max)
      obs.writeSet(encoder, encoder, encoder, encoder, javaSet)
    }
  }

  implicit def outputBitStream2RichOutputBitStream(obs: OutputBitStream) :RichOutputBitStream = RichOutputBitStream(obs)

  def writeSymbolArrays(symbolArrays: scala.collection.IndexedSeq[String], obs: OutputBitStream): Unit = {

    // Include the number of symbol arrays
    val symbolArraysLength = symbolArrays.length
    println(s"Exporting all strings ($symbolArraysLength in total)")
    obs.writeNaturalNumber(symbolArraysLength)

    if (symbolArraysLength > 0) {
      val charCountMap = symbolArrays.foldLeft(scala.collection.mutable.Map[Char, Int]()) {
        (map, string) =>
          for (char <- string) {
            map(char) = map.getOrElse(char, 0) + 1
          }
          map
      }.toMap

      val nat3Table = new NaturalNumberHuffmanTable(3)
      val nat4Table = new NaturalNumberHuffmanTable(4)

      // Include charSet Huffman table
      val huffmanTable = DefinedHuffmanTable.withFrequencies(
        scala.collection.JavaConverters.mapAsJavaMap(
          charCountMap.mapValues(Integer.valueOf)), Character.compare _)
      obs.writeHuffmanTable[Char](huffmanTable,
        ch => obs.writeNaturalNumber(ch),
        (prev, elem) => obs.writeHuffmanSymbol[Integer](nat4Table, elem - prev - 1))

      // Include suitable bit alignment for symbolArray lengths Huffman table
      val symbolArrayLengthFreqMap = symbolArrays.foldLeft(new java.util.HashMap[Int, java.lang.Integer]()) { case (map, array) =>
        val arrayLength = array.length
        val newValue = if (map.containsKey(arrayLength)) map.get(arrayLength) + 1 else 1
        map.put(arrayLength, newValue)
        map
      }
      val symbolArrayLengthHuffmanTable = DefinedHuffmanTable.withFrequencies(symbolArrayLengthFreqMap, Integer.compare _)
      obs.writeHuffmanTable[Int](symbolArrayLengthHuffmanTable, length => obs.writeNaturalNumber(length), (prev, elem) => obs.writeHuffmanSymbol[Integer](nat3Table, elem - prev - 1))

      // Include all symbol arrays
      for (array <- symbolArrays) {
        obs.writeHuffmanSymbol(symbolArrayLengthHuffmanTable, array.length)
        for (ch <- array) {
          obs.writeHuffmanSymbol(huffmanTable, ch)
        }
      }
    }
  }

  private def writeLanguagesAndAlphabets(
      languages: scala.collection.Seq[Main.Language],
      alphabets: scala.collection.Seq[Int],
      symbolArrayTable: RangedIntegerHuffmanTable,
      symbolArrays: scala.collection.IndexedSeq[String],
      obs: OutputBitStream): Unit = {
    val nat2Table = new NaturalNumberHuffmanTable(2)
    val languagesLength = languages.size
    obs.writeNaturalNumber(languagesLength)
    var baseAlphabetCounter = StreamedDatabaseConstants.minValidAlphabet
    for (language <- languages) {
      val symbolArrayIndex = symbolArrays.indexOf(language.code)
      obs.writeHuffmanSymbol[Integer](symbolArrayTable, symbolArrayIndex)
      val alphabetCount = language.alphabets.size

      if (language.alphabets.min != baseAlphabetCounter && language.alphabets.max != baseAlphabetCounter + alphabetCount - 1) {
        throw new AssertionError(s"Invalid alphabets for language '${language.code}'")
      }
      obs.writeHuffmanSymbol[Integer](nat2Table, alphabetCount)
      baseAlphabetCounter += alphabetCount
    }
  }

  private def writeConversions(
      conversions: scala.collection.Set[Conversion],
      maxValidAlphabet: Int,
      symbolArrayTable: HuffmanTable[Integer],
      obs: OutputBitStream): Unit = {
    val sortedConversions = conversions.toList.sortWith((a,b) => a.sourceAlphabet < b.sourceAlphabet ||
      a.sourceAlphabet == b.sourceAlphabet && a.targetAlphabet < b.targetAlphabet)

    obs.writeNaturalNumber(sortedConversions.size)
    var minSourceAlphabet = minValidAlphabet
    var minTargetAlphabet = minValidAlphabet
    for (conv <- sortedConversions) {
      val sourceAlphabet = conv.sourceAlphabet
      val sourceAlphabetTable = new RangedIntegerHuffmanTable(minSourceAlphabet, maxValidAlphabet)
      obs.writeHuffmanSymbol[Integer](sourceAlphabetTable, sourceAlphabet)

      if (minSourceAlphabet != sourceAlphabet) {
        minTargetAlphabet = minValidAlphabet
        minSourceAlphabet = sourceAlphabet
      }

      val targetAlphabet = conv.targetAlphabet
      val targetAlphabetTable = new RangedIntegerHuffmanTable(minTargetAlphabet, maxValidAlphabet)
      obs.writeHuffmanSymbol[Integer](targetAlphabetTable, targetAlphabet)
      minTargetAlphabet = targetAlphabet + 1

      val pairCount = conv.sources.length
      obs.writeNaturalNumber(pairCount)
      for (i <- 0 until pairCount) {
        obs.writeHuffmanSymbol[Integer](symbolArrayTable, conv.sources(i))
        obs.writeHuffmanSymbol[Integer](symbolArrayTable, conv.targets(i))
      }
    }
  }

  private def writeAcceptations(
      bufferSet: BufferSet,
      wordTable: => RangedIntegerHuffmanTable,
      conceptTable: => RangedIntegerHuffmanTable,
      symbolArrayTable: RangedIntegerHuffmanTable,
      minAlphabet: Int,
      maxAlphabet: Int,
      obs: OutputBitStream): Unit = {

    // Export correlations
    val correlationsLength = bufferSet.correlations.length
    obs.writeNaturalNumber(correlationsLength)

    if (correlationsLength > 0) {
      val correlationTable = new RangedIntegerHuffmanTable(0, correlationsLength - 1)
      val mapLengthFrequency = new java.util.HashMap[Integer, Integer]()
      for (correlation <- bufferSet.correlations) {
        val length = correlation.size
        var amount: Integer = mapLengthFrequency.get(length)
        if (amount == null) {
          amount = 0
        }

        mapLengthFrequency.put(length, amount + 1)
      }

      val intEncoder = new IntegerEncoder(obs)
      val correlationLengthTable = DefinedHuffmanTable.withFrequencies(mapLengthFrequency, intEncoder)
      obs.writeHuffmanTable[Integer](correlationLengthTable, intEncoder, intEncoder)

      for (correlation <- bufferSet.correlations) {
        val keyEncoder = new RangedIntegerSetEncoder(obs, correlationLengthTable, minAlphabet, maxAlphabet)
        val javaMap = new java.util.HashMap[Integer, Integer]()
        for (pair <- correlation) {
          javaMap.put(pair._1, pair._2)
        }
        obs.writeMap[Integer, Integer](keyEncoder, keyEncoder, keyEncoder, keyEncoder, item => obs.writeHuffmanSymbol(symbolArrayTable, item), javaMap)
      }

      // Export correlation arrays
      val correlationArraysLength = bufferSet.correlationArrays.length
      obs.writeNaturalNumber(correlationArraysLength)

      if (correlationArraysLength > 0) {
        val correlationArrayTable = new RangedIntegerHuffmanTable(0, correlationArraysLength - 1)

        mapLengthFrequency.clear()
        for (correlationArray <- bufferSet.correlationArrays) {
          val length = correlationArray.size
          var amount: Integer = mapLengthFrequency.get(length)
          if (amount == null) {
            amount = 0
          }

          mapLengthFrequency.put(length, amount + 1)
        }

        val corrArrayLengthTable = DefinedHuffmanTable.withFrequencies[Integer](mapLengthFrequency, (a, b) => Integer.compare(a, b))
        obs.writeHuffmanTable[Integer](corrArrayLengthTable, intEncoder, intEncoder)

        for (correlationArray <- bufferSet.correlationArrays) {
          obs.writeHuffmanSymbol[Integer](corrArrayLengthTable, correlationArray.size)
          for (item <- correlationArray) {
            obs.writeHuffmanSymbol[Integer](correlationTable, item)
          }
        }

        // Export acceptations
        val acceptationsLength = bufferSet.newAcceptations.length
        obs.writeNaturalNumber(acceptationsLength)

        for (acc <- bufferSet.newAcceptations) {
          obs.writeHuffmanSymbol[Integer](wordTable, acc.word)
          obs.writeHuffmanSymbol[Integer](conceptTable, acc.concept)
          obs.writeHuffmanSymbol[Integer](correlationArrayTable, acc.correlation)
        }
      }
    }
  }

  def writeBunchAcceptations(
      bufferSet: BufferSet,
      conceptTable: RangedIntegerHuffmanTable,
      minAcceptation: Int,
      maxAcceptation: Int,
      obs: OutputBitStream): Unit = {
    val bunchAcceptations = bufferSet.bunchNewAcceptations
    val bunchAcceptationsLength = bunchAcceptations.size
    obs.writeNaturalNumber(bunchAcceptationsLength)

    val acceptationSetLengthTable = if (bunchAcceptationsLength > 0) {
      val bunchAcceptationsLengthFrecMap = new java.util.HashMap[Integer, Integer]()
      for ((_, accs) <- bunchAcceptations) {
        val length = accs.size
        val currentCount = bunchAcceptationsLengthFrecMap.getOrDefault(length, 0)
        bunchAcceptationsLengthFrecMap.put(length, currentCount + 1)
      }

      val intEncoder = new IntegerEncoder(obs)
      val table = DefinedHuffmanTable.withFrequencies(bunchAcceptationsLengthFrecMap, intEncoder)
      obs.writeHuffmanTable(table, intEncoder, intEncoder)
      table
    }
    else null

    for ((bunch, accs) <- bunchAcceptations) {
      obs.writeHuffmanSymbol[Integer](conceptTable, bunch)
      obs.writeRangedNumberSet(acceptationSetLengthTable, minAcceptation, maxAcceptation, accs)
    }
  }

  def write(bufferSet: BufferSet, obs: OutputBitStream): Unit = {
    val symbolArraysLength = bufferSet.symbolArrays.length
    writeSymbolArrays(bufferSet.symbolArrays, obs)

    // Export languages
    val symbolArrayTable = new RangedIntegerHuffmanTable(0, symbolArraysLength - 1)
    writeLanguagesAndAlphabets(bufferSet.languages, bufferSet.alphabets, symbolArrayTable, bufferSet.symbolArrays, obs)

    // Export conversions
    val maxValidAlphabet = minValidAlphabet + bufferSet.alphabets.size - 1
    writeConversions(bufferSet.conversions, maxValidAlphabet, symbolArrayTable, obs)

    // Export the amount of words and concepts in order to range integers
    val (maxWord, maxConcept) = bufferSet.maxWordAndConceptIndexes
    lazy val wordTable = new RangedIntegerHuffmanTable(minValidWord, maxWord)
    lazy val conceptTable = new RangedIntegerHuffmanTable(minValidConcept, maxConcept)
    obs.writeNaturalNumber(maxWord + 1)
    obs.writeNaturalNumber(maxConcept + 1)

    // Export acceptations
    writeAcceptations(bufferSet, wordTable, conceptTable, symbolArrayTable, minValidAlphabet, maxValidAlphabet, obs)

    // Export bunchConcepts
    val bunchConceptsLength = bufferSet.bunchConcepts.size
    obs.writeNaturalNumber(bunchConceptsLength)

    val bunchConceptsLengthTable = if (bunchConceptsLength > 0) {
      val bunchConceptsLengthFrecMap = new java.util.HashMap[Integer, Integer]()
      for ((_, concepts) <- bufferSet.bunchConcepts) {
        val length = concepts.size
        val currentCount: Int = bunchConceptsLengthFrecMap.getOrDefault(length, 0)
        bunchConceptsLengthFrecMap.put(length, currentCount + 1)
      }

      val table = DefinedHuffmanTable.withFrequencies[Integer](bunchConceptsLengthFrecMap, (x, y) => Integer.compare(x, y))
      obs.writeHuffmanTable[Integer](table, symbol => obs.writeNaturalNumber(symbol), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))
      table
    }
    else null

    for ((bunch, concepts) <- bufferSet.bunchConcepts) {
      obs.writeHuffmanSymbol[Integer](conceptTable, bunch)
      obs.writeRangedNumberSet(bunchConceptsLengthTable, minValidConcept, maxConcept, concepts)
    }

    // Export bunchAcceptations
    writeBunchAcceptations(bufferSet, conceptTable, 0, bufferSet.newAcceptations.size - 1, obs)

    def listOrder(a: List[Int], b: List[Int]): Boolean = {
      b.nonEmpty && (a.isEmpty || a.nonEmpty && (a.head < b.head || a.head == b.head && listOrder(a.tail, b.tail)))
    }

    // Export agents
    val sortedAgents = bufferSet.agents.toVector.sortWith { (a1, a2) =>
      a1.targetBunch < a2.targetBunch ||
      a1.targetBunch == a2.targetBunch && {
        val a1sources = a1.sourceBunches.toList.sorted
        val a2sources = a2.sourceBunches.toList.sorted
        listOrder(a1sources, a2sources)
      }
    }

    val agentsLength = sortedAgents.size
    obs.writeNaturalNumber(agentsLength)
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)

      val sourceSetLengthFreqMap = new java.util.HashMap[Integer, Integer]()
      for (agent <- sortedAgents) {
        val sourceLength = agent.sourceBunches.size
        val sourceValue = sourceSetLengthFreqMap.getOrDefault(sourceLength, 0)
        sourceSetLengthFreqMap.put(sourceLength, sourceValue + 1)
      }

      val sourceSetLengthTable = DefinedHuffmanTable.withFrequencies[Integer](sourceSetLengthFreqMap, (x,y) => Integer.compare(x,y))
      obs.writeHuffmanTable[Integer](sourceSetLengthTable, l => obs.writeHuffmanSymbol(nat3Table, l), null)

      val correlationTable = new RangedIntegerHuffmanTable(0, bufferSet.correlations.length - 1)
      val nullCorrelation = bufferSet.addCorrelation(Map())

      var lastTarget = StreamedDatabaseConstants.nullBunchId
      var minSource = StreamedDatabaseConstants.minValidConcept
      for (agent <- sortedAgents) {
        val newTarget = agent.targetBunch
        val targetTable = new RangedIntegerHuffmanTable(lastTarget, maxConcept)
        obs.writeHuffmanSymbol[Integer](targetTable, newTarget)

        if (newTarget != lastTarget) {
          minSource = StreamedDatabaseConstants.minValidConcept
        }

        val sourceBunches = agent.sourceBunches
        obs.writeRangedNumberSet(sourceSetLengthTable, minSource, maxConcept, sourceBunches)
        minSource = {
          if (sourceBunches.isEmpty) minSource
          else sourceBunches.min
        }

        obs.writeHuffmanSymbol[Integer](correlationTable, agent.matcher)
        obs.writeHuffmanSymbol[Integer](correlationTable, agent.adder)

        if (agent.adder != nullCorrelation) {
          obs.writeHuffmanSymbol[Integer](conceptTable, agent.rule)
        }

        if (agent.matcher != nullCorrelation || agent.adder != nullCorrelation) {
          obs.writeBoolean(agent.fromStart)
        }

        lastTarget = newTarget
      }
    }

    // Export ruleConcepts
    //
    // Table that relates concept when the rule in agents are applied.
    // This is required within the database but impossible here.
    // Conversion done within this project always creates agents with
    // the targetBunch to null. Thus there is no list in the resulting
    // database that include muted words, whose acceptations would
    // include concepts where rules are applied. Then, while all targets
    // bunches in agents are null, this table will be always empty.
    obs.writeNaturalNumber(0)

    obs.close()
  }

  def write(bufferSet: BufferSet, fileName: String): Unit = {
    val file = new File(fileName)
    val fos = new FileOutputStream(file)
    try {
      val header = Array(
        'S'.toByte, 'D'.toByte, 'B'.toByte, 0.toByte)

      fos.write(header)
      fos.write(Array.ofDim[Byte](16))

      val md = MessageDigest.getInstance("MD5")
      val dos = new DigestOutputStream(fos, md)
      val obs = new OutputBitStream(dos)
      try {
        write(bufferSet, obs)
      }
      finally {
        try {
          obs.close()
        }
        catch {
          case _: IOException => // Nothing to worry
        }

        try {
          fos.close()
        }
        catch {
          case _: IOException => // Nothing to worry
        }
      }

      // Adding the MD5 hash
      val raf = new RandomAccessFile(file, "rw")
      raf.write(header)
      raf.write(md.digest())
      raf.close()
    }
    finally {
      try {
        fos.close()
      }
      catch {
        case _: IOException => // Nothing to be done
      }
    }
  }
}
