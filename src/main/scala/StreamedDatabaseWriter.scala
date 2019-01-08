import java.io.{File, FileOutputStream, IOException, RandomAccessFile}
import java.security.{DigestOutputStream, MessageDigest}
import java.util.Comparator

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
      val intEncoder = new IntegerEncoder(obs)
      val symbolArrayLengthHuffmanTable = generateHuffmanTable[String, Integer](symbolArrays, _.length, intEncoder)
      obs.writeHuffmanTable[Integer](symbolArrayLengthHuffmanTable, length => obs.writeNaturalNumber(length), (prev, elem) => obs.writeHuffmanSymbol[Integer](nat3Table, elem - prev - 1))

      // Include all symbol arrays
      for (array <- symbolArrays) {
        obs.writeHuffmanSymbol[Integer](symbolArrayLengthHuffmanTable, array.length)
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
    println(s"Exporting languages ($languagesLength in total)")

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

    println(s"Exporting conversions (${sortedConversions.size} in total)")
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

  private class IteratorMapper[A,B](collection: Iterable[A], mapFunc: A => B) extends java.util.Iterator[B] {
    val it = collection.iterator
    override def next() = mapFunc(it.next())
    override def hasNext = it.hasNext
  }

  private class IterableMapper[A,B](collection: Iterable[A], mapFunc: A => B) extends java.lang.Iterable[B] {
    override def iterator = new IteratorMapper(collection, mapFunc)
  }

  private def generateHuffmanTable[A,B](collection: Iterable[A], mapFunc: A => B, comparator: Comparator[B]): DefinedHuffmanTable[B] = {
    val correlationLengthIterable = new IterableMapper[A, B](collection, mapFunc)
    DefinedHuffmanTable.from(correlationLengthIterable, comparator)
  }

  private def writeAcceptations(
      bufferSet: BufferSet,
      conceptTable: => RangedIntegerHuffmanTable,
      symbolArrayTable: RangedIntegerHuffmanTable,
      minAlphabet: Int,
      maxAlphabet: Int,
      obs: OutputBitStream): Unit = {

    // Export correlations
    val correlationsLength = bufferSet.correlations.length
    println(s"Exporting correlations ($correlationsLength in total)")
    obs.writeNaturalNumber(correlationsLength)

    if (correlationsLength > 0) {
      val correlationTable = new RangedIntegerHuffmanTable(0, correlationsLength - 1)
      val intEncoder = new IntegerEncoder(obs)
      val correlationLengthTable = generateHuffmanTable[BufferSet.Correlation, Integer](bufferSet.correlations, _.size, intEncoder)
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
      println(s"Exporting correlation arrays ($correlationArraysLength in total)")
      obs.writeNaturalNumber(correlationArraysLength)

      if (correlationArraysLength > 0) {
        val corrArrayLengthTable = generateHuffmanTable[Seq[Int], Integer](bufferSet.correlationArrays, _.size, intEncoder)
        obs.writeHuffmanTable[Integer](corrArrayLengthTable, intEncoder, intEncoder)

        for (correlationArray <- bufferSet.correlationArrays) {
          obs.writeHuffmanSymbol[Integer](corrArrayLengthTable, correlationArray.size)
          for (item <- correlationArray) {
            obs.writeHuffmanSymbol[Integer](correlationTable, item)
          }
        }

        // Export acceptations
        val acceptationsLength = bufferSet.acceptations.length
        println(s"Exporting acceptations ($acceptationsLength in total)")
        obs.writeNaturalNumber(acceptationsLength)

        if (acceptationsLength > 0) {
          val corrArraySetLengthTable = generateHuffmanTable[Int, Integer](
            bufferSet.acceptations.indices,
            index => bufferSet.acceptationCorrelations.getOrElse(index, Set[Int]()).size,
            intEncoder)
          obs.writeHuffmanTable[Integer](corrArraySetLengthTable, intEncoder, intEncoder)

          for (i <- bufferSet.acceptations.indices) {
            val acc = bufferSet.acceptations(i)
            obs.writeHuffmanSymbol[Integer](conceptTable, acc.concept)

            val set = bufferSet.acceptationCorrelations.getOrElse(i, Set[Int]())
            obs.writeRangedNumberSet(corrArraySetLengthTable, 0, correlationArraysLength - 1, set)
          }
        }
      }
    }
  }

  def writeBunchAcceptations(
      bufferSet: BufferSet,
      minValidConcept: Int,
      maxValidConcept: Int,
      minAcceptation: Int,
      maxAcceptation: Int,
      obs: OutputBitStream): Unit = {
    val bunchAcceptations = bufferSet.bunchAcceptations
    val bunchAcceptationsLength = bunchAcceptations.size
    println(s"Exporting bunch acceptations ($bunchAcceptationsLength in total)")
    obs.writeNaturalNumber(bunchAcceptationsLength)

    val acceptationSetLengthTable = if (bunchAcceptationsLength > 0) {
      val natEncoder = new NaturalEncoder(obs)
      val table = generateHuffmanTable[(Int,Set[Int]), Integer](bunchAcceptations, _._2.size, natEncoder)
      obs.writeHuffmanTable(table, natEncoder, natEncoder)
      table
    }
    else null

    var remainingBunches = bunchAcceptationsLength
    var minBunchConcept = minValidConcept
    for ((bunch, accs) <- bunchAcceptations.toSeq.sortWith(_._1 < _._1)) {
      val table = new RangedIntegerHuffmanTable(minBunchConcept, maxValidConcept - remainingBunches + 1)
      obs.writeHuffmanSymbol[Integer](table, bunch)
      minBunchConcept = bunch + 1
      remainingBunches -= 1
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
    val (_, maxConcept) = bufferSet.maxWordAndConceptIndexes
    lazy val conceptTable = new RangedIntegerHuffmanTable(minValidConcept, maxConcept)
    obs.writeNaturalNumber(maxConcept + 1)

    // Export acceptations
    writeAcceptations(bufferSet, conceptTable, symbolArrayTable, minValidAlphabet, maxValidAlphabet, obs)

    // Export bunchConcepts
    val bunchConceptsLength = bufferSet.bunchConcepts.size
    println(s"Exporting bunch concepts ($bunchConceptsLength in total)")
    obs.writeNaturalNumber(bunchConceptsLength)

    val bunchConceptsLengthTable = if (bunchConceptsLength > 0) {
      val natEncoder = new NaturalEncoder(obs)
      val table = generateHuffmanTable[(Int, Set[Int]), Integer](bufferSet.bunchConcepts, _._2.size, natEncoder)
      obs.writeHuffmanTable[Integer](table, natEncoder, natEncoder)
      table
    }
    else null

    var remainingBunches = bunchConceptsLength
    var minBunchConcept = minValidConcept
    for ((bunch, concepts) <- bufferSet.bunchConcepts.toSeq.sortWith(_._1 < _._1)) {
      val table = new RangedIntegerHuffmanTable(minBunchConcept, maxConcept - remainingBunches + 1)
      obs.writeHuffmanSymbol[Integer](table, bunch)
      minBunchConcept = bunch + 1
      remainingBunches -= 1

      obs.writeRangedNumberSet(bunchConceptsLengthTable, minValidConcept, maxConcept, concepts)
    }

    // Export bunchAcceptations
    writeBunchAcceptations(bufferSet, minValidConcept, maxConcept, 0, bufferSet.acceptations.size - 1, obs)

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
    println(s"Exporting agents ($agentsLength in total)")
    obs.writeNaturalNumber(agentsLength)
    if (agentsLength > 0) {
      val nat3Table = new NaturalNumberHuffmanTable(3)
      val intEncoder = new IntegerEncoder(obs)
      val sourceSetLengthTable = generateHuffmanTable[Agent, Integer](sortedAgents, _.sourceBunches.size, intEncoder)
      obs.writeHuffmanTable[Integer](sourceSetLengthTable, l => obs.writeHuffmanSymbol(nat3Table, l), null)

      val nullCorrelation = bufferSet.addCorrelation(Map())
      val correlationTable = new RangedIntegerHuffmanTable(0, bufferSet.correlations.length - 1)

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

        val correlations = {
          if (agent.matcher == nullCorrelation && agent.adder == nullCorrelation) {
            Vector(nullCorrelation, nullCorrelation, nullCorrelation, nullCorrelation)
          }
          else if (agent.fromStart) {
            Vector(agent.matcher, agent.adder, nullCorrelation, nullCorrelation)
          }
          else {
            Vector(nullCorrelation, nullCorrelation, agent.matcher, agent.adder)
          }
        }

        for (correlation <- correlations) {
          obs.writeHuffmanSymbol[Integer](correlationTable, correlation)
        }

        if (agent.matcher != agent.adder) {
          obs.writeHuffmanSymbol[Integer](conceptTable, agent.rule)
        }

        lastTarget = newTarget
      }
    }

    // Export relevant dynamic acceptations - None to be added here
    obs.writeNaturalNumber(0)

    // Export sentence spans - None to be added here
    obs.writeNaturalNumber(0)

    // Export sentence meanings - None to be added here
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
