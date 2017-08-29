import java.io.{File, FileOutputStream, IOException, RandomAccessFile}
import java.security.{DigestOutputStream, MessageDigest}

import BufferSet.Correlation
import StreamedDatabaseConstants.{minValidAlphabet, minValidConcept, minValidWord}
import sword.bitstream.{DefinedHuffmanTable, NaturalNumberHuffmanTable, OutputBitStream}

object StreamedDatabaseWriter {

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
      obs.writeHuffmanTable[Char](huffmanTable, obs.writeChar _, (prev, elem) => obs.writeHuffmanSymbol[java.lang.Long](nat4Table, (elem - prev - 1).toLong))

      // Include suitable bit alignment for symbolArray lengths Huffman table
      val symbolArrayLengthFreqMap = symbolArrays.foldLeft(new java.util.HashMap[Int, java.lang.Integer]()) { case (map, array) =>
        val arrayLength = array.length
        val newValue = if (map.containsKey(arrayLength)) map.get(arrayLength) + 1 else 1
        map.put(arrayLength, newValue)
        map
      }
      val symbolArrayLengthHuffmanTable = DefinedHuffmanTable.withFrequencies(symbolArrayLengthFreqMap, Integer.compare _)
      obs.writeHuffmanTable[Int](symbolArrayLengthHuffmanTable, length => obs.writeNaturalNumber(length), (prev, elem) => obs.writeHuffmanSymbol[java.lang.Long](nat3Table, (elem - prev - 1).toLong))

      // Include all symbol arrays
      for (array <- symbolArrays) {
        obs.writeHuffmanSymbol(symbolArrayLengthHuffmanTable, array.length)
        for (ch <- array) {
          obs.writeHuffmanSymbol(huffmanTable, ch)
        }
      }
    }
  }

  def write(bufferSet: BufferSet, obs: OutputBitStream): Unit = {
    val symbolArraysLength = bufferSet.symbolArrays.length
    writeSymbolArrays(bufferSet.symbolArrays, obs)

    // Export languages
    val nat2Table = new NaturalNumberHuffmanTable(2)
    val languagesLength = bufferSet.languages.size
    obs.writeNaturalNumber(languagesLength)
    var baseAlphabetCounter = StreamedDatabaseConstants.minValidAlphabet
    for (language <- bufferSet.languages) {
      val symbolArrayIndex = bufferSet.symbolArrays.indexOf(language.code)
      obs.writeRangedNumber(0, symbolArraysLength - 1, symbolArrayIndex)
      val alphabetCount = language.alphabets.size

      if (language.alphabets.min != baseAlphabetCounter && language.alphabets.max != baseAlphabetCounter + alphabetCount - 1) {
        throw new AssertionError(s"Invalid alphabets for language '${language.code}'")
      }
      obs.writeHuffmanSymbol[java.lang.Long](nat2Table, alphabetCount.toLong)
      baseAlphabetCounter += alphabetCount
    }

    // Export the amount of words and concepts in order to range integers
    val (maxWord, maxConcept) = bufferSet.maxWordAndConceptIndexes
    obs.writeNaturalNumber(maxWord + 1)
    obs.writeNaturalNumber(maxConcept + 1)

    // Export acceptations
    val acceptationsLength = bufferSet.acceptations.length
    println(s"Exporting acceptations ($acceptationsLength in total)")
    obs.writeNaturalNumber(acceptationsLength)
    for (acc <- bufferSet.acceptations) {
      obs.writeRangedNumber(minValidWord, maxWord, acc.word)
      obs.writeRangedNumber(minValidConcept, maxConcept, acc.concept)
    }

    // Export word representations
    var wordRepresentationLength = 0
    for (repr <- bufferSet.wordRepresentations) {
      if (repr.word >= minValidWord && repr.alphabet >= minValidAlphabet && repr.symbolArray >= 0) {
        wordRepresentationLength += 1
      }
    }

    val maxValidAlphabet = minValidAlphabet + bufferSet.alphabets.size - 1
    println(s"Exporting word representations ($wordRepresentationLength in total)")
    obs.writeNaturalNumber(wordRepresentationLength)
    for (repr <- bufferSet.wordRepresentations) {
      if (repr.word >= minValidWord && repr.alphabet >= minValidAlphabet && repr.symbolArray >= 0) {
        obs.writeRangedNumber(minValidWord, maxWord, repr.word)
        obs.writeRangedNumber(minValidAlphabet, maxValidAlphabet, repr.alphabet)
        obs.writeRangedNumber(0, symbolArraysLength - 1, repr.symbolArray)
      }
    }

    // Export kanji-kana correlations
    val kanjiKanaCorrelationsLength = bufferSet.kanjiKanaCorrelations.length
    obs.writeNaturalNumber(kanjiKanaCorrelationsLength)
    for ((kanji, kana) <- bufferSet.kanjiKanaCorrelations) {
      obs.writeRangedNumber(0, symbolArraysLength - 1, kanji)
      obs.writeRangedNumber(0, symbolArraysLength - 1, kana)
    }

    // Export jaWordCorrelations
    val jaWordCorrelationsLength = bufferSet.jaWordCorrelations.size
    obs.writeNaturalNumber(jaWordCorrelationsLength)

    if (jaWordCorrelationsLength > 0) {
      val (
        correlationReprCountHuffmanTable,
        correlationConceptCountHuffmanTable,
        correlationVectorLengthHuffmanTable
      ) = {
        val reprCount = new java.util.HashMap[Int, Integer]()
        val lengths = new java.util.HashMap[Int, Integer]()
        val conceptCount = new java.util.HashMap[Int, Integer]()

        for ((_, set) <- bufferSet.jaWordCorrelations) {
          val reprCountKey = set.size
          if (reprCountKey == 0) {
            throw new AssertionError("Not expected to find an empty set")
          }

          val reprCountValue = if (reprCount.containsKey(reprCountKey)) reprCount.get(reprCountKey).intValue() else 0
          reprCount.put(reprCountKey, reprCountValue + 1)

          for ((conceptSet, corrArray) <- set) {
            val conceptCountKey = conceptSet.size
            if (conceptCountKey == 0) {
              throw new AssertionError("Not expected to find an empty set")
            }
            val conceptCountValue = if (conceptCount.containsKey(conceptCountKey)) conceptCount.get(conceptCountKey).intValue() else 0
            conceptCount.put(conceptCountKey, conceptCountValue + 1)

            val lengthKey = corrArray.length
            if (lengthKey == 0) {
              throw new AssertionError("Not expected to find an empty Vector")
            }
            val lengthValue = if (lengths.containsKey(lengthKey)) lengths.get(lengthKey).intValue() else 0
            lengths.put(lengthKey, lengthValue + 1)
          }
        }

        (
          DefinedHuffmanTable.withFrequencies(reprCount, Integer.compare _),
          DefinedHuffmanTable.withFrequencies(conceptCount, Integer.compare _),
          DefinedHuffmanTable.withFrequencies(lengths, Integer.compare _)
        )
      }

      obs.writeHuffmanTable[Int](correlationReprCountHuffmanTable, symbol => obs.writeNaturalNumber(symbol), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))
      obs.writeHuffmanTable[Int](correlationConceptCountHuffmanTable, symbol => obs.writeNaturalNumber(symbol), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))
      obs.writeHuffmanTable[Int](correlationVectorLengthHuffmanTable, symbol => obs.writeNaturalNumber(symbol), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))

      for ((wordId, set) <- bufferSet.jaWordCorrelations) {
        obs.writeRangedNumber(minValidWord, maxWord, wordId)
        obs.writeHuffmanSymbol(correlationReprCountHuffmanTable, set.size)

        for ((conceptSet, corrArray) <- set) {
          obs.writeHuffmanSymbol(correlationConceptCountHuffmanTable, conceptSet.size)
          for (concept <- conceptSet) {
            obs.writeRangedNumber(minValidConcept, maxConcept, concept)
          }

          obs.writeHuffmanSymbol(correlationVectorLengthHuffmanTable, corrArray.length)
          for (corr <- corrArray) {
            obs.writeRangedNumber(0, kanjiKanaCorrelationsLength - 1, corr)
          }
        }
      }
    }

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
      obs.writeHuffmanTable[Integer](table, symbol => obs.writeNaturalNumber(symbol.toLong), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))
      table
    }
    else null

    for ((bunch, concepts) <- bufferSet.bunchConcepts) {
      val javaSet = new java.util.HashSet[Integer]()
      for (concept <- concepts) {
        javaSet.add(concept)
      }

      obs.writeRangedNumber(minValidConcept, maxConcept, bunch)
      obs.writeRangedNumberSet(bunchConceptsLengthTable, minValidConcept, maxConcept, javaSet)
    }

    // Export bunchAcceptations
    val bunchAcceptationsLength = bufferSet.bunchAcceptations.size
    obs.writeNaturalNumber(bunchAcceptationsLength)

    val bunchAcceptationsLengthTable = if (bunchAcceptationsLength > 0) {
      val bunchAcceptationsLengthFrecMap = new java.util.HashMap[Integer, Integer]()
      for ((_, accs) <- bufferSet.bunchAcceptations) {
        val length = accs.size
        val currentCount = bunchAcceptationsLengthFrecMap.getOrDefault(length, 0)
        bunchAcceptationsLengthFrecMap.put(length, currentCount + 1)
      }

      val table = DefinedHuffmanTable.withFrequencies[Integer](bunchAcceptationsLengthFrecMap, (x, y) => Integer.compare(x, y))
      obs.writeHuffmanTable[Integer](table, symbol => obs.writeNaturalNumber(symbol.toLong), (prev, elem) => obs.writeNaturalNumber(elem - prev - 1))
      table
    }
    else null

    for ((bunch, accs) <- bufferSet.bunchAcceptations) {
      val javaSet = new java.util.HashSet[Integer]()
      for (acc <- accs) {
        javaSet.add(acc)
      }

      obs.writeRangedNumber(minValidConcept, maxConcept, bunch)
      obs.writeRangedNumberSet(bunchAcceptationsLengthTable, 0, acceptationsLength - 1, javaSet)
    }

    def listOrder(a: List[Int], b: List[Int]): Boolean = {
      a.isEmpty || b.nonEmpty && (a.head < b.head || a.head == b.head && listOrder(a.tail, b.tail))
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
      val matcherSetLengthFreqMap = new java.util.HashMap[java.lang.Long, Integer]()
      for (agent <- sortedAgents) {
        val sourceLength = agent.sourceBunches.size
        val sourceValue = sourceSetLengthFreqMap.getOrDefault(sourceLength, 0)
        sourceSetLengthFreqMap.put(sourceLength, sourceValue + 1)

        val matcherLength = agent.matcher.size.toLong
        val matcherValue = matcherSetLengthFreqMap.getOrDefault(matcherLength, 0)
        matcherSetLengthFreqMap.put(matcherLength, matcherValue + 1)

        val adderLength = agent.adder.size.toLong
        val adderValue = matcherSetLengthFreqMap.getOrDefault(adderLength, 0)
        matcherSetLengthFreqMap.put(adderLength, adderValue + 1)
      }
      val sourceSetLengthTable = DefinedHuffmanTable.withFrequencies[Integer](sourceSetLengthFreqMap, (x,y) => Integer.compare(x,y))
      val matcherSetLengthTable = DefinedHuffmanTable.withFrequencies[java.lang.Long](matcherSetLengthFreqMap, (x,y) => java.lang.Long.compare(x,y))

      obs.writeHuffmanTable[Integer](sourceSetLengthTable, l => obs.writeHuffmanSymbol[java.lang.Long](nat3Table, l.toLong), null)
      obs.writeHuffmanTable[java.lang.Long](matcherSetLengthTable, l => obs.writeHuffmanSymbol(nat3Table, l), null)

      var lastTarget = StreamedDatabaseConstants.nullBunchId
      var minSource = StreamedDatabaseConstants.minValidConcept
      for (agent <- sortedAgents) {
        val newTarget = agent.targetBunch
        obs.writeRangedNumber(lastTarget, maxConcept, newTarget)

        if (newTarget != lastTarget) {
          minSource = StreamedDatabaseConstants.minValidConcept
        }

        val sourceBunches = agent.sourceBunches
        val valueSet = new java.util.HashSet[Integer]()
        var newMinSource = {
          if (sourceBunches.isEmpty) minSource
          else sourceBunches.min
        }

        for (value <- sourceBunches) {
          valueSet.add(value)
        }

        obs.writeRangedNumberSet(sourceSetLengthTable, minSource, maxConcept, valueSet)
        minSource = newMinSource

        def writeCorrelationMap(map: Correlation): Unit = {
          val maxAlphabet = bufferSet.alphabets.size - 1
          val mapLength = map.size
          obs.writeHuffmanSymbol[java.lang.Long](matcherSetLengthTable, java.lang.Long.valueOf(mapLength))
          if (mapLength > 0) {
            val list = map.toList.sortWith(_._1 < _._1)
            var minAlphabet = 0
            for ((alphabet, symbolArrayIndex) <- list) {
              obs.writeRangedNumber(minAlphabet, maxAlphabet, alphabet)
              minAlphabet = alphabet + 1

              obs.writeRangedNumber(0, symbolArraysLength - 1, symbolArrayIndex)
            }
          }
        }

        writeCorrelationMap(agent.matcher)
        writeCorrelationMap(agent.adder)

        if (agent.adder.nonEmpty) {
          obs.writeRangedNumber(StreamedDatabaseConstants.minValidConcept, maxConcept, agent.rule)
        }

        if (agent.matcher.nonEmpty || agent.adder.nonEmpty) {
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
