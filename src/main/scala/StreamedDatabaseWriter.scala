import StreamedDatabaseConstants.{maxValidAlphabet, minValidAlphabet, minValidWord}
import sword.bitstream.{DefinedHuffmanTable, NaturalNumberHuffmanTable, OutputBitStream}

import scala.collection.mutable.ArrayBuffer

object StreamedDatabaseWriter {

  def writeSymbolArrays(symbolArrays: scala.collection.IndexedSeq[String], obs: OutputBitStream): Unit = {

    val charCountMap = symbolArrays.foldLeft(scala.collection.mutable.Map[Char, Int]()) {
      (map, string) =>
        for (char <- string) {
          map(char) = map.getOrElse(char, 0) + 1
        }
        map
    }.toMap

    // Include charSet Huffman table
    val huffmanTable = DefinedHuffmanTable.withFrequencies(
      scala.collection.JavaConverters.mapAsJavaMap(
        charCountMap.mapValues(Integer.valueOf)))
    obs.writeHuffmanTable(huffmanTable, ch => obs.writeChar(ch))

    // Include suitable bit alignment for symbolArray lengths Huffman table
    val symbolArrayLengthFreqMap = symbolArrays.foldLeft(new java.util.HashMap[Int, java.lang.Integer]()) { case (map, array) =>
      val arrayLength = array.length
      val newValue = if (map.containsKey(arrayLength)) map.get(arrayLength) + 1 else 1
      map.put(arrayLength, newValue)
      map
    }
    val symbolArrayLengthHuffmanTable = DefinedHuffmanTable.withFrequencies(symbolArrayLengthFreqMap)
    obs.writeHuffmanTable[Int](symbolArrayLengthHuffmanTable, length => obs.writeNaturalNumber(length))

    // Include all symbol arrays
    val symbolArraysLength = symbolArrays.length
    println(s"Exporting all strings ($symbolArraysLength in total)")
    obs.writeNaturalNumber(symbolArraysLength)
    for (array <- symbolArrays) {
      obs.writeHuffmanSymbol(symbolArrayLengthHuffmanTable, array.length)
      for (ch <- array) {
        obs.writeHuffmanSymbol(huffmanTable, ch)
      }
    }
  }

  def write(bufferSet: BufferSet, obs: OutputBitStream) = {
    writeSymbolArrays(bufferSet.symbolArrays, obs)

    // Export the amount of words and concepts in order to range integers
    val (maxWord, maxConcept) = bufferSet.maxWordAndConceptIndexes
    obs.writeNaturalNumber(maxWord)
    obs.writeNaturalNumber(maxConcept)

    // Export acceptations
    val acceptationsLength = bufferSet.acceptations.length
    println(s"Exporting acceptations ($acceptationsLength in total)")
    obs.writeNaturalNumber(acceptationsLength)
    for (acc <- bufferSet.acceptations) {
      obs.writeRangedNumber(minValidWord, maxWord, acc.word)
      obs.writeRangedNumber(minValidWord, maxConcept, acc.concept)
    }

    // Export word representations
    val symbolArraysLength = bufferSet.symbolArrays.length
    val wordRepresentationLength = bufferSet.wordRepresentations.length
    println(s"Exporting word representations ($wordRepresentationLength in total)")
    obs.writeNaturalNumber(wordRepresentationLength)
    for (repr <- bufferSet.wordRepresentations) {
      if (repr.word >= minValidWord && repr.alphabet >= minValidAlphabet && repr.symbolArray >= 0) {
        obs.writeRangedNumber(minValidWord, maxWord, repr.word)
        obs.writeRangedNumber(minValidAlphabet, maxValidAlphabet, repr.alphabet)
        obs.writeRangedNumber(0, symbolArraysLength - 1, repr.symbolArray)
      }
    }

    // Export acceptation representations
    val accRepresentationLength = bufferSet.accRepresentations.length
    println(s"Exporting acceptation representations ($accRepresentationLength in total)")
    obs.writeNaturalNumber(accRepresentationLength)
    for (repr <- bufferSet.accRepresentations) {
      obs.writeRangedNumber(0, acceptationsLength - 1, repr.acc)
      obs.writeRangedNumber(0, symbolArraysLength - 1, repr.symbolArray)
    }

    obs.close()
  }
}
