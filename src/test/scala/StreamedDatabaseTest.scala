import org.scalatest.{FlatSpec, Matchers}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable.ArrayBuffer
import sword.bitstream.{InputBitStream, OutputBitStream}

class StreamedDatabaseTest extends FlatSpec with Matchers {

  behavior of "StreamedDatabaseWriter and StreamedDatabaseReader"

  it should "match on write and read symbol arrays" in {
    val symbolArrays = Vector(
      "first word",
      "second word",
      "third word"
    )

    val baos = new ByteArrayOutputStream
    val obs = new OutputBitStream(baos)

    StreamedDatabaseWriter.writeSymbolArrays(symbolArrays, obs)
    obs.close()

    val array = baos.toByteArray
    val bais = new ByteArrayInputStream(array)
    val ibs = new InputBitStream(bais)

    val readSymbolArrays = new ArrayBuffer[String]()
    StreamedDatabaseReader.readSymbolArrays(readSymbolArrays, ibs)

    readSymbolArrays shouldBe symbolArrays
  }

  private def checkWriteAndRead(sourceSet: BufferSet): Unit = {
    val baos = new ByteArrayOutputStream
    val obs = new OutputBitStream(baos)

    StreamedDatabaseWriter.write(sourceSet, obs)
    obs.close()

    val array = baos.toByteArray
    val bais = new ByteArrayInputStream(array)
    val ibs = new InputBitStream(bais)

    val targetSet = new BufferSet()
    StreamedDatabaseReader.read(targetSet, ibs)

    targetSet shouldBe sourceSet
  }

  it should "match on write and read acceptations and its representations even if only include symbol arrays" in {
    val sourceSet = new BufferSet()

    sourceSet.symbolArrays ++= Vector(
      "importante",
      "cabello",
      "papel",
      "dios"
    )

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read acceptations and its representations" in {
    val sourceSet = new BufferSet()

    val wordBase = StreamedDatabaseConstants.minValidWord
    val conceptBase = StreamedDatabaseConstants.minValidConcept

    sourceSet.symbolArrays ++= Vector(
      "大事", // 0
      "だいじ",
      "大切",
      "たいせつ",
      "importante",
      "髪",  // 5
      "紙",
      "神",
      "かみ",
      "cabello",
      "papel", // 10
      "dios"
    )

    sourceSet.acceptations ++= Vector(
      Acceptation(wordBase + 0, conceptBase + 0), //だいじ
      Acceptation(wordBase + 1, conceptBase + 0), //たいせつ
      Acceptation(wordBase + 2, conceptBase + 0), //importante
      Acceptation(wordBase + 3, conceptBase + 1), //かみ(hear)
      Acceptation(wordBase + 3, conceptBase + 2), //かみ(paper)
      Acceptation(wordBase + 3, conceptBase + 3), //かみ(God)
      Acceptation(wordBase + 4, conceptBase + 1), //cabello
      Acceptation(wordBase + 5, conceptBase + 2), //papel
      Acceptation(wordBase + 6, conceptBase + 3) //Dios
    )

    sourceSet.wordRepresentations ++= Vector(
      WordRepresentation(wordBase + 0, Main.kanjiAlphabet, 0),
      WordRepresentation(wordBase + 0, Main.kanaAlphabet, 1),
      WordRepresentation(wordBase + 1, Main.kanjiAlphabet, 2),
      WordRepresentation(wordBase + 1, Main.kanaAlphabet, 3),
      WordRepresentation(wordBase + 2, Main.esAlphabet, 4),
      WordRepresentation(wordBase + 3, Main.kanaAlphabet, 8),
      WordRepresentation(wordBase + 4, Main.esAlphabet, 9),
      WordRepresentation(wordBase + 5, Main.esAlphabet, 10),
      WordRepresentation(wordBase + 6, Main.esAlphabet, 11)
    )

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read acceptations, its representations and correlations" in {
    val sourceSet = new BufferSet()

    sourceSet.symbolArrays ++= Vector(
      "大事", // 0
      "だいじ",
      "大切",
      "たいせつ",
      "importante",
      "髪",  // 5
      "紙",
      "神",
      "かみ",
      "cabello",
      "papel", // 10
      "dios",
      "大",
      "事",
      "だい",
      "じ", // 15
      "切",
      "たい",
      "せつ"
    )

    val wordBase = StreamedDatabaseConstants.minValidWord
    val conceptBase = StreamedDatabaseConstants.minValidConcept

    sourceSet.acceptations ++= Vector(
      Acceptation(wordBase, conceptBase), //だいじ
      Acceptation(wordBase + 1, conceptBase), //たいせつ
      Acceptation(wordBase + 2, conceptBase), //importante
      Acceptation(wordBase + 3, conceptBase + 1), //かみ(hear)
      Acceptation(wordBase + 3, conceptBase + 2), //かみ(paper)
      Acceptation(wordBase + 3, conceptBase + 3), //かみ(God)
      Acceptation(wordBase + 4, conceptBase + 1), //cabello
      Acceptation(wordBase + 5, conceptBase + 2), //papel
      Acceptation(wordBase + 6, conceptBase + 3) //Dios
    )

    sourceSet.wordRepresentations ++= Vector(
      WordRepresentation(wordBase + 0, Main.kanjiAlphabet, 0),
      WordRepresentation(wordBase + 0, Main.kanaAlphabet, 1),
      WordRepresentation(wordBase + 1, Main.kanjiAlphabet, 2),
      WordRepresentation(wordBase + 1, Main.kanaAlphabet, 3),
      WordRepresentation(wordBase + 2, Main.esAlphabet, 4),
      WordRepresentation(wordBase + 3, Main.kanaAlphabet, 8),
      WordRepresentation(wordBase + 4, Main.esAlphabet, 9),
      WordRepresentation(wordBase + 5, Main.esAlphabet, 10),
      WordRepresentation(wordBase + 6, Main.esAlphabet, 11)
    )

    sourceSet.kanjiKanaCorrelations ++= Vector(
      (12, 14),
      (12, 17),
      (13, 15),
      (16, 18),
      (5, 8),
      (6, 8),
      (7, 8)
    )

    sourceSet.jaWordCorrelations ++= Vector(
      (wordBase, Set((Set(conceptBase), Vector(0, 2)))),
      (wordBase + 1, Set((Set(conceptBase), Vector(1, 3)))),
      (wordBase + 3, Set(
        (Set(conceptBase + 1), Vector(4)),
        (Set(conceptBase + 2), Vector(5)),
        (Set(conceptBase + 3), Vector(6))
      ))
    )

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read concept bunches" in {
    val sourceSet = new BufferSet()

    val (_, lastConcept) = sourceSet.maxWordAndConceptIndexes

    val ratConcept = lastConcept + 1
    val colorConcept = lastConcept + 2
    val redConcept = lastConcept + 3
    val greenConcept = lastConcept + 4
    val blueConcept = lastConcept + 5
    val animalConcept = lastConcept + 6

    sourceSet.bunchConcepts(colorConcept) =
        Set(redConcept, greenConcept, blueConcept)
    sourceSet.bunchConcepts(animalConcept) = Set(ratConcept)

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read acceptation bunches" in {
    val sourceSet = new BufferSet()

    val verbSymbolArray = sourceSet.addSymbolArray("verb")
    val runSymbolArray = sourceSet.addSymbolArray("red")
    val jumpSymbolArray = sourceSet.addSymbolArray("green")

    val countrySymbolArray = sourceSet.addSymbolArray("country")
    val japanSymbolArray = sourceSet.addSymbolArray("japan")

    val (lastWord, lastConcept) = sourceSet.maxWordAndConceptIndexes

    val verbConcept = lastConcept + 1
    val runConcept = lastConcept + 2
    val jumpConcept = lastConcept + 3
    val countryConcept = lastConcept + 4
    val japanConcept = lastConcept + 5

    val verbWord = lastWord + 1
    val runWord = lastWord + 2
    val jumpWord = lastWord + 3
    val countryWord = lastWord + 4
    val japanWord = lastWord + 5

    sourceSet.wordRepresentations ++= Vector(
      WordRepresentation(verbWord, Main.enAlphabet, verbSymbolArray),
      WordRepresentation(runWord, Main.enAlphabet, runSymbolArray),
      WordRepresentation(jumpWord, Main.enAlphabet, jumpSymbolArray),
      WordRepresentation(countryWord, Main.enAlphabet, countrySymbolArray),
      WordRepresentation(japanWord, Main.enAlphabet, japanSymbolArray)
    )

    val accBase = sourceSet.acceptations.size
    sourceSet.acceptations ++= Vector(
      Acceptation(verbWord, verbConcept),
      Acceptation(runWord, runConcept),
      Acceptation(jumpWord, jumpConcept),
      Acceptation(japanWord, japanConcept),
      Acceptation(countryWord, countryConcept)
    )

    sourceSet.bunchAcceptations(verbConcept) = Set(accBase + 1, accBase +2)
    sourceSet.bunchAcceptations(countryConcept) = Set(accBase + 3)

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read agent matching all い ended words" in {
    val bufferSet = new BufferSet()

    val iSymbolArray = bufferSet.addSymbolArray("い")

    var (_, conceptCount) = bufferSet.maxWordAndConceptIndexes
    conceptCount += 1
    val targetBunch = conceptCount

    val matcher = Map(
      Main.kanjiAlphabet -> iSymbolArray
    )

    bufferSet.agents += Agent(
      targetBunch,
      sourceBunches = Set(),
      matcher,
      adder = Map(),
      rule = StreamedDatabaseConstants.nullBunchId,
      fromStart = false
    )

    checkWriteAndRead(bufferSet)
  }

  it should "match on write and read agent collecting all adjective" in {
    val bufferSet = new BufferSet()

    var (_, conceptCount) = bufferSet.maxWordAndConceptIndexes

    conceptCount += 1
    val iAdjBunch = conceptCount

    conceptCount += 1
    val naAdjBunch = conceptCount

    conceptCount += 1
    val allAdjBunch = conceptCount

    bufferSet.agents += Agent(
      targetBunch = allAdjBunch,
      sourceBunches = Set(iAdjBunch, naAdjBunch),
      matcher = Map(),
      adder = Map(),
      rule = StreamedDatabaseConstants.nullBunchId,
      fromStart = false
    )

    checkWriteAndRead(bufferSet)
  }

  it should "match on write and read agent changing suffix" in {
    val bufferSet = new BufferSet()

    val iSymbolArrayIndex = bufferSet.addSymbolArray("い")
    val kattaSymbolArrayIndex = bufferSet.addSymbolArray("かった")

    var (_, conceptCount) = bufferSet.maxWordAndConceptIndexes

    conceptCount += 1
    val iAdjBunch = conceptCount

    conceptCount += 1
    val ruleConcept = conceptCount

    bufferSet.agents += Agent(
      targetBunch = StreamedDatabaseConstants.nullBunchId,
      sourceBunches = Set(iAdjBunch),
      matcher = Map(Main.kanjiAlphabet -> iSymbolArrayIndex),
      adder = Map(Main.kanjiAlphabet -> kattaSymbolArrayIndex),
      rule = ruleConcept,
      fromStart = false
    )

    checkWriteAndRead(bufferSet)
  }

  it should "match on write and read agent changing prefix" in {
    val bufferSet = new BufferSet()

    val goSymbolArrayIndex = bufferSet.addSymbolArray("ご")

    var (_, conceptCount) = bufferSet.maxWordAndConceptIndexes

    conceptCount += 1
    val waseiKangoBunch = conceptCount

    conceptCount += 1
    val ruleConcept = conceptCount

    bufferSet.agents += Agent(
      targetBunch = StreamedDatabaseConstants.nullBunchId,
      sourceBunches = Set(waseiKangoBunch),
      matcher = Map(),
      adder = Map(Main.kanjiAlphabet -> goSymbolArrayIndex),
      rule = ruleConcept,
      fromStart = true
    )

    checkWriteAndRead(bufferSet)
  }
}
