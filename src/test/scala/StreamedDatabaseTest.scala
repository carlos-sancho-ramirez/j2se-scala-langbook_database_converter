import org.scalatest.{FlatSpec, Matchers}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import sword.bitstream.{InputBitStream, OutputBitStream}

class StreamedDatabaseTest extends FlatSpec with Matchers {

  behavior of "StreamedDatabaseWriter and StreamedDatabaseReader"

  it should "match on write and read symbol arrays" in {
    val symbolArrays = Vector(
      "first word",
      "second word",
      "third word"
    )

    val bufferSet1 = new BufferSet()
    for (array <- symbolArrays) {
      bufferSet1.addSymbolArray(array)
    }

    val baos = new ByteArrayOutputStream
    val obs = new OutputBitStream(baos)

    StreamedDatabaseWriter.writeSymbolArrays(bufferSet1.symbolArrays, obs)
    obs.close()

    val array = baos.toByteArray
    val bais = new ByteArrayInputStream(array)
    val ibs = new InputBitStream(bais)

    val bufferSet2 = new BufferSet()
    StreamedDatabaseReader.readSymbolArrays(bufferSet2, ibs)

    bufferSet2.symbolArrays shouldBe bufferSet1.symbolArrays
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

  it should "match on write and read conversions" in {
    val sourceSet = new BufferSet()

    val kanaRoumaPairs = Vector(
      sourceSet.addSymbolArray("か") -> sourceSet.addSymbolArray("ka"),
      sourceSet.addSymbolArray("ぞ") -> sourceSet.addSymbolArray("zo"),
      sourceSet.addSymbolArray("く") -> sourceSet.addSymbolArray("ku")
    )

    sourceSet.conversions += Conversion(Main.kanaAlphabet, Main.roumajiAlphabet, kanaRoumaPairs)

    val kanjiKanaPairs = Vector(
      sourceSet.addSymbolArray("家") -> sourceSet.addSymbolArray("か"),
      sourceSet.addSymbolArray("族") -> sourceSet.addSymbolArray("ぞく")
    )

    sourceSet.conversions += Conversion(Main.kanjiAlphabet, Main.kanaAlphabet, kanjiKanaPairs)

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read acceptations and its representations even if only include symbol arrays" in {
    val sourceSet = new BufferSet()

    val symbolArrays = Vector(
      "importante",
      "cabello",
      "papel",
      "dios"
    )

    for (array <- symbolArrays) sourceSet.addSymbolArray(array)
    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read acceptations, its representations and correlations" in {
    val sourceSet = new BufferSet()

    val wordBase = StreamedDatabaseConstants.minValidWord
    val conceptBase = StreamedDatabaseConstants.minValidConcept

    val daijiCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.kanjiAlphabet -> "大事",
      Main.kanaAlphabet -> "だいじ")))
    val taisetsuCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.kanjiAlphabet -> "大切",
      Main.kanaAlphabet -> "たいせつ")))
    val importanteCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.esAlphabet -> "importante")))
    val hearCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.kanjiAlphabet -> "髪",
      Main.kanaAlphabet -> "かみ")))
    val paperCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.kanjiAlphabet -> "紙",
      Main.kanaAlphabet -> "かみ")))
    val godCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.kanjiAlphabet -> "神",
      Main.kanaAlphabet -> "かみ")))
    val cabelloCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.esAlphabet -> "cabello")))
    val papelCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.esAlphabet -> "papel")))
    val diosCorrArrayId = sourceSet.addCorrelationArray(Vector(Map(
      Main.esAlphabet -> "dios")))

    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 0, conceptBase + 0))) = Set(daijiCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 1, conceptBase + 0))) = Set(taisetsuCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 2, conceptBase + 0))) = Set(importanteCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 3, conceptBase + 1))) = Set(hearCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 3, conceptBase + 2))) = Set(paperCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 3, conceptBase + 3))) = Set(godCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 4, conceptBase + 1))) = Set(cabelloCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 5, conceptBase + 2))) = Set(papelCorrArrayId)
    sourceSet.acceptationCorrelations(sourceSet.addAcceptation(Acceptation(wordBase + 6, conceptBase + 3))) = Set(diosCorrArrayId)

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
    val runSymbolArray = sourceSet.addSymbolArray("run")
    val jumpSymbolArray = sourceSet.addSymbolArray("jump")

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

    val accBase = sourceSet.acceptations.size
    sourceSet.acceptations ++= Vector(
      Acceptation(verbWord, verbConcept),
      Acceptation(runWord, runConcept),
      Acceptation(jumpWord, jumpConcept),
      Acceptation(japanWord, japanConcept),
      Acceptation(countryWord, countryConcept)
    )

    sourceSet.acceptationCorrelations(accBase) = Set(sourceSet.addCorrelationArrayForIndex(Vector(Map(Main.enAlphabet -> verbSymbolArray))))
    sourceSet.acceptationCorrelations(accBase + 1) = Set(sourceSet.addCorrelationArrayForIndex(Vector(Map(Main.enAlphabet -> runSymbolArray))))
    sourceSet.acceptationCorrelations(accBase + 2) = Set(sourceSet.addCorrelationArrayForIndex(Vector(Map(Main.enAlphabet -> jumpSymbolArray))))
    sourceSet.acceptationCorrelations(accBase + 3) = Set(sourceSet.addCorrelationArrayForIndex(Vector(Map(Main.enAlphabet -> japanSymbolArray))))
    sourceSet.acceptationCorrelations(accBase + 4) = Set(sourceSet.addCorrelationArrayForIndex(Vector(Map(Main.enAlphabet -> countrySymbolArray))))

    sourceSet.bunchAcceptations(verbConcept) = Set(accBase + 1, accBase + 2)
    sourceSet.bunchAcceptations(countryConcept) = Set(accBase + 3)

    checkWriteAndRead(sourceSet)
  }

  it should "match on write and read agent matching all い ended words" in {
    val bufferSet = new BufferSet()

    val iSymbolArray = bufferSet.addSymbolArray("い")

    var (_, conceptCount) = bufferSet.maxWordAndConceptIndexes
    conceptCount += 1
    val targetBunch = conceptCount

    val matcher = bufferSet.addCorrelation(Map(
      Main.kanjiAlphabet -> iSymbolArray
    ))

    bufferSet.agents += Agent(
      targetBunch,
      sourceBunches = Set(),
      matcher,
      adder = bufferSet.addCorrelation(Map()),
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

    val nullCorrelation = bufferSet.addCorrelation(Map())
    bufferSet.agents += Agent(
      targetBunch = allAdjBunch,
      sourceBunches = Set(iAdjBunch, naAdjBunch),
      matcher = nullCorrelation,
      adder = nullCorrelation,
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
      matcher = bufferSet.addCorrelation(Map(Main.kanjiAlphabet -> iSymbolArrayIndex)),
      adder = bufferSet.addCorrelation(Map(Main.kanjiAlphabet -> kattaSymbolArrayIndex)),
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
      matcher = bufferSet.addCorrelation(Map()),
      adder = bufferSet.addCorrelation(Map(Main.kanjiAlphabet -> goSymbolArrayIndex)),
      rule = ruleConcept,
      fromStart = true
    )

    checkWriteAndRead(bufferSet)
  }
}
