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
      Acceptation(0, 0), //だいじ
      Acceptation(1, 0), //たいせつ
      Acceptation(2, 0), //importante
      Acceptation(3, 1), //かみ(hear)
      Acceptation(3, 2), //かみ(paper)
      Acceptation(3, 3), //かみ(God)
      Acceptation(4, 1), //cabello
      Acceptation(5, 2), //papel
      Acceptation(6, 3) //Dios
    )

    sourceSet.wordRepresentations ++= Vector(
      WordRepresentation(0, Main.kanjiAlphabet, 0),
      WordRepresentation(0, Main.kanaAlphabet, 1),
      WordRepresentation(1, Main.kanjiAlphabet, 2),
      WordRepresentation(1, Main.kanaAlphabet, 3),
      WordRepresentation(2, Main.esAlphabet, 4),
      WordRepresentation(3, Main.kanaAlphabet, 8),
      WordRepresentation(4, Main.esAlphabet, 9),
      WordRepresentation(5, Main.esAlphabet, 10),
      WordRepresentation(6, Main.esAlphabet, 11)
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

    sourceSet.acceptations ++= Vector(
      Acceptation(0, 0), //だいじ
      Acceptation(1, 0), //たいせつ
      Acceptation(2, 0), //importante
      Acceptation(3, 1), //かみ(hear)
      Acceptation(3, 2), //かみ(paper)
      Acceptation(3, 3), //かみ(God)
      Acceptation(4, 1), //cabello
      Acceptation(5, 2), //papel
      Acceptation(6, 3) //Dios
    )

    sourceSet.wordRepresentations ++= Vector(
      WordRepresentation(0, Main.kanjiAlphabet, 0),
      WordRepresentation(0, Main.kanaAlphabet, 1),
      WordRepresentation(1, Main.kanjiAlphabet, 2),
      WordRepresentation(1, Main.kanaAlphabet, 3),
      WordRepresentation(2, Main.esAlphabet, 4),
      WordRepresentation(3, Main.kanaAlphabet, 8),
      WordRepresentation(4, Main.esAlphabet, 9),
      WordRepresentation(5, Main.esAlphabet, 10),
      WordRepresentation(6, Main.esAlphabet, 11)
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
      (0, Set((Set(0), Vector(0, 2)))),
      (1, Set((Set(0), Vector(1, 3)))),
      (3, Set(
        (Set(1), Vector(4)),
        (Set(2), Vector(5)),
        (Set(3), Vector(6))
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

    sourceSet.acceptations ++= Vector(
      Acceptation(verbWord, verbConcept),
      Acceptation(runWord, runConcept),
      Acceptation(jumpWord, jumpConcept),
      Acceptation(japanWord, japanConcept),
      Acceptation(countryWord, countryConcept)
    )

    sourceSet.bunchAcceptations(verbConcept) = Set(runConcept, jumpConcept)
    sourceSet.bunchAcceptations(countryConcept) = Set(japanConcept)

    checkWriteAndRead(sourceSet)
  }
}
