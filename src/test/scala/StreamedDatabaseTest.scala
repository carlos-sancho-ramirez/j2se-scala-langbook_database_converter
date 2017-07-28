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

  private def checkAcceptationsAndItsRepresentations(sourceSet: BufferSet): Unit = {
    val baos = new ByteArrayOutputStream
    val obs = new OutputBitStream(baos)

    StreamedDatabaseWriter.write(sourceSet, obs)
    obs.close()

    val array = baos.toByteArray
    val bais = new ByteArrayInputStream(array)
    val ibs = new InputBitStream(bais)

    val targetSet = new BufferSet()
    StreamedDatabaseReader.read(targetSet, ibs)

    targetSet.symbolArrays shouldBe sourceSet.symbolArrays
    targetSet.acceptations shouldBe sourceSet.acceptations
    targetSet.wordRepresentations shouldBe sourceSet.wordRepresentations
    targetSet.accRepresentations shouldBe sourceSet.accRepresentations
  }

  it should "match on write and read acceptations and its representations even if only include symbol arrays" in {
    val sourceSet = new BufferSet()

    sourceSet.symbolArrays ++= Vector(
      "importante",
      "cabello",
      "papel",
      "dios"
    )

    checkAcceptationsAndItsRepresentations(sourceSet)
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

    sourceSet.accRepresentations ++= Vector(
      AccRepresentation(3, 5),
      AccRepresentation(4, 6),
      AccRepresentation(5, 7)
    )

    checkAcceptationsAndItsRepresentations(sourceSet)
  }
}
