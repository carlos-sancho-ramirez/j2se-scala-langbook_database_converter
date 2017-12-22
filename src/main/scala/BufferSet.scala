import BufferSet.Correlation
import Main.languages

import scala.collection.mutable.ArrayBuffer

case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)
case class Acceptation(word: Int, concept: Int)
case class NewAcceptation(word: Int, concept: Int, correlation: Int)

/** Data for Agents.
  *
  * An agent is a dynamic algorithm that fills the target bunch
  * considering the acceptations in sourceBunches that are not
  * included in the diffBunches.
  * In addition, if matcher is present, only acceptations whose
  * words matches the matcher will be processed.
  * If adder is present, the words may be converted in its
  * derivatives following the adder, creating new acceptations.
  * Only if the adder is present, the rule parameter will be
  * associated with the mutation applied.
  *
  * @param targetBunch bunch to be filled.
  *                    It can be [[StreamedDatabaseConstants.nullBunchId]]
  *                    if there is no other agent that is going
  *                    to use the result of this as source.
  * @param sourceBunches Set of bunches to get the words from.
  *                      [[StreamedDatabaseConstants.nullBunchId]]
  *                      cannot be included in this set. In order
  *                      to mention that this agent applies to
  *                      each acceptation in the database an
  *                      empty set must be used.
  * @param matcher Optional map from Alphabet to symbol array
  *                used as filter in the acceptations.
  * @param adder Optional map from alphabet to symbol array used
  *              to replace all matched symbol arrays.
  * @param rule Concept for the rule applied. This should be
  *             [[StreamedDatabaseConstants.nullBunchId]] when
  *             no adder is provided as no modifications are
  *             given in the word.
  * @param fromStart If true, matcher and adder is applied from
  *                  the beginning of the word (prefix).
  *                  If false, they are applied from the rear part (suffix).
  */
case class Agent(targetBunch: Int, sourceBunches: Set[Int], matcher: Int /* Correlation */, adder: Int /* Correlation */, rule: Int /* concept */, fromStart: Boolean) {
  if (targetBunch == StreamedDatabaseConstants.nullBunchId) {
    if (matcher == adder) {
      throw new IllegalArgumentException("When target is null, modification is expected. But matcher and adder are equivalent")
    }

    if (rule == StreamedDatabaseConstants.nullRuleId) {
      throw new IllegalArgumentException("When target is null, modification is expected. But rule is null")
    }
  }

  if (rule != StreamedDatabaseConstants.nullRuleId && matcher == adder) {
    throw new IllegalArgumentException("When rule is provided, modification is expected. But matcher and adder are equivalent")
  }
}

object InvalidRegister {
  val wordRepresentation = WordRepresentation(-1, -1, -1)
}

object BufferSet {
  type Correlation = Map[Int /* Alphabet */, Int /* Symbol Array */]
}

/**
  * Contain all mutable collections representing in memory the current database state.
  */
class BufferSet {
  private val hashTableSize = 256
  private val hashTableMask = hashTableSize - 1

  private val _symbolArrays = ArrayBuffer[String]()
  private val _symbolArrayHashes = Array.ofDim[Int](hashTableSize).map(_ => Set[Int]())
  def symbolArrays: scala.collection.IndexedSeq[String] = _symbolArrays

  val alphabets = Main.alphabets
  val languages = Main.languages

  val conversions = scala.collection.mutable.Set[Conversion]()

  // This is currently really specific for Japanese, this must be adapted for any alphabet
  val kanjiKanaCorrelations = ArrayBuffer[(Int /* kanji symbol array */, Int /* kana symbol array */)]()
  val wordRepresentations = ArrayBuffer[WordRepresentation]()
  val acceptations = ArrayBuffer[Acceptation]()
  // This is currently really specific for Japanese.
  // So far Japanese is the only language using correlations, thus for now it is assumed that
  // source alphabet is always kanji and target alphabet is always kana
  // TODO: Make this for any language.
  //val jaWordCorrelations = scala.collection.mutable.Map[Int /* acc id */, Vector[Int /* Indexes within kanjiKanaCorrelations */]]()
  val jaWordCorrelations = scala.collection.mutable.Map[Int /* word id */, Set[(Set[Int] /* concepts */, Vector[Int /* Indexes within kanjiKanaCorrelations */])]]()

  private val _correlations = ArrayBuffer[Correlation]()
  private val _correlationHashes = Array.ofDim[Int](hashTableSize).map(_ => Set[Int]())
  def correlations: scala.collection.IndexedSeq[Correlation] = _correlations

  private val _correlationArrays = ArrayBuffer[Seq[Int /* correlation index */]]()
  private val _correlationArrayHashes = Array.ofDim[Int](hashTableSize).map(_ => Set[Int]())
  def correlationArrays: scala.collection.IndexedSeq[Seq[Int]] = _correlationArrays

  private val _newAcceptations = ArrayBuffer[NewAcceptation]()
  private val _newAcceptationHashes = Array.ofDim[Int](hashTableSize).map(_ => Set[Int]())
  def newAcceptations: scala.collection.IndexedSeq[NewAcceptation] = _newAcceptations

  private val _oldNewAcceptationsMap = scala.collection.mutable.Map[Int /* Old Acceptation index */, Int /* New Acceptation index */]()
  val bunchConcepts = scala.collection.mutable.Map[Int /* Bunch */, Set[Int] /* concepts within the bunch */]()
  val bunchAcceptations = scala.collection.mutable.Map[Int /* Bunch */, Set[Int] /* acceptations indexes within the bunch */]()
  def bunchNewAcceptations: scala.collection.Map[Int /* Bunch */, Set[Int]] = bunchAcceptations.mapValues(_.map(_oldNewAcceptationsMap))

  val agents = scala.collection.mutable.Set[Agent]()

  override def hashCode: Int = {
    _symbolArrays.length + acceptations.length + bunchAcceptations.size + _newAcceptations.length
  }

  override def equals(other: Any): Boolean = {
    other != null && other.isInstanceOf[BufferSet] && {
      val that = other.asInstanceOf[BufferSet]

      symbolArrays == that.symbolArrays &&
      conversions == that.conversions &&
      acceptations == that.acceptations &&
      wordRepresentations == that.wordRepresentations &&
      kanjiKanaCorrelations == that.kanjiKanaCorrelations &&
      jaWordCorrelations == that.jaWordCorrelations &&
      _newAcceptations == that._newAcceptations &&
      _correlations == that._correlations &&
      _correlationArrays == that._correlationArrays &&
      bunchConcepts == that.bunchConcepts &&
      bunchAcceptations == that.bunchAcceptations &&
      agents == that.agents
    }
  }

  override def toString: String = conversions.toString

  private def insertIfNotPresent[T](array: ArrayBuffer[T], hashes: scala.collection.mutable.IndexedSeq[Set[Int]], element: T): Int = {
    val shortHash = element.hashCode & hashTableMask
    val candidates = hashes(shortHash)

    var found = -1
    val size = array.size
    candidates.exists { index =>
      if (array(index) == element) {
        found = index
        true
      }
      else false
    }

    if (found >= 0) found
    else {
      array += element
      hashes(shortHash) = candidates + size
      size
    }
  }

  /**
    * Checks if the given symbol array already exist in the list.
    * If so, the index is returned. If not it is appended into
    * the list and the index is returned.
    */
  def addSymbolArray(symbolArray: String): Int = {
    if (symbolArray == null) {
      throw new IllegalArgumentException()
    }

    insertIfNotPresent(_symbolArrays, _symbolArrayHashes, symbolArray)
  }

  /**
    * Checks if the given correlation already exists in the list.
    * If so, the index is returned. If not it is appended into
    * the list and the index is returned.
    */
  def addCorrelation(correlation: Correlation): Int = {
    if (correlation == null) {
      throw new IllegalArgumentException()
    }

    insertIfNotPresent(_correlations, _correlationHashes, correlation)
  }

  /**
    * Checks if the given correlation already exists in the list.
    * If so, the index is returned. If not it is appended into
    * the list and the index is returned.
    */
  def addCorrelationForString(correlation: Map[Int, String]): Int = {
    if (correlation == null) {
      throw new IllegalArgumentException()
    }

    addCorrelation(correlation.mapValues(addSymbolArray))
  }

  def addCorrelationArrayForIntArray(array: scala.collection.Seq[Int]) = {
    insertIfNotPresent(_correlationArrays, _correlationArrayHashes, array)
  }

  def addCorrelationArrayForIndex(array: scala.collection.Seq[Map[Int, Int]]) = {
    val arr = array.map(addCorrelation)
    addCorrelationArrayForIntArray(arr)
  }

  def addCorrelationArray(array: scala.collection.Seq[Map[Int, String]]) = {
    val arr = array.map(addCorrelationForString)
    addCorrelationArrayForIntArray(arr)
  }

  def addAcceptation(acc: NewAcceptation) = {
    val index = insertIfNotPresent(_newAcceptations, _newAcceptationHashes, acc)

    val oldAcc = Acceptation(acc.word, acc.concept)
    var accIndex = acceptations.indexOf(oldAcc)
    if (accIndex < 0) {
      accIndex = acceptations.size
      acceptations += oldAcc
    }

    _oldNewAcceptationsMap(accIndex) = index
    index
  }

  def charCountMap: Map[Char, Int] = {
    symbolArrays.foldLeft(scala.collection.mutable.Map[Char, Int]()) {
      (map, string) =>
        for (char <- string) {
          map(char) = map.getOrElse(char, 0) + 1
        }
        map
    }.toMap
  }

  def fullCharSet: Array[Char] = charCountMap.toArray.sortWith((a,b) => a._2 > b._2).map(_._1)

  def maxWordAndConceptIndexes: (Int, Int) = {
    val wordMin = StreamedDatabaseConstants.minValidWord - 1
    val conceptMin = StreamedDatabaseConstants.minValidConcept - 1
    val (maxWordFromAcceptations, maxConceptFromAcceptations) = acceptations.foldLeft((wordMin, conceptMin)) {
      case ((word, concept), acc) =>
        val maxWord = if (acc.word > word) acc.word else word
        val maxConcept = if (acc.concept > concept) acc.concept else concept
        (maxWord, maxConcept)
    }

    val (maxWordFromNewAcceptations, maxConceptFromNewAcceptations) = _newAcceptations.foldLeft((wordMin, conceptMin)) {
      case ((word, concept), acc) =>
        val maxWord = if (acc.word > word) acc.word else word
        val maxConcept = if (acc.concept > concept) acc.concept else concept
        (maxWord, maxConcept)
    }

    val maxConceptFromAlphabets = alphabets.max
    val maxConceptFromLanguages = languages.map(_.concept).max

    val maxConceptFromBunchConcepts = bunchConcepts.foldLeft(-1) { case (max, (bunch, concepts)) =>
      val thisMax = (concepts + bunch).max
      if (thisMax > max) thisMax
      else max
    }

    val maxConceptFromBunchAcceptations = (bunchAcceptations.keySet + (-1)).max

    val maxConceptFromAgentTargets = agents.foldLeft(-1) {
      case (acc, agent) =>
        if (agent.targetBunch > acc) agent.targetBunch
        else acc
    }

    val maxConceptFromAgentRules = agents.foldLeft(-1) {
      case (acc, agent) =>
        if (agent.rule > acc) agent.rule
        else acc
    }

    val maxConceptFromAgentSources = agents.foldLeft(-1) {
      case (acc, agent) =>
        if (agent.sourceBunches.isEmpty) acc
        else Math.max(agent.sourceBunches.max, acc)
    }

    val maxConcept = Set(
      maxConceptFromAcceptations,
      maxConceptFromNewAcceptations,
      maxConceptFromAlphabets,
      maxConceptFromLanguages,
      maxConceptFromBunchConcepts,
      maxConceptFromBunchAcceptations,
      maxConceptFromAgentTargets,
      maxConceptFromAgentRules,
      maxConceptFromAgentSources
    ).max

    val maxAcceptation = Set(
      maxWordFromAcceptations,
      maxWordFromNewAcceptations
    ).max

    (maxAcceptation, maxConcept)
  }

  // Include symbol arrays for language identifiers
  for (language <- languages) {
    addSymbolArray(language.code)
  }
}
