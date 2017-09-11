import Main.languages

import scala.collection.mutable.ArrayBuffer

case class WordRepresentation(word: Int, alphabet: Int, symbolArray: Int)
case class Acceptation(word: Int, concept: Int)

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
case class Agent(targetBunch: Int, sourceBunches: Set[Int], matcher: BufferSet.Correlation, adder: BufferSet.Correlation, rule: Int /* concept */, fromStart: Boolean) {
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
  val alphabets = Main.alphabets
  val languages = Main.languages
  val wordRepresentations = ArrayBuffer[WordRepresentation]()
  val acceptations = ArrayBuffer[Acceptation]()
  val symbolArrays = ArrayBuffer[String]()
  val bunchConcepts = scala.collection.mutable.Map[Int /* Bunch */, Set[Int] /* concepts within the bunch */]()
  val bunchAcceptations = scala.collection.mutable.Map[Int /* Bunch */, Set[Int] /* acceptations indexes within the bunch */]()
  val conversions = scala.collection.mutable.Set[Conversion]()

  // This is currently really specific for Japanese, this must be adapted for any alphabet
  val kanjiKanaCorrelations = ArrayBuffer[(Int /* kanji symbol array */, Int /* kana symbol array */)]()

  // This is currently really specific for Japanese.
  // So far Japanese is the only language using correlations, thus for now it is assumed that
  // source alphabet is always kanji and target alphabet is always kana
  // TODO: Make this for any language.
  //val jaWordCorrelations = scala.collection.mutable.Map[Int /* acc id */, Vector[Int /* Indexes within kanjiKanaCorrelations */]]()
  val jaWordCorrelations = scala.collection.mutable.Map[Int /* word id */, Set[(Set[Int] /* concepts */, Vector[Int /* Indexes within kanjiKanaCorrelations */])]]()

  val agents = scala.collection.mutable.Set[Agent]()

  override def hashCode: Int = {
    symbolArrays.length + acceptations.length + bunchAcceptations.size
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
      bunchConcepts == that.bunchConcepts &&
      bunchAcceptations == that.bunchAcceptations &&
      agents == that.agents
    }
  }

  override def toString: String = conversions.toString

  /**
    * Checks if the given symbol array already exist in the list.
    * If so, the index is returned. If not it is appended into
    * the list and the index is returned.
    */
  def addSymbolArray(symbolArray: String): Int = {
    if (symbolArray == null) {
      throw new IllegalArgumentException()
    }

    // This should be optimized indexing the string instead of check if it exists one by one.
    var found = -1
    var i = 0
    val size = symbolArrays.size
    while (found < 0 && i < size) {
      if (symbolArrays(i) == symbolArray) found = i
      i += 1
    }

    if (found >= 0) found
    else {
      symbolArrays += symbolArray
      size
    }
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
    val conceptMin = StreamedDatabaseConstants.minValidConcept - 1
    val (maxWordFromAcceptations, maxConceptFromAcceptations) = acceptations.foldLeft((-1, conceptMin)) {
      case ((word, concept), acc) =>
        val maxWord = if (acc.word > word) acc.word else word
        val maxConcept = if (acc.concept > concept) acc.concept else concept
        (maxWord, maxConcept)
    }

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
      maxConceptFromBunchConcepts,
      maxConceptFromBunchAcceptations,
      maxConceptFromAgentTargets,
      maxConceptFromAgentRules,
      maxConceptFromAgentSources
    ).max

    (maxWordFromAcceptations, maxConcept)
  }

  // Include symbol arrays for language identifiers
  for (language <- languages) {
    addSymbolArray(language.code)
  }
}
