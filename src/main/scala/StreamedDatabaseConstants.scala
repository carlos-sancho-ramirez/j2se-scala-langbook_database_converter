
/** Constants within any StreamedDataBase for Langbook */
object StreamedDatabaseConstants {

  /** Reserved for agents for null references */
  val nullBunchId = 0

  /** Parent concept for all alphabets within the database */
  val alphabetConcept = 1

  /** Parent concept for all languages within the database */
  val languageConcept = 2

  /** First alphabet within the database */
  val minValidAlphabet = 3

  /** First concept within the database that is considered to be a valid concept */
  val minValidConcept = 1

  /** First word within the database that is considered to be a valid word */
  val minValidWord = 0
}
