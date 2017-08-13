# j2se-scala-langbook_database_converter
Program that reads a [Langbook](https://github.com/carlos-sancho-ramirez/generic-scala-langbook) SQLite database (version 3) and transforms it to a Streamed database for [Langbook](https://github.com/carlos-sancho-ramirez/generic-scala-langbook)

## How to run this program
This program is used to convert one database format (SQLite3) to the Langbook Streamed Database format. That means that an SQLite database containing valid data for the Langbook app (version 3) is required. First, copy the SQLite database file to read into src/main/resources/langbook.db

This program uses the [Bit Stream Library](https://github.com/carlos-sancho-ramirez/lib-java-bitstream) to encode the resulting Langbook Streamed databse file. The dependency is automatically handled by [SBT](http://www.scala-sbt.org/), but in order to resolve the dependency the library must be first packed and published locally in SBT. Please read the README file for the Bit Streams Library for more details.

Once satisfied the previous dependencies. This can be run just executing

    sbt run

