name := "Langbook Database Converter"

description := "Program that reads a Langbook SQLite database (version 3) and transforms it to a Streamed database for Langbook"

version := "1.0"

scalaVersion := "2.12.2"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.19.3",
  "com.github.carlos-sancho-ramirez" % "lib-java-bitstream" % "aacc49f5ebc3acb7d195d69255e8d2f4155a106a",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

testOptions in Test += Tests.Argument("-oF") //Show the full stack trace when a test fails

