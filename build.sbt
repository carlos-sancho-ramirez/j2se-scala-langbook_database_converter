name := "Langbook database v3 jdbc reader"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.19.3",
  "sword" %% "bit-streams-library" % "1.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

testOptions in Test += Tests.Argument("-oF") //Show the full stack trace when a test fails

