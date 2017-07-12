name := "Langbook database v3 jdbc reader"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.19.3",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

