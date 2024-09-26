name := "BlastRDD"
organization := "gov.nih.nlm.ncbi"
version := "0.0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"    % "2.4.0",
  "org.scalatest"      %% "scalatest"     % "3.0.5" % "test"
)
