name := "entropy"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-junit" % "4.3.5" % Test pomOnly (),
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)
