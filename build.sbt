name := "TalkGraphX"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-graphx" % "1.5.2",
  "org.graphstream" % "gs-ui" % "1.3",
  "org.graphstream" % "gs-core" % "1.3"
)
