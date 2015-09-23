name := """arpa"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
	"org.apache.jena" % "jena-arq" % "2.12.1",
	ws
)
