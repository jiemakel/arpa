name := """reconciliator"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
	"org.apache.jena" % "jena-arq" % "2.12.1"
)
