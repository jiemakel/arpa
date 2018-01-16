name := """arpa"""

version := "1.0"

lazy val root = (project in file(".")).enablePlugins(
  PlayScala,
  SystemdPlugin,
  DockerPlugin,
  AshScriptPlugin)

scalaVersion := "2.11.12"

maintainer := "Eetu Mäkelä <eetu.makela@helsinki.fi>"

packageSummary := "arpa"

packageDescription := "Named entity linking web service"

sources in (Compile, doc) := Seq.empty

publishArtifact in (Compile, packageDoc) := false

dockerBaseImage := "openjdk:alpine"

dockerExposedPorts in Docker := Seq(9000, 9443)

dockerExposedVolumes := Seq("/opt/docker/logs","/opt/docker/services")

dockerUsername := Some("jiemakel")

libraryDependencies ++= Seq(
	"org.apache.jena" % "jena-arq" % "2.12.1",
    "org.hjson" % "hjson" % "1.1.2",
	ws
)
