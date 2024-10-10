import sbtassembly.AssemblyKeys.{assembly, assemblyShadeRules}
import sbtassembly.AssemblyPlugin.autoImport.*

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "listener",

    assembly / assemblyShadeRules := Seq(
      // spark uses json4s too and it's a known yet not fixed issue
      ShadeRule.rename("org.json4s.**" -> "shadedjson4s.@1").inAll
    )
  )

libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.16"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided"
libraryDependencies += "org.zeromq" % "jeromq" % "0.6.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "4.0.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test"
libraryDependencies += "org.mockito" %% "mockito-scala" % "1.17.37" % "test"
