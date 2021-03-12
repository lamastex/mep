lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.lamastex",
      scalaVersion := "2.13.3",
    )),
    name := "mep"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "org.twitter4j" % "twitter4j-examples" % "4.0.7"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"

scalacOptions += "-deprecation"

