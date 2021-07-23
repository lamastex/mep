lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.lamastex",
      scalaVersion := "2.12.10"
    )),
    name := "mep"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % Test
libraryDependencies += "org.twitter4j" % "twitter4j-examples" % "4.0.7"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.9.6"
libraryDependencies += "org.jboss" % "jdk-misc" % "3.Final"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
scalacOptions += "-deprecation"
