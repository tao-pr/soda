import Build.autoImport._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

import sbt._

// REF: https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html#Configuration-level+tasks

scalaVersion := "2.13.8"
sbtVersion := "1.6.2"

lazy val root = Project("soda", file("."))
  .settings(
    publish := {},
    publishArtifact := false,
    name := "soda"
  )
  .aggregate(
    `soda-cli`,
    `soda-etl`
  )

lazy val `soda-cli` = project.in(file("soda-cli"))
  .settings(
    libraryDependencies ++= Seq.empty
  )
  .dependsOn(`soda-etl`)

val slf4jBinding = ExclusionRule(organization = "org.slf4j")
lazy val `soda-etl` = project.in(file("soda-etl"))
  .settings(
    libraryDependencies ++= Seq(
      sparkCore excludeAll(slf4jBinding),
      sparkSql excludeAll(slf4jBinding),
      scalaLogging,
      logback
    )
  )

lazy val `soda-benchmark` = project.in(file("soda-benchmark"))
  .dependsOn(`soda-cli`)
  .enablePlugins(JmhPlugin)
  .settings(
    Test / fork := true,
    libraryDependencies ++= Seq(
      "pl.project13.scala" % "sbt-jmh-extras" % SbtJmhVersion,
      "org.openjdk.jmh" % "jmh-core" % JmhVersion,
      "org.openjdk.jmh" % "jmh-generator-asm" % JmhVersion,
      "org.openjdk.jmh" % "jmh-generator-bytecode" % JmhVersion,
      "org.openjdk.jmh" % "jmh-generator-reflection" % JmhVersion
    )
  )

resolvers += "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"

// Doesn't work with sbt console -> "run" or "runMain"
mainClass / run := Some("de.tao.soda.Main")
