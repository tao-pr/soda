import sbt.Keys._
import sbt.{Def, _}

object Build extends AutoPlugin {

  object autoImport {
    val org = "de.tao.soda"
    val ScalatestVersion = "3.2.11"
    val SbtJmhVersion = "0.3.7"
    val JmhVersion = "1.34"
    val sparkVersion = "3.2.1"
    val jacksonVersion = "2.13.2"
    val scalaTestVersion = "3.2.11"

    val jacksonModule = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.11"
    val pureCSV = "io.kontainers" %% "purecsv" % "1.3.10"
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
    val sparkCore =  "org.apache.spark" %% "spark-core" % sparkVersion
    val sparkSql =  "org.apache.spark" %% "spark-sql" % sparkVersion
    val scalaTestLactic = "org.scalactic" %% "scalactic" % scalaTestVersion
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

  }

  import autoImport._

  def releaseVersion: String = sys.env.getOrElse("RELEASE_VERSION", "")
  def isRelease: Boolean = releaseVersion != ""

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    organization := org,
    resolvers += Resolver.mavenLocal,
    Test / parallelExecution := false,
    Test / scalacOptions ++= Seq("-Xmax-inlines:64"),
    javacOptions := Seq("-source", "1.17", "-target", "1.17"),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala3-compiler_3" % scalaVersion.value,
      "org.scalatest"     % "scalatest_3"       % ScalatestVersion      % "test"
    )
  )

}