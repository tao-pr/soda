import sbt.Keys._
import sbt.{Def, _}

object Build extends AutoPlugin {

  object autoImport {
    val org = "de.tao.soda"
    val Log4jVersion = "1.2.17"
    val ScalatestVersion = "3.2.11"
    val SbtJmhVersion = "0.3.7"
    val JmhVersion = "1.34"
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
      "log4j"             % "log4j"             % Log4jVersion          % "test",
      "org.scalatest"     % "scalatest_3"       % ScalatestVersion      % "test"
    )
  )

}