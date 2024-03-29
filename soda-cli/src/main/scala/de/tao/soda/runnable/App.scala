package de.tao.soda.runnable

import de.tao.soda.etl.data.Wget
import de.tao.soda.{Help, Preset}

private object CommandPrefix {
  val cmd = "-do:"
  val path = "-p:" // input path or identifier
  val output = "-o:" // output path or identifier
  val config = "-i:"
  val app = "-app:"
}

private object ConfigParams {
  implicit class StringSeq(val arr: Array[String]) extends AnyVal {
    def toTuple = (arr(0), if (arr.size>1) arr(1) else "")
  }
  def fromString(str: String): Map[String, String] = {
    str.split(",").view.map{_.split("=").toTuple}.toMap
  }

  val DEBUG = "debug"
  val EXISTS_OK = "existok"
}

object Main extends App with Help {
  val argPool = if (args.size == 0) Set.empty[String] else args.toSet

  lazy val isDebug = argPool.contains("--debug")
  lazy val cmd = argPool.find(_.startsWith(CommandPrefix.cmd)).map(_.stripPrefix(CommandPrefix.cmd))
  lazy val app = argPool.find(_.startsWith(CommandPrefix.app)).map(_.stripPrefix(CommandPrefix.cmd))
  lazy val path = argPool.find(_.startsWith(CommandPrefix.path)).map(_.stripPrefix(CommandPrefix.path))
  lazy val output = argPool.find(_.startsWith(CommandPrefix.output)).map(_.stripPrefix(CommandPrefix.output))
  lazy val config = argPool.find(_.startsWith(CommandPrefix.config))
    .map{a => ConfigParams.fromString(a.stripPrefix(CommandPrefix.config))}
    .getOrElse(Map.empty)

  Console.println("[soda-cli]")

  cmd match {
    case Some("help") | None => printHelp
    case Some("wget") => // download arbitrary file or zip
      for {
        url <- path
        localOutput <- output
        existsOk = config.contains(ConfigParams.EXISTS_OK)
      }
        yield Wget.downloadToLocal(url, localOutput, existsOk)
    case Some("preset") => // run preset app
      for { ap <- app }
        yield Preset.runApp(ap)

    case _ => throw new UnsupportedOperationException(s"Unknown command : ${cmd.getOrElse("<empty>")}")
  }
}
