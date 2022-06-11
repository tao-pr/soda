package de.tao.soda

import de.tao.soda.etl.data.Wget

private object CommandPrefix {
  val cmd = "-do:"
  val path = "-p:" // input path or identifier
  val output = "-o:" // output path or identifier
  val config = "-i:"
}

private object ConfigParams {
  implicit class StringSeq(val arr: Array[String]) extends AnyVal {
    def toTuple = (arr(0), if (arr.size>1) arr(1) else "")
  }
  def fromString(str: String): Map[String, String] = {
    str.split(",").view.map{_.split("=").toTuple}.toMap
  }

  val DRY_RUN = "dry"
  val DEBUG = "debug"
  val EXISTS_OK = "existok"
}

object Main extends App with Help {
  val argPool = if (args.size == 0) Set.empty[String] else args.toSet

  lazy val isDebug = argPool.contains("--debug")
  lazy val cmd = argPool.find(_.startsWith(CommandPrefix.cmd)).map(_.stripPrefix(CommandPrefix.cmd))
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
        dry = config.contains(ConfigParams.DRY_RUN)
      }
        yield Wget.downloadToLocal(url, localOutput, existsOk, dry)

    case _ => throw new UnsupportedOperationException(s"Unknown command : ${cmd.getOrElse("<empty>")}")
  }
}
