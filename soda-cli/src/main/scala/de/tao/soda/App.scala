package de.tao.soda

private object CommandPrefix {
  val cmd = "-do:"
  val path = "-p:" // input path or identifier
  val output = "-o:" // output path or identifier
  val config = "-i:"
}

object Main extends App with Help {
  val logger = org.log4s.getLogger
  val argPool = if (args.size == 0) Set.empty[String] else args.toSet

  lazy val isDebug = argPool.contains("--debug")
  lazy val cmd = argPool.find(_.startsWith(CommandPrefix.cmd)).map(_.stripPrefix(CommandPrefix.cmd))
  lazy val path = argPool.find(_.startsWith(CommandPrefix.path)).map(_.stripPrefix(CommandPrefix.path))
  lazy val output = argPool.find(_.startsWith(CommandPrefix.output)).map(_.stripPrefix(CommandPrefix.output))
  lazy val config = argPool.find(_.startsWith(CommandPrefix.config)).map(_.stripPrefix(CommandPrefix.config)) // TODO: load as a config

  Console.println("[soda-cli]")

  // TODO: do something
  cmd match {
    case Some("help") | None => printHelp
    case Some("cat") => ???
    case _ => throw new UnsupportedOperationException(s"Unknown command : ${cmd.getOrElse("<empty>")}")
  }
}
