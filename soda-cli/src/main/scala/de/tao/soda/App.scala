package de.tao.soda

private object CommandPrefix {
  val cmd = "-do:"
  val path = "-p:"
  val output = "-o:"
}

object Main extends App {
  val argPool = if (args.size == 0) Set.empty[String] else args.toSet

  lazy val isDebug = argPool.contains("--debug")
  lazy val cmd = argPool.find(_.startsWith(CommandPrefix.cmd)).map(_.stripPrefix(CommandPrefix.cmd))
  lazy val path = argPool.find(_.startsWith(CommandPrefix.path)).map(_.stripPrefix(CommandPrefix.path))
  lazy val output = argPool.find(_.startsWith(CommandPrefix.output)).map(_.stripPrefix(CommandPrefix.output))

  Console.println(s"Test start, args : ${argPool}")
  cmd.map{ c => Console.println(s"... command : ${c}")}
  path.map{ c => Console.println(s"... path : ${c}")}
}
