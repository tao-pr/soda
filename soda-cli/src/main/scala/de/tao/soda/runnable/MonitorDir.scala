package de.tao.soda.runnable

import com.typesafe.scalalogging.LazyLogging
import de.tao.soda.etl.Workflow
import de.tao.soda.etl.data.{TimestampUUIDPath, Watcher}

import java.time.format.DateTimeFormatter

class LogFilename extends Workflow[String, String]{
  override def run(input: String): String = {
    logger.info(s"LogFilename : $input")
    input
  }
}

object MonitorDir extends App with LazyLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

  val bucket = "soda-monitor"
  val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
  val nameGen = TimestampUUIDPath("soda-monitor", fmt, ".json")
  val wf = new LogFilename()
  val monitor = new Watcher[String](Some(wf), None)
  val path = System.getProperty("user.dir") // cwd

  logger.info("[MonitorDir] starting watching $path")
  monitor.run(path)
  while (true){
    Thread.sleep(25)
  }
}
