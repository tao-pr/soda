package de.tao.soda.etl.data

import better.files.File

import java.nio.file.{Path, Paths}
import de.tao.soda.etl.{DataLoader, InputIdentifier, Workflow}
import io.methvin.better.files.RecursiveFileMonitor

import scala.concurrent.ExecutionContext

/**
 * Directory watcher
 * binds with a Workflow which takes a fullpath to the file to process
 */
class Watcher[T](pipeCreate: Option[Workflow[String, T]], pipeDelete: Option[Workflow[String, T]])
  (implicit val ec: ExecutionContext) extends DataLoader[Unit] {

  override def run(input: String): Unit = {
    logger.info(s"Watcher start watching $input")
    val path = better.files.File(input)
    val watcher = new RecursiveFileMonitor(path) {
      override def onCreate(file: File, count: Int) = {
        if (pipeCreate.isDefined) {
          logger.info(s"Watcher receiving a CREATE signal: $file")
          pipeCreate.map(_.run(file.canonicalPath))
        }
        else logger.info(s"Watch ignoring a CREATE signal: $file")
      }
      override def onModify(file: File, count: Int) = {
        if (pipeCreate.isDefined) {
          logger.info(s"Watcher receiving a MODIFY signal: $file")
          pipeCreate.map(_.run(file.canonicalPath))
        }
        else logger.info(s"Watcher ignoring a MODIFY signal: file")
      }
      override def onDelete(file: File, count: Int) = {
        if (pipeDelete.isDefined) {
          logger.info(s"Watcher receiving a DELETE signal: $file")
          pipeDelete.map(_.run(file.canonicalPath))
        }
        else logger.info(s"Watch ignoring a DELETE signal: $file")
      }
    }
    watcher.start()
  }

  override def printTree(level: Int): String = {
    val nlevel = level+1
    super.printTree(level) + "\n" +
    pipeCreate.map { pipe =>
        s"${prefixTree(nlevel)}[onCreate/Modify]" + "\n" +
        pipe.printTree(nlevel + 1) + "\n"
      }.getOrElse("") +
    pipeDelete.map { pipe =>
      s"${prefixTree(nlevel)}[onDelete]" + "\n" +
      printTree(nlevel + 1)
    }.getOrElse("")
  }
}