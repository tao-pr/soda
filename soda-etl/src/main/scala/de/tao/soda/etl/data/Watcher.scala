package de.tao.soda.etl.data

import better.files.File
import de.tao.soda.etl.{DataLoader, InputIdentifier, Workflow}
import io.methvin.better.files.RecursiveFileMonitor

/**
 * Directory watcher
 * binds with a Workflow which takes a fullpath to the file to process
 */
class Watcher[String, T](pipePresence: Workflow[Predef.String, T], pipeDelete: Workflow[Predef.String, T]) extends DataLoader[Unit] {

  override def run(input: Predef.String, dry: Boolean): Unit = {
    if (dry){
      logger.info(s"Watcher to watch $input")
    }
    else {
      logger.info(s"Watcher start watching $input")
      val watcher = new RecursiveFileMonitor(File(input)) {
        override def onCreate(file: File, count: Int) = {
          logger.info(s"Watcher receiving a CREATE signal: $file")
          trigger(file)
        }
        override def onModify(file: File, count: Int) = {
          logger.info(s"Watcher receiving a MODIFY signal: $file")
          trigger(file)
        }
        override def onDelete(file: File, count: Int) = {
          logger.info(s"Watcher receiving a DELETE signal: $file")
          trigger(file, asDelete=true)
        }
      }
    }
  }

  private def trigger(file: File, asDelete: Boolean=false): Unit ={
    if (asDelete)
      pipeDelete.run(file.canonicalPath)
    else
      pipePresence.run(file.canonicalPath)
  }
}