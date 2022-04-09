package de.tao.soda.etl.data

import de.tao.soda.etl.DataReader
import org.apache.commons.io.FileExistsException

import java.io.{File, FileOutputStream, InputStream}
import java.net.URL
import java.util.zip.ZipInputStream

case object WgetStream extends DataReader[InputStream] {
  override def run(input: String, dry: Boolean): InputStream = {
    new URL(input).openStream()
  }
}

case object WgetZippedStream extends DataReader[InputStream] {
  override def run(input: String, dry: Boolean): InputStream = {
    new ZipInputStream(new URL(input).openStream())
  }
}

class WgetToFile(toPath: String, append: Boolean=false, existsOk: Boolean=true) extends DataReader[String] {
  override def run(input: String, dry: Boolean): String = {
    if (!existsOk && new File(toPath).exists()){
      throw new FileExistsException(s"File ${toPath} already exists. Download aborted")
    }
    else {
      val ins = new URL(input).openStream()
      val oss = new FileOutputStream(toPath, append)
      ins.transferTo(oss)
      toPath
    }
  }
}

class WgetZippedToFiles(toPath: String, append: Boolean=false, existsOk: Boolean=true) extends DataReader[String] {
  override def run(input: String, dry: Boolean): String = {
    val outDir = new File(toPath)
    if (!existsOk && outDir.exists() && outDir.isDirectory){
      val m = s"Directory ${toPath} already exists. Download aborted"
      val e = new FileExistsException(m)
      logger.error(e)(m)
      throw e
    }
    else {
      val ins = new ZipInputStream(new URL(input).openStream())
      logger.info(s"Downloading ${input} into ${toPath}")
      LazyList.continually(ins.getNextEntry).takeWhile(_ != null).foreach { file =>
        if (!file.isDirectory){
          // ensure parent directories exist
          val outFullPath = outDir.toPath.resolve(file.getName)
          if (!outFullPath.getParent.toFile.exists() && !dry)
            outFullPath.getParent.toFile.mkdirs()

          // TODO: if dry run, make another /dev/null/ output stream
          val oss = new FileOutputStream(outFullPath.toFile)
          val buffer = new Array[Byte](1024)
          LazyList.continually(ins.read(buffer)).takeWhile(_ != -1).foreach(oss.write(buffer, 0, _))
          oss.close
        }
      }
    }
    toPath
  }
}

object Wget {
  def downloadToLocal(url: String, outputPath: String, existsOk: Boolean, dry: Boolean) = {
    val isZipped = url.endsWith(".gz") || url.endsWith(".zip") || url.endsWith(".tar")
    val workflow = if (isZipped) new WgetZippedToFiles(outputPath, false, existsOk)
    else new WgetToFile(outputPath, false, existsOk)
    workflow.run(url, dry)
  }
}