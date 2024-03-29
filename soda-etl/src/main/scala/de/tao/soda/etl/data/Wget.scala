package de.tao.soda.etl.data

import de.tao.soda.etl.DataLoader
import org.apache.commons.io.FileExistsException

import java.io.{File, FileOutputStream, InputStream}
import java.net.URL
import java.util.zip.ZipInputStream

case object WgetStream extends DataLoader[InputStream] {
  override def run(input: String): InputStream = {
    new URL(input).openStream()
  }
}

case object WgetZippedStream extends DataLoader[InputStream] {
  override def run(input: String): InputStream = {
    new ZipInputStream(new URL(input).openStream())
  }
}

class WgetToFile(toPath: String, append: Boolean=false, existsOk: Boolean=true) extends DataLoader[String] {
  override def run(input: String): String = {
    if (!existsOk && new File(toPath).exists()){
      throw new FileExistsException(s"File ${toPath} already exists. Download aborted")
    }
    else if (new File(toPath).exists()){
      // exists ok
      logger.info(s"WgetToFile skipping download, $toPath already exists")
      toPath
    }
    else {
      logger.info(s"Downloading $toPath from $input")
      val ins = new URL(input).openStream()
      val oss = new FileOutputStream(toPath, append)
      ins.transferTo(oss)
      toPath
    }
  }
}

class WgetZippedToFiles(toPath: String) extends DataLoader[String] {
  override def run(input: String): String = {
    val outDir = new File(toPath)
    val ins = new ZipInputStream(new URL(input).openStream())
    logger.info(s"Downloading ${input} into ${toPath}")
    LazyList.continually(ins.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (!file.isDirectory){
        // ensure parent directories exist
        val outFullPath = outDir.toPath.resolve(file.getName)
        if (!outFullPath.getParent.toFile.exists())
          outFullPath.getParent.toFile.mkdirs()

        logger.info(s"Reading from zip into : ${outFullPath}")
        logger.info(s"Extracting file $outFullPath")
        val oss = new FileOutputStream(outFullPath.toFile)
        val buffer = new Array[Byte](1024)
        LazyList.continually(ins.read(buffer)).takeWhile(_ != -1).foreach(oss.write(buffer, 0, _))
        oss.close()
      }
    }
    toPath
  }
}

object Wget {
  def downloadToLocal(url: String, outputPath: String, existsOk: Boolean) = {
    val isZipped = url.endsWith(".gz") || url.endsWith(".zip") || url.endsWith(".tar")
    val workflow = if (isZipped) new WgetZippedToFiles(outputPath)
    else new WgetToFile(outputPath, false, existsOk)
    workflow.run(url)
  }
}