package de.tao.soda.etl.data

import de.tao.soda.etl.{DataDescriptor, DataReader}
import org.apache.commons.io.FileExistsException

import java.io.{File, FileOutputStream, InputStream}
import java.net.URL
import java.util.zip.ZipInputStream

class WgetStream(url: String) extends DataReader[InputStream] {
  override def run(input: DataDescriptor[InputStream], dry: Boolean): InputStream = {
    new URL(url).openStream()
  }
}

class WgetZippedStream(url: String) extends DataReader[InputStream] {
  override def run(input: DataDescriptor[InputStream], dry: Boolean): InputStream = {
    new ZipInputStream(new URL(url).openStream())
  }
}

class WgetToFile(url: String, toPath: String, append: Boolean=false, existsOk: Boolean=true) extends DataReader[String] {
  override def run(input: DataDescriptor[String], dry: Boolean): String = {
    if (!existsOk && new File(toPath).exists()){
      throw new FileExistsException(s"File ${toPath} already exists. Download aborted")
    }
    else {
      val ins = new URL(url).openStream()
      val oss = new FileOutputStream(toPath, append)
      ins.transferTo(oss)
      toPath
    }
  }
}

class WgetZippedToFiles(url: String, toPath: String, append: Boolean=false, existsOk: Boolean=true) extends DataReader[String] {
  override def run(input: DataDescriptor[String], dry: Boolean): String = {
    val outDir = new File(toPath)
    if (!existsOk && outDir.exists() && outDir.isDirectory){
      throw new FileExistsException(s"Directory ${toPath} already exists. Download aborted")
    }
    else {
      val ins = new ZipInputStream(new URL(url).openStream())
      LazyList.continually(ins.getNextEntry).takeWhile(_ != null).foreach { file =>
        if (!file.isDirectory){
          // ensure parent directories exist
          val outFullPath = outDir.toPath.resolve(file.getName)
          if (!outFullPath.getParent.toFile.exists())
            outFullPath.getParent.toFile.mkdirs()

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