package de.tao.soda.etl.data

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import de.tao.soda.etl.Workflow
import org.xerial.snappy.Snappy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Serializable}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

abstract class Serializer[T] extends Workflow[T, Array[Byte]]

abstract class Deserializer[T] extends Workflow[Array[Byte], T]

class JSONSerializer[T <: Product with Serializable](implicit clazz: Class[T]) extends Serializer[T] {
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: T): Array[Byte] = {
    logger.info(s"JSONFileWriter writing $clazz")

    val ostream = new ByteArrayOutputStream()
    mapper.writeValue(ostream, input)
    ostream.toByteArray

  }
}

class JSONDeserializer[T <: Product with Serializable](implicit clazz: Class[T]) extends Deserializer[T]{
  lazy val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  override def run(input: Array[Byte]) = {
    logger.info(s"JSONDeserializer reading ${clazz.getName} ${input.size} bytes")
    mapper.readValue[T](new ByteArrayInputStream(input), clazz)
  }
}

class GzipCompress extends Workflow[Array[Byte], Array[Byte]]{
  override def run(input: Array[Byte]): Array[Byte] = {
    logger.info(s"Gzip compressing input of size : ${input.size} bytes")
    val presize = input.size
    val bstream = new ByteArrayOutputStream(input.size)
    val gzip = new GZIPOutputStream(bstream)
    gzip.write(input)
    gzip.close()
    val zipped = bstream.toByteArray
    val postsize = zipped.size
    val percent = 100*(presize-postsize)/presize.toDouble
    logger.info(s"Gzip compress ${percent} % ($presize => $postsize) bytes")
    zipped
  }
}

class GzipDecompress extends Workflow[Array[Byte], Array[Byte]]{
  override def run(input: Array[Byte]): Array[Byte] = {
    logger.info(s"Gzip decompressing input of size : ${input.size} bytes")
    val presize = input.size
    val gzip = new GZIPInputStream(new ByteArrayInputStream(input))
    val zipped = gzip.readAllBytes()
    val postsize = zipped.size
    gzip.close()
    val percent = 100*(postsize-presize)/postsize.toDouble
    logger.info(s"Gzip decompress ${percent} % ($presize => $postsize) bytes")
    zipped
  }
}

class SnappyCompress extends Workflow[Array[Byte], Array[Byte]]{
  override def run(input: Array[Byte]): Array[Byte] = {
    logger.info(s"SnappyCompress compressing input of size : ${input.size} bytes")
    val comp = Snappy.compress(input)
    val postsize = comp.size
    val percent = 100*(input.size-postsize)/input.size.toDouble
    logger.info(s"SnappyCompress compresses ${percent} % (${input.size} => $postsize) bytes")
    comp
  }
}

class SnappyDecompress extends Workflow[Array[Byte], Array[Byte]]{
  override def run(input: Array[Byte]): Array[Byte] = {
    logger.info(s"SnappyCompress decompressing input of size : ${input.size} bytes")
    val decom = Snappy.uncompress(input)
    val postsize = decom.size
    val percent = 100*(postsize-input.size)/postsize.toDouble
    logger.info(s"SnappyCompress decompresses ${percent} % ($postsize => ${input.size}) bytes")
    decom
  }
}