package de.tao.soda.etl.data

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, S3Object, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import de.tao.soda.etl._
import de.tao.soda.etl.data.OutputIdentifier.$

import java.io.{ByteArrayInputStream, File, InputStream, ObjectInputStream}
import java.util
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

trait S3Default {
  val region: Regions
  final lazy val s3: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withCredentials(new ProfileCredentialsProvider)
    .withRegion(region)
    .build()

  def bucketExists(bucket: String): Boolean = s3.doesBucketExistV2(bucket)
}


/**
 * Read a specific object from a specific bucket
 * @param bucket
 * @param default only used when dry run
 * @param region
 * @param classTag
 * @tparam T
 */
class ReadS3Object[T <:Product with Serializable](
  bucket: String, override val region: Regions = Regions.DEFAULT_REGION)
  (implicit val classTag: ClassTag[T]) extends DataLoader[T] with S3Default {

  override def run(input: String) = {
    val fullURI = s"s3://$bucket/$input"
    logger.info(s"ReadS3Object reading ${classTag} from $fullURI (region=${s3.getRegionName})")
    if (s3.doesObjectExist(bucket, input)){
      val obj: S3Object = s3.getObject(bucket, input)
      val reader = new ObjectInputStream(obj.getObjectContent)
      val data = reader.readObject().asInstanceOf[T]
      reader.close()
      data
    }
    else {
      logger.error(s"ReadS3Object : Object does not exist : $fullURI")
      throw new AmazonS3Exception(s"S3 Object does not exist : $fullURI")
    }
  }
}

// todo: DRY
class ReadS3ZippedObject[T <:Product with Serializable](
  bucket: String, override val region: Regions = Regions.DEFAULT_REGION)
  (implicit val classTag: ClassTag[T]) extends DataLoader[T] with S3Default {

  override def run(input: String) = {
    val fullURI = s"s3://$bucket/$input"
    logger.info(s"ReadS3ZippedObject reading ${classTag} from $fullURI (region=${s3.getRegionName})")
    if (s3.doesObjectExist(bucket, input)){
      val obj: S3Object = s3.getObject(bucket, input)
      val reader = new ObjectInputStream(new GZIPInputStream(obj.getObjectContent))
      val data = reader.readObject().asInstanceOf[T]
      reader.close()
      data
    }
    else {
      logger.error(s"ReadS3ZippedObject : Object does not exist : $fullURI")
      throw new AmazonS3Exception(s"S3 Object does not exist : $fullURI")
    }
  }
}

class ReadS3ObjectAsStream(bucket: String, encoding: String, override val region: Regions = Regions.DEFAULT_REGION)
extends DataLoader[InputIdentifier] with S3Default {

  override def run(input: String): InputIdentifier = {
    val fullURI = s"s3://$bucket/$input"
    logger.info(s"ReadS3ObjectAsStream reading input stream ($encoding) from $fullURI (region=${s3.getRegionName})")
    if (s3.doesObjectExist(bucket, input)){
      val obj: S3Object = s3.getObject(bucket, input)
      StreamIdentifier(obj.getObjectContent, encoding)
    }
    else {
      logger.error(s"ReadS3ObjectAsStream : Object does not exist : $fullURI")
      throw new AmazonS3Exception(s"S3 Object does not exist : $fullURI")
    }
  }
}

/**
 * Reads all objects from the specified,
 * assuming all objects have the same schema
 * @param bucket
 * @param default
 * @param region
 * @param classTag
 * @tparam T
 */
class ReadS3Bucket[T <:Product with Serializable](
  bucket: String, key: String, override val region: Regions = Regions.DEFAULT_REGION)
  (implicit val classTag: ClassTag[T]) extends Generator[Iterable[T]] with S3Default {

  override def run(input: Unit): Iterable[T] = {
    val fullURI = s"s3://$bucket/$key"
    logger.info(s"ReadS3Bucket reading ${classTag} from $fullURI (region=${s3.getRegionName})")
    if (s3.doesBucketExistV2(bucket)){
      val olist: util.List[S3ObjectSummary] = s3.listObjects(bucket).getObjectSummaries()
      logger.info(s"ReadS3Bucket reading ${olist.size()} keys from bucket $bucket")
      val output = new ListBuffer[T]()
      olist.forEach { os =>
        val key = os.getKey
        logger.info(s"ReadS3Bucket reading object from bucket $bucket : $key")
        val obj: S3Object = s3.getObject(bucket, key)
        val reader = new ObjectInputStream(obj.getObjectContent)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        output.addOne(data)
      }
      output.toList
    }
    else {
      logger.error(s"ReadS3Bucket : Bucket does not exist : $bucket")
      throw new AmazonS3Exception(s"S3 Bucket does not exist : $bucket")
    }
  }
}

class WriteS3Object[T <: Product with Serializable](
  bucket: String,  key: OutputIdentifier, override val region: Regions = Regions.DEFAULT_REGION, overwrite: Boolean = true)
  (implicit val classTag: ClassTag[T]) extends DataWriter[T] with S3Default {

  import OutputIdentifier._

  override def run(input: T): InputIdentifier = {
    val objname = key.toString
    val fullURI = s"s3://$bucket/$objname"
    logger.info(s"WriteS3Object writing ${classTag} to $fullURI (region=${s3.getRegionName})")

    if (!s3.doesBucketExistV2(bucket)){
      logger.info(s"WriteS3Object trying to create bucket s3://${bucket}")
      s3.createBucket(bucket)
    }

    if (s3.doesObjectExist(bucket, objname) && !overwrite){
      logger.error(s"WriteS3Object will not overwrite existing $fullURI")
      throw new AmazonS3Exception(s"S3 key $fullURI already exists")
    }
    // Write object to temp file
    val tempFile = java.io.File.createTempFile("soda", s"$bucket-$key")
    logger.info(s"WriteS3Object writing to temp file : ${tempFile.getAbsolutePath}")
    logger.info(s"WriteS3Object uploading $classTag to $fullURI")

    WriteAsObject($(tempFile.getAbsolutePath)).run(input)
    s3.putObject(bucket, objname, tempFile)

    tempFile.delete()
    PathIdentifier(fullURI)
  }
}

// todo: DRY
class WriteS3ZippedObject[T <: Product with Serializable](
  bucket: String,  key: OutputIdentifier, override val region: Regions = Regions.DEFAULT_REGION, overwrite: Boolean = true)
  (implicit val classTag: ClassTag[T]) extends DataWriter[T] with S3Default {

  import OutputIdentifier._

  override def run(input: T): InputIdentifier = {
    val objname = key.toString
    val fullURI = s"s3://$bucket/$objname"
    logger.info(s"WriteS3ZippedObject writing ${classTag} to $fullURI (region=${s3.getRegionName})")

    if (!s3.doesBucketExistV2(bucket)){
      logger.info(s"WriteS3ZippedObject trying to create bucket s3://${bucket}")
      s3.createBucket(bucket)
    }

    if (s3.doesObjectExist(bucket, objname) && !overwrite){
      logger.error(s"WriteS3ZippedObject will not overwrite existing $fullURI")
      throw new AmazonS3Exception(s"S3 key $fullURI already exists")
    }
    // Zip object to temp file
    val tempFile = java.io.File.createTempFile("soda", s"$bucket-$objname")
    logger.info(s"WriteS3ZippedObject writing to temp file : ${tempFile.getAbsolutePath}")
    logger.info(s"WriteS3ZippedObject uploading $classTag to $fullURI")

    WriteAsZippedObject($(tempFile.getAbsolutePath)).run(input)
    s3.putObject(bucket, objname, tempFile)

    tempFile.delete()
    PathIdentifier(objname)
  }
}

class UploadToS3(bucket: String, key: OutputIdentifier, override val region: Regions = Regions.DEFAULT_REGION, overwrite: Boolean = true)
extends DataWriter[InputIdentifier] with S3Default {

  override def run(input: InputIdentifier) = {
    val objname = key.toString
    val fullURI = s"s3://$bucket/$objname"
    logger.info(s"UploadToS3 uploading $input to $fullURI (region=${s3.getRegionName})")

    if (!s3.doesBucketExistV2(bucket)){
      logger.info(s"UploadToS3 trying to create bucket s3://${bucket}")
      s3.createBucket(bucket)
    }

    if (s3.doesObjectExist(bucket, objname) && !overwrite){
      logger.error(s"UploadToS3 will not overwrite existing $fullURI")
      throw new AmazonS3Exception(s"S3 key $fullURI already exists")
    }

    input match {
      case PathIdentifier(s, _) =>
        val file = new File(s)
        s3.putObject(bucket, objname, file)

      case StreamIdentifier(s, _) =>
        s3.putObject(bucket, objname, s, new ObjectMetadata())

      case SourceIdentifier(s) =>
        val sreader = s.reader()
        val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
        val bstream = new ByteArrayInputStream(bytes)
        logger.info(s"UploadToS3 uploading ${bytes.length} bytes")
        s3.putObject(bucket, objname, bstream, new ObjectMetadata())
    }
    PathIdentifier(objname)
  }
}

// todo: ACL for created bucket & object
