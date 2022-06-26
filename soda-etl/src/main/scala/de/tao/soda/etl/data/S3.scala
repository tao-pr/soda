package de.tao.soda.etl.data

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, S3Object, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import de.tao.soda.etl._

import java.io.{ByteArrayInputStream, File, ObjectInputStream}
import java.util
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

trait S3Default {
  val region: Regions
  final lazy val s3: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(region).build()
}


/**
 * Read a specific object from a specific bucket
 * @param bucket
 * @param default only used when dry run
 * @param region
 * @param classTag
 * @tparam T
 */
class S3ObjectReader[T <:Product with Serializable](
  bucket: String, default: T, override val region: Regions = Regions.DEFAULT_REGION)
(implicit val classTag: ClassTag[T]) extends DataLoader[T] with S3Default {

  override def run(input: String, dry: Boolean) = {
    val fullURI = s"s3://$bucket/$input"
    if (dry){
      logger.info(s"S3ObjectReader to read ${classTag} from $fullURI (region=${s3.getRegionName})")
      default
    }
    else {
      logger.info(s"S3ObjectReader reading ${classTag} from $fullURI (region=${s3.getRegionName})")
      if (s3.doesObjectExist(bucket, input)){
        val obj: S3Object = s3.getObject(bucket, input)
        val reader = new ObjectInputStream(obj.getObjectContent)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        data
      }
      else {
        logger.error(s"S3ObjectReader : Object does not exist : $fullURI")
        throw new AmazonS3Exception(s"S3 Object does not exist : $fullURI")
      }
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
class S3BucketReader[T <:Product with Serializable](
  bucket: String, key: String, default: T, override val region: Regions = Regions.DEFAULT_REGION)
  (implicit val classTag: ClassTag[T]) extends Generator[Iterable[T]] with S3Default {

  override def run(input: Unit, dry: Boolean): Iterable[T] = {
    val fullURI = s"s3://$bucket/$key"
    if (dry){
      logger.info(s"S3BucketReader to read ${classTag} from $fullURI (region=${s3.getRegionName})")
      Iterable.empty[T]
    }
    else {
      logger.info(s"S3BucketReader reading ${classTag} from $fullURI (region=${s3.getRegionName})")
      if (s3.doesBucketExistV2(bucket)){
        val olist: util.List[S3ObjectSummary] = s3.listObjects(bucket).getObjectSummaries()
        logger.info(s"S3BucketReader reading ${olist.size()} keys from bucket $bucket")
        val output = new ListBuffer[T]()
        olist.forEach { os =>
          val key = os.getKey
          logger.info(s"S3BucketReader reading object from bucket $bucket : $key")
          val obj: S3Object = s3.getObject(bucket, key)
          val reader = new ObjectInputStream(obj.getObjectContent)
          val data = reader.readObject().asInstanceOf[T]
          reader.close()
          output.addOne(data)
        }
        output.toList
      }
      else {
        logger.error(s"S3BucketReader : Bucket does not exist : $bucket")
        throw new AmazonS3Exception(s"S3 Bucket does not exist : $bucket")
      }
    }
  }
}

class S3Writer[T <: Product with Serializable](
  bucket: String,  key: String, override val region: Regions = Regions.DEFAULT_REGION, overwrite: Boolean = true)
  (implicit val classTag: ClassTag[T]) extends DataWriter[T] with S3Default {

  override def run(input: T, dry: Boolean): String = {
    val fullURI = s"s3://$bucket/$key"
    if (dry) {
      logger.info(s"S3Writer to write ${classTag} to $fullURI (region=${s3.getRegionName})")
    }
    else {
      logger.info(s"S3Writer writing ${classTag} to $fullURI (region=${s3.getRegionName})")
    }

    if (!s3.doesBucketExistV2(bucket)){
      logger.info(s"S3Writer trying to create bucket s3://${bucket}")
      if (!dry)
        s3.createBucket(bucket)
    }

    if (s3.doesObjectExist(bucket, key) && !overwrite){
      logger.error(s"S3Writer will not overwrite existing $fullURI")
      throw new AmazonS3Exception(s"S3 key $fullURI already exists")
    }
    // Write object to temp file
    val tempFile = java.io.File.createTempFile("soda", s"$bucket-$key")
    logger.info(s"S3Writer writing to temp file : ${tempFile.getAbsolutePath}")
    logger.info(s"S3Writer uploading $classTag to $fullURI")

    if (!dry){
      ObjectWriter(tempFile.getAbsolutePath).run(input)
      s3.putObject(bucket, key, tempFile)
    }

    tempFile.delete()
    fullURI
  }
}

class S3Uploader(bucket: String, key: String, override val region: Regions = Regions.DEFAULT_REGION, overwrite: Boolean = true)
extends DataWriter[InputIdentifier] with S3Default {

  override def run(input: InputIdentifier, dry: Boolean) = {
    val fullURI = s"s3://$bucket/$key"
    if (dry){
      logger.info(s"S3Uploader to upload $input to $fullURI (region=${s3.getRegionName})")
    }
    else {
      logger.info(s"S3Uploader uploading $input to $fullURI (region=${s3.getRegionName})")
    }

    if (!s3.doesBucketExistV2(bucket)){
      logger.info(s"S3Uploader trying to create bucket s3://${bucket}")
      if (!dry)
        s3.createBucket(bucket)
    }

    if (s3.doesObjectExist(bucket, key) && !overwrite){
      logger.error(s"S3Uploader will not overwrite existing $fullURI")
      throw new AmazonS3Exception(s"S3 key $fullURI already exists")
    }

    if (!dry){
      input match {
        case PathIdentifier(s, _) =>
          val file = new File(s)
          s3.putObject(bucket, key, file)

        case SourceIdentifier(s) =>
          val sreader = s.reader()
          val bytes = LazyList.continually(sreader.read).takeWhile(_ != -1).map(_.toByte).toArray
          val bstream = new ByteArrayInputStream(bytes)
          logger.info(s"S3Uploader uploading ${bytes.length} bytes")
          s3.putObject(bucket, key, bstream, new ObjectMetadata())
      }
    }

    fullURI

  }
}
