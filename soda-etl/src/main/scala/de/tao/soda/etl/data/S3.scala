package de.tao.soda.etl.data

import de.tao.soda.etl.{DataLoader, DataReader, Generator, InputIdentifier}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{AmazonS3Exception, Bucket, S3Object, S3ObjectSummary}

import java.io.ObjectInputStream
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
    if (dry){
      logger.info(s"S3ObjectReader to read ${classTag} from $bucket/$input")
      default
    }
    else {
      logger.info(s"S3ObjectReader reading ${classTag} from $bucket/$input")
      if (s3.doesObjectExist(bucket, input)){
        val obj: S3Object = s3.getObject(bucket, input)
        val reader = new ObjectInputStream(obj.getObjectContent)
        val data = reader.readObject().asInstanceOf[T]
        reader.close()
        data
      }
      else {
        logger.error(s"S3ObjectReader : Object does not exist : $bucket/$input")
        throw new AmazonS3Exception(s"S3 Object does not exist : $bucket/$input")
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
  bucket: String, default: T, override val region: Regions = Regions.DEFAULT_REGION)
  (implicit val classTag: ClassTag[T]) extends Generator[Iterable[T]] with S3Default {

  override def run(input: Nothing, dry: Boolean) = {
    if (dry){
      logger.info(s"S3BucketReader to read ${classTag} from $bucket")
      Iterable.empty[T]
    }
    else {
      logger.info(s"S3BucketReader reading ${classTag} from $bucket")
      if (s3.doesBucketExistV2(bucket)){
        val olist: util.List[S3ObjectSummary] = s3.listObjects(bucket).getObjectSummaries()
        logger.info(s"S3BucketReader reading ${olist.size()} keys from bucket $bucket")
        val output = new ListBuffer[T]()
        olist.forEach { os =>
          val key = os.getKey
          logger.info(s"S3BucketReader reading object from bucket $bucket : $key")
          val obj: S3Object = s3.getObject(bucket, input)
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

