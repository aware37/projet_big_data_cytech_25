import io.minio.{MinioClient, UploadObjectArgs, PutObjectArgs, MakeBucketArgs, BucketExistsArgs}
import java.nio.file.{Files, Paths}
import java.net.URL
import scala.util.Using



object Main extends App {

    val endpoint = "http://localhost:9000"
    val accessKey = "minio"
    val secretKey = "minio123"
    val bucketName = "nyc-yellow-tripdata"

    val fileURL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    val localURL = "../data/raw/yellow_tripdata_2025-01.parquet"

    // Configuration du client MinIO
    val minioClient = MinioClient.builder()
        .endpoint(endpoint)
        .credentials(accessKey, secretKey)
        .build()

    // Créer le bucket s'il n'existe pas
    val bucketExists = minioClient.bucketExists(
        BucketExistsArgs.builder()
            .bucket(bucketName)
            .build()
    )
    
    if (!bucketExists) {
        minioClient.makeBucket(
            MakeBucketArgs.builder()
                .bucket(bucketName)
                .build()
        )
        println(s"Bucket '$bucketName' créé.")
    } else {
        println(s"Bucket '$bucketName' existe déjà.")
    }

    val reponse = requests.get(fileURL)
    Files.write(Paths.get(localURL), reponse.bytes)

    minioClient.uploadObject(
        UploadObjectArgs.builder()
            .bucket(bucketName)
            .`object`("yellow_tripdata_2025-01.parquet")
            .filename(localURL)
            .contentType("application/octet-stream")
            .build()
    )

    Using.resource(new URL(fileURL).openStream()) { inputStream =>
      val connection = new URL(fileURL).openConnection()
      val size = connection.getContentLengthLong
      
      minioClient.putObject(
        PutObjectArgs.builder()
          .bucket(bucketName)
          .`object`("Stream_yellow_tripdata_2025-01.parquet")
          .stream(inputStream, size, -1)
          .contentType("application/octet-stream")
          .build()
      )

    }
}