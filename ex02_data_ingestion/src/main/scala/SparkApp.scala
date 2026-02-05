import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.SaveMode

object SparkApp extends App {


  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000/") // A changer lors du déploiement
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "6000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  val inputPath = "s3a://nyc-yellow-tripdata/yellow_tripdata_2025-01.parquet"
  val parquetFileDF = spark.read.parquet(inputPath)
  println(s"Lignes lues : ${parquetFileDF.count()}")


  // Branche 1
  val mlDF = parquetFileDF
  .filter(col("trip_distance") > 0.0)
  .filter(col("passenger_count") > 0)
  .filter(col("total_amount") >= 0.0)
  .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
  .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
  .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))

  val outputPath = "s3a://nyc-yellow-tripdata/cleaned/yellow_tripdata_2025-01"

  mlDF.write
    .mode(SaveMode.Overwrite)
    .parquet(outputPath)

  println("[Branche 1] Insertion terminée dans S3.")
  println(s"Lignes lues : ${mlDF.count()}")


  // Branche 2
  val dwDF = parquetFileDF
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("tpep_pickup_datetime", "tpep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "tpep_dropoff_datetime")
    .withColumnRenamed("passenger_count", "passenger_count")
    .withColumnRenamed("trip_distance", "trip_distance")
    .withColumnRenamed("RatecodeID", "rate_code_id")
    .withColumnRenamed("store_and_fwd_flag", "store_and_fwd_flag")
    .withColumnRenamed("PULocationID", "pu_location_id")
    .withColumnRenamed("DOLocationID", "do_location_id")
    .withColumnRenamed("payment_type", "payment_type_id")
    .withColumnRenamed("fare_amount", "fare_amount")
    .withColumnRenamed("extra", "extra")
    .withColumnRenamed("mta_tax", "mta_tax")
    .withColumnRenamed("tip_amount", "tip_amount")
    .withColumnRenamed("tolls_amount", "tolls_amount")
    .withColumnRenamed("improvement_surcharge", "improvement_surcharge")
    .withColumnRenamed("total_amount", "total_amount")
    .withColumnRenamed("congestion_surcharge", "congestion_surcharge")
    .withColumnRenamed("Airport_fee", "airport_fee")
    .withColumnRenamed("cbd_congestion_fee", "cbd_congestion_fee")

  val postgresUrl = "jdbc:postgresql://localhost:5432/bigdata_db"
  val dbProps = new Properties()

  dbProps.put("user", "postgres")
  dbProps.put("password", "postgres")
  dbProps.put("driver", "org.postgresql.Driver")
  dbProps.put("batchsize", "5000")

  println("   Insertion en cours dans 'fact_trips'...")
  
  dwDF.write
    .mode(SaveMode.Append)
    .jdbc(postgresUrl, "fact_trips", dbProps)

  println("[Branche 2] Insertion terminée dans Postgres.")
  println(s"Lignes lues : ${dwDF.count()}")

  spark.stop()

}

