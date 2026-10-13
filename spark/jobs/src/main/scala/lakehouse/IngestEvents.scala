package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IngestEvents {

  private val Table = "bronze.events"

  // Đọc mọi cột dạng string, ép kiểu tường minh ở bước select
  private val RawSchema = StructType(Seq(
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session"
  ).map(StructField(_, StringType)))

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Usage: IngestEvents <month> [<month> ...]   vd: 2019-Oct")

    val spark = SparkSession.builder().appName("ingest-events").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $Table (
         |  event_time    TIMESTAMP,
         |  event_type    STRING,
         |  product_id    BIGINT,
         |  category_id   BIGINT,
         |  category_code STRING,
         |  brand         STRING,
         |  price         DECIMAL(10,2),
         |  user_id       BIGINT,
         |  user_session  STRING,
         |  _source_file  STRING,
         |  _ingested_at  TIMESTAMP
         |) USING iceberg
         |PARTITIONED BY (months(event_time))""".stripMargin)

    args.foreach { month =>
      val path = s"/data/archive/$month.csv"

      val events = spark.read
        .option("header", "true")
        .option("enforceSchema", "false")   // báo lỗi nếu header CSV lệch với RawSchema
        .schema(RawSchema)
        .csv(path)
        .select(
          col("event_time").cast(TimestampType).as("event_time"),
          col("event_type"),
          col("product_id").cast(LongType).as("product_id"),
          col("category_id").cast(LongType).as("category_id"),
          col("category_code"),
          col("brand"),
          col("price").cast(DecimalType(10, 2)).as("price"),
          col("user_id").cast(LongType).as("user_id"),
          col("user_session"),
          col("_metadata.file_path").as("_source_file"),
          current_timestamp().as("_ingested_at")
        )

      events.writeTo(Table).overwritePartitions()
      println(s"[ingest-events] $month -> $Table: done")
    }

    spark.stop()
  }
}
