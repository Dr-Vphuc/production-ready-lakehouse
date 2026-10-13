package lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.format.DateTimeFormatter
import java.time.{YearMonth, ZoneOffset}
import java.util.Locale

object IngestEvents {

  private val Table = "bronze.events"

  // Tên file dạng 2019-Oct
  private val MonthFormat = DateTimeFormatter.ofPattern("yyyy-MMM", Locale.ENGLISH)

  // Đọc mọi cột dạng string, ép kiểu tường minh ở bước select
  private val RawSchema = StructType(Seq(
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session"
  ).map(StructField(_, StringType)))

  def main(args: Array[String]): Unit = {
    require(args.nonEmpty, "Usage: IngestEvents <month> [<month> ...]   vd: 2019-Oct")

    val spark = SparkSession.builder().appName("ingest-events").getOrCreate()
    // Cast sai (vd 'abc' -> BIGINT) sẽ ném lỗi thay vì lặng lẽ trả về NULL
    spark.conf.set("spark.sql.ansi.enabled", "true")


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
      val ym    = YearMonth.parse(month, MonthFormat)
      val start = ym.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant
      val end   = ym.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant
      val path  = s"/data/archive/$month.csv"

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


      // (1) Chốt chặn trước khi ghi: mọi dòng phải có event_time và nằm trong đúng tháng
      val pre = events.agg(
        count(lit(1)).as("rows"),
        count(when(col("event_time").isNull, 1)).as("null_time"),
        count(when(col("event_time") < lit(start) || col("event_time") >= lit(end), 1)).as("out_of_month")
      ).first()
      val rows       = pre.getAs[Long]("rows")
      val nullTime   = pre.getAs[Long]("null_time")
      val outOfMonth = pre.getAs[Long]("out_of_month")
      require(nullTime == 0 && outOfMonth == 0,
        s"$month: $nullTime dòng thiếu event_time, $outOfMonth dòng nằm ngoài [$start, $end)")

      // (2) Ghi: chỉ thay partition của tháng này
      events.writeTo(Table).overwritePartitions()

      // (3) Đối soát sau khi ghi: số dòng trong bảng của tháng này phải bằng số dòng CSV
      val written = spark.table(Table)
        .where(col("event_time") >= lit(start) && col("event_time") < lit(end))
        .count()
      require(written == rows, s"$month: CSV có $rows dòng nhưng bảng có $written dòng")

      println(s"[ingest-events] $month -> $Table: $rows rows OK")
    }


    spark.stop()
  }
}
