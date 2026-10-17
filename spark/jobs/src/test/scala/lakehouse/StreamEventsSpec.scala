package lakehouse

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.time.Instant

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamEventsSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val Topic = "ecommerce.events"

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("stream-events-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    // Giống job thật: ép kiểu lỗi trả về NULL thay vì ném lỗi
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  // Các cột của DataFrame mà Spark đọc từ Kafka (chỉ phần parse dùng tới)
  private val KafkaSchema = StructType(Seq(
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType)))

  // Tin hợp lệ y hệt producer gửi: mọi giá trị là chuỗi, ô trống là null
  private val ValidEvent = Map(
    "event_time"    -> "2019-10-01 00:00:03 UTC",
    "event_type"    -> "view",
    "product_id"    -> "5773203",
    "category_id"   -> "1487580005134238553",
    "category_code" -> "electronics.smartphone",
    "brand"         -> "apple",
    "price"         -> "2.62",
    "user_id"       -> "463240011",
    "user_session"  -> "26dd6e6e-4dac-4778-8d2c-92e149dab885")

  // JSON của một tin hợp lệ, ghi đè một vài trường; giá trị null thành null trong JSON
  private def event(overrides: (String, String)*): String =
    (ValidEvent ++ overrides).map {
      case (k, null) => s""""$k":null"""
      case (k, v)    => s""""$k":"$v""""
    }.mkString("{", ",", "}")

  // DataFrame giống DataFrame đọc từ Kafka: mọi tin ở partition 2, offset tăng dần từ 0
  private def kafka(messages: String*): DataFrame = {
    val rows = messages.zipWithIndex.map { case (m, i) =>
      Row(Option(m).map(_.getBytes(UTF_8)).orNull, Topic, 2, i.toLong, new Timestamp(0L))
    }
    spark.createDataFrame(rows.asJava, KafkaSchema)
  }

  private def parsed(messages: String*): DataFrame = StreamEvents.parse(kafka(messages: _*))

  // Lý do bị loại của từng tin theo thứ tự gửi; "" nghĩa là tin hợp lệ
  private def reasons(messages: String*): Seq[String] =
    parsed(messages: _*).orderBy("kafka_offset").select("reason").collect().map(_.getString(0)).toSeq

  private def columnTypes(df: DataFrame): Seq[(String, DataType)] =
    df.schema.fields.map(f => f.name -> f.dataType).toSeq

  private def ddlTypes(ddl: String): Seq[(String, DataType)] =
    StructType.fromDDL(ddl).fields.map(f => f.name -> f.dataType).toSeq

  // ---------- parse ----------

  test("a valid message has no reason and correctly typed columns") {
    val row = parsed(event()).first()

    row.getAs[String]("reason") shouldBe ""
    row.getAs[Timestamp]("event_time").toInstant shouldBe Instant.parse("2019-10-01T00:00:03Z")
    row.getAs[String]("event_type") shouldBe "view"
    row.getAs[Long]("product_id") shouldBe 5773203L
    row.getAs[Long]("category_id") shouldBe 1487580005134238553L
    row.getAs[java.math.BigDecimal]("price") shouldBe new java.math.BigDecimal("2.62")
    row.getAs[Long]("user_id") shouldBe 463240011L
    row.getAs[String]("user_session") shouldBe "26dd6e6e-4dac-4778-8d2c-92e149dab885"
  }

  test("optional fields may be missing") {
    val message = event("category_id" -> null, "category_code" -> null, "brand" -> null,
      "price" -> null, "user_session" -> null)

    reasons(message) shouldBe Seq("")
  }

  test("a missing required field is rejected with its name") {
    for (field <- Seq("event_time", "event_type", "product_id", "user_id"))
      withClue(field) { reasons(event(field -> null)) shouldBe Seq(field) }
  }

  test("a required field that cannot be cast is rejected with its name") {
    val bad = Seq("event_time" -> "not a time", "product_id" -> "abc", "user_id" -> "abc")

    for ((field, value) <- bad)
      withClue(s"$field=$value") { reasons(event(field -> value)) shouldBe Seq(field) }
  }

  test("an optional field that has a value but cannot be cast is rejected") {
    val bad = Seq(
      "category_id" -> "abc",
      "price"       -> "abc",
      "price"       -> "123456789.00")   // 9 chữ số phần nguyên, vượt DECIMAL(10,2)

    for ((field, value) <- bad)
      withClue(s"$field=$value") { reasons(event(field -> value)) shouldBe Seq(field) }
  }

  test("reasons are joined by commas in a fixed order") {
    reasons(event("price" -> "abc", "user_id" -> null, "event_time" -> null)) shouldBe
      Seq("event_time,user_id,price")
  }

  test("malformed JSON and an empty message are rejected") {
    reasons("not json", null) shouldBe Seq.fill(2)("event_time,event_type,product_id,user_id")
  }

  test("a negative price passes bronze, silver is the one that turns it into NULL") {
    val row = parsed(event("price" -> "-1.50")).first()

    row.getAs[String]("reason") shouldBe ""
    row.getAs[java.math.BigDecimal]("price") shouldBe new java.math.BigDecimal("-1.50")
  }

  // ---------- toBronze / toRejected ----------

  test("toBronze keeps only valid messages, in the schema of bronze.events") {
    val bronze = StreamEvents.toBronze(
      parsed(event(), event("price" -> "abc"), event("user_id" -> null)))

    bronze.count() shouldBe 1
    // Phải khớp câu CREATE TABLE trong IngestEvents, nếu không ghi vào bảng Iceberg sẽ lỗi
    columnTypes(bronze) shouldBe ddlTypes(
      """event_time TIMESTAMP, event_type STRING, product_id BIGINT, category_id BIGINT,
        |category_code STRING, brand STRING, price DECIMAL(10,2), user_id BIGINT,
        |user_session STRING, _source_file STRING, _ingested_at TIMESTAMP""".stripMargin)
  }

  test("toBronze records the Kafka position in _source_file") {
    val bronze = StreamEvents.toBronze(parsed(event("price" -> "abc"), event()))

    bronze.select("_source_file").first().getString(0) shouldBe s"kafka://$Topic/2/1"
  }

  test("toRejected keeps only invalid messages, with the raw text and the reason") {
    val raw      = event("price" -> "abc")
    val rejected = StreamEvents.toRejected(parsed(event(), raw))

    rejected.count() shouldBe 1
    val row = rejected.first()
    row.getAs[String]("raw_value") shouldBe raw
    row.getAs[String]("reason") shouldBe "price"
    row.getAs[Int]("kafka_partition") shouldBe 2
    row.getAs[Long]("kafka_offset") shouldBe 1L
    // Phải khớp câu CREATE TABLE bronze.events_rejected trong StreamEvents
    columnTypes(rejected) shouldBe ddlTypes(
      """raw_value STRING, reason STRING, kafka_partition INT, kafka_offset BIGINT,
        |kafka_timestamp TIMESTAMP, _ingested_at TIMESTAMP""".stripMargin)
  }

  test("every message goes to exactly one of the two tables") {
    val messages = Seq(event(), event("price" -> "abc"), event("user_id" -> null), "not json",
      event("brand" -> null))
    val events = parsed(messages: _*)

    StreamEvents.toBronze(events).count() + StreamEvents.toRejected(events).count() shouldBe
      messages.size.toLong
  }
}
