package lakehouse

import org.apache.spark.sql.SparkSession

object IngestEvents {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ingest-events").getOrCreate()

    spark.sql("SHOW NAMESPACES").show()

    spark.stop()
  }
}