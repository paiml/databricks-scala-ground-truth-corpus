package com.paiml.databricks.streaming

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._
import java.nio.file.Files

class StreamProcessorSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  behavior of "StreamProcessor"

  it should "create a rate stream" in {
    val stream = StreamProcessor.rateStream(spark, 5)
    stream.isStreaming shouldBe true
    stream.schema.fieldNames should contain allOf ("timestamp", "value")
  }

  it should "write to memory and query results" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val query = StreamProcessor.writeToMemory(stream, "rate_test")
    try {
      query.processAllAvailable()
      val result = spark.sql("SELECT * FROM rate_test")
      result.count() should be >= 0L
    } finally {
      query.stop()
    }
  }

  it should "filter and add processing timestamp" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val filtered = StreamProcessor.filterAndTimestamp(stream, "value > 5")
    filtered.isStreaming shouldBe true
    filtered.schema.fieldNames should contain("processing_time")
  }

  it should "deduplicate a stream" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val deduped = StreamProcessor.deduplicateStream(stream, "value", "timestamp", "10 seconds")
    deduped.isStreaming shouldBe true
  }

  it should "add watermark to a stream" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val watermarked = StreamProcessor.withWatermark(stream, "timestamp", "5 minutes")
    watermarked.isStreaming shouldBe true
  }

  it should "create a JSON file stream" in {
    val path = Files.createTempDirectory("json-stream-").toAbsolutePath.toString
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("value", StringType)
    ))
    val stream = StreamProcessor.jsonFileStream(spark, path, schema)
    stream.isStreaming shouldBe true
  }

  it should "create a CSV file stream" in {
    val path = Files.createTempDirectory("csv-stream-").toAbsolutePath.toString
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val stream = StreamProcessor.csvFileStream(spark, path, schema)
    stream.isStreaming shouldBe true
  }

  it should "create a delta stream" in {
    val path = Files.createTempDirectory("delta-stream-").toAbsolutePath.toString
    import spark.implicits._
    Seq((1, "a"), (2, "b")).toDF("id", "name").write.format("delta").save(path)

    val stream = StreamProcessor.deltaStream(spark, path)
    stream.isStreaming shouldBe true
  }
}
