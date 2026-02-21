package com.paiml.databricks.delta

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.delta.tables.DeltaTable
import java.nio.file.Files

class ChangeDataCaptureSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private def tmpDir(): String =
    Files.createTempDirectory("cdc-test-").toAbsolutePath.toString

  behavior of "ChangeDataCapture"

  it should "detect inserts, updates, and deletes" in {
    val source = Seq(
      (1, "alice", "US"),
      (2, "bob_updated", "UK"),
      (4, "dave", "FR")
    ).toDF("id", "name", "country")

    val target = Seq(
      (1, "alice", "US"),
      (2, "bob", "UK"),
      (3, "charlie", "DE")
    ).toDF("id", "name", "country")

    val changes = ChangeDataCapture.detectChanges(source, target, "id", Seq("name", "country"))
    val types = changes.select("_change_type").as[String].collect().sorted

    types should contain("insert")    // id=4
    types should contain("update")    // id=2
    types should contain("delete")    // id=3
  }

  it should "perform SCD Type 1 merge (overwrite)" in {
    val path = tmpDir()
    val initial = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(initial, path)

    val updates = Seq((2, "robert"), (3, "charlie")).toDF("id", "name")
    val deltaTable = DeltaTable.forPath(spark, path)
    ChangeDataCapture.scdType1Merge(deltaTable, updates, "id")

    val result = DeltaTableOps.readDelta(spark, path)
    result.count() shouldBe 3
    result.filter("id = 2").first().getString(1) shouldBe "robert"
  }

  it should "summarize changes by type" in {
    val changes = Seq(
      (1, "insert"), (2, "update"), (3, "delete"), (4, "insert")
    ).toDF("id", "_change_type")

    val summary = ChangeDataCapture.changeSummary(changes)
    val counts = summary.collect().map(r => r.getString(0) -> r.getLong(1)).toMap
    counts("insert") shouldBe 2
    counts("update") shouldBe 1
    counts("delete") shouldBe 1
  }

  it should "apply a batch of CDC events" in {
    val path = tmpDir()
    val initial = Seq((1, "alice"), (2, "bob"), (3, "charlie")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(initial, path)

    val cdcEvents = Seq(
      (2, "robert", "update"),
      (4, "dave", "insert"),
      (3, "charlie", "delete")
    ).toDF("id", "name", "_change_type")

    val deltaTable = DeltaTable.forPath(spark, path)
    ChangeDataCapture.applyCdcBatch(deltaTable, cdcEvents, "id")

    val result = DeltaTableOps.readDelta(spark, path)
    result.count() shouldBe 3 // 1(alice) + 2(robert) + 4(dave) - 3(charlie)
    result.filter("id = 2").first().getString(1) shouldBe "robert"
    result.filter("id = 3").count() shouldBe 0
  }
}
