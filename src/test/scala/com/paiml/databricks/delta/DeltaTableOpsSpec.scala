package com.paiml.databricks.delta

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.delta.tables.DeltaTable
import java.nio.file.Files

class DeltaTableOpsSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private def tmpDir(): String =
    Files.createTempDirectory("delta-test-").toAbsolutePath.toString

  behavior of "DeltaTableOps"

  it should "create and read a Delta table" in {
    val path = tmpDir()
    val df = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    val result = DeltaTableOps.readDelta(spark, path)
    result.count() shouldBe 2
  }

  it should "append data to a Delta table" in {
    val path = tmpDir()
    val df1 = Seq((1, "alice")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df1, path)

    val df2 = Seq((2, "bob")).toDF("id", "name")
    DeltaTableOps.appendToDelta(df2, path)

    val result = DeltaTableOps.readDelta(spark, path)
    result.count() shouldBe 2
  }

  it should "read a specific version via time travel" in {
    val path = tmpDir()
    val df1 = Seq((1, "v0")).toDF("id", "data")
    DeltaTableOps.createDeltaTable(df1, path)

    val df2 = Seq((1, "v1")).toDF("id", "data")
    DeltaTableOps.createDeltaTable(df2, path) // overwrite = version 1

    val v0 = DeltaTableOps.readDeltaVersion(spark, path, 0)
    v0.first().getString(1) shouldBe "v0"

    val v1 = DeltaTableOps.readDeltaVersion(spark, path, 1)
    v1.first().getString(1) shouldBe "v1"
  }

  it should "upsert data with merge" in {
    val path = tmpDir()
    val initial = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(initial, path)

    val updates = Seq((2, "robert"), (3, "charlie")).toDF("id", "name")
    val deltaTable = DeltaTable.forPath(spark, path)
    DeltaTableOps.upsert(deltaTable, updates, "id")

    val result = DeltaTableOps.readDelta(spark, path)
    result.count() shouldBe 3
    result.filter("id = 2").first().getString(1) shouldBe "robert"
  }

  it should "delete rows matching a condition" in {
    val path = tmpDir()
    val df = Seq((1, "alice"), (2, "bob"), (3, "charlie")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    val deltaTable = DeltaTable.forPath(spark, path)
    DeltaTableOps.deleteWhere(deltaTable, "id > 1")

    DeltaTableOps.readDelta(spark, path).count() shouldBe 1
  }

  it should "return transaction history" in {
    val path = tmpDir()
    val df = Seq((1, "alice")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    val deltaTable = DeltaTable.forPath(spark, path)
    val hist = DeltaTableOps.history(deltaTable)
    hist.count() should be >= 1L
    hist.columns should contain("version")
  }

  it should "return current version number" in {
    val path = tmpDir()
    val df = Seq((1, "alice")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    val deltaTable = DeltaTable.forPath(spark, path)
    DeltaTableOps.currentVersion(deltaTable) shouldBe 0L
  }

  it should "update rows matching a condition" in {
    val path = tmpDir()
    val df = Seq((1, "alice", 100), (2, "bob", 200)).toDF("id", "name", "amount")
    DeltaTableOps.createDeltaTable(df, path)

    val deltaTable = DeltaTable.forPath(spark, path)
    DeltaTableOps.updateWhere(deltaTable, "id = 1", Map("amount" -> "999"))

    val result = DeltaTableOps.readDelta(spark, path)
    result.filter("id = 1").first().getAs[Int]("amount") shouldBe 999
  }

  it should "read a Delta table at a timestamp" in {
    val path = tmpDir()
    val df = Seq((1, "v0")).toDF("id", "data")
    DeltaTableOps.createDeltaTable(df, path)

    // Get the timestamp of version 0 from history
    val deltaTable = DeltaTable.forPath(spark, path)
    val hist = DeltaTableOps.history(deltaTable, 1)
    val ts = hist.select("timestamp").first().getTimestamp(0).toString

    val result = DeltaTableOps.readDeltaTimestamp(spark, path, ts)
    result.count() shouldBe 1
    result.first().getString(1) shouldBe "v0"
  }

  it should "vacuum a Delta table" in {
    val path = tmpDir()
    val df = Seq((1, "alice")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    val deltaTable = DeltaTable.forPath(spark, path)
    noException should be thrownBy DeltaTableOps.vacuum(deltaTable, 168.0)
  }

  it should "compact small files" in {
    val path = tmpDir()
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name")
    DeltaTableOps.createDeltaTable(df, path)

    noException should be thrownBy DeltaTableOps.compact(spark, path, 1)
    DeltaTableOps.readDelta(spark, path).count() shouldBe 3
  }
}
