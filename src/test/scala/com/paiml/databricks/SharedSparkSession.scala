package com.paiml.databricks

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin trait providing a shared local SparkSession for tests.
  *
  * Creates a single SparkSession with Delta Lake support for the entire
  * test suite and stops it after all tests complete.
  */
trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-warehouse")
    .getOrCreate()

  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }
}
