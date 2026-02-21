package com.paiml.databricks.delta

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.apache.spark.sql.types._
import java.nio.file.Files

class SchemaEvolutionSpec
    extends AnyFlatSpec
    with Matchers
    with SharedSparkSession
    with ScalaCheckPropertyChecks {

  import spark.implicits._

  private def tmpDir(): String =
    Files.createTempDirectory("schema-test-").toAbsolutePath.toString

  behavior of "SchemaEvolution"

  it should "detect added columns" in {
    val old = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val newer = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("email", StringType)
    ))
    val changes = SchemaEvolution.compareSchemas(old, newer)
    changes.added shouldBe Seq("email")
    changes.removed shouldBe empty
    changes.typeChanged shouldBe empty
  }

  it should "detect removed columns" in {
    val old = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val newer = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val changes = SchemaEvolution.compareSchemas(old, newer)
    changes.removed shouldBe Seq("age")
    changes.added shouldBe empty
  }

  it should "detect type changes" in {
    val old = StructType(Seq(StructField("id", IntegerType), StructField("score", IntegerType)))
    val newer = StructType(Seq(StructField("id", IntegerType), StructField("score", DoubleType)))
    val changes = SchemaEvolution.compareSchemas(old, newer)
    changes.typeChanged shouldBe Seq("score")
  }

  it should "detect nullability changes" in {
    val old = StructType(Seq(StructField("id", IntegerType, nullable = false)))
    val newer = StructType(Seq(StructField("id", IntegerType, nullable = true)))
    val changes = SchemaEvolution.compareSchemas(old, newer)
    changes.nullabilityChanged shouldBe Seq("id")
  }

  it should "report no changes for identical schemas" in {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val changes = SchemaEvolution.compareSchemas(schema, schema)
    changes.hasChanges shouldBe false
  }

  it should "validate a conforming DataFrame" in {
    val df = Seq((1, "alice")).toDF("id", "name")
    val expected = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val errors = SchemaEvolution.validateSchema(df, expected)
    errors shouldBe empty
  }

  it should "report missing columns" in {
    val df = Seq(1).toDF("id")
    val expected = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val errors = SchemaEvolution.validateSchema(df, expected)
    errors should have length 1
    errors.head should include("Missing column: name")
  }

  it should "report type mismatches" in {
    val df = Seq("1").toDF("id")
    val expected = StructType(Seq(StructField("id", IntegerType)))
    val errors = SchemaEvolution.validateSchema(df, expected)
    errors should have length 1
    errors.head should include("Type mismatch")
  }

  it should "evolve a DataFrame to match a target schema" in {
    val df = Seq((1, "alice")).toDF("id", "name")
    val target = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("active", BooleanType)
    ))
    val result = SchemaEvolution.evolveToSchema(df, target)
    result.columns should contain allOf ("id", "name", "age", "active")
  }

  it should "write with schema merge" in {
    val path = tmpDir()
    val df1 = Seq((1, "alice")).toDF("id", "name")
    df1.write.format("delta").save(path)

    val df2 = Seq((2, "bob", 30)).toDF("id", "name", "age")
    noException should be thrownBy SchemaEvolution.writeWithSchemaMerge(df2, path)

    val result = spark.read.format("delta").load(path)
    result.columns should contain("age")
  }

  it should "write with schema overwrite" in {
    val path = tmpDir()
    val df1 = Seq((1, "alice")).toDF("id", "name")
    df1.write.format("delta").save(path)

    val df2 = Seq((1, 100)).toDF("key", "value")
    noException should be thrownBy SchemaEvolution.writeWithSchemaOverwrite(df2, path)

    val result = spark.read.format("delta").load(path)
    result.columns should contain allOf ("key", "value")
    result.columns should not contain "name"
  }

  it should "identify safe schema evolution (additions only)" in {
    val safe = SchemaChanges(added = Seq("email"), removed = Seq.empty,
      typeChanged = Seq.empty, nullabilityChanged = Seq.empty)
    SchemaEvolution.isSafeEvolution(safe) shouldBe true

    val unsafe = SchemaChanges(added = Seq.empty, removed = Seq("name"),
      typeChanged = Seq.empty, nullabilityChanged = Seq.empty)
    SchemaEvolution.isSafeEvolution(unsafe) shouldBe false
  }
}
