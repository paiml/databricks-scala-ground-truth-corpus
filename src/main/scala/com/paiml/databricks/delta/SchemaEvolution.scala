package com.paiml.databricks.delta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/** Schema evolution and enforcement patterns for Delta Lake.
  *
  * Implements schema comparison, migration, and enforcement strategies
  * for evolving Delta tables in production data pipelines.
  */
object SchemaEvolution {

  /** Compares two schemas and returns added, removed, and changed fields. */
  def compareSchemas(
      oldSchema: StructType,
      newSchema: StructType
  ): SchemaChanges = {
    val oldFields = oldSchema.fields.map(f => f.name -> f).toMap
    val newFields = newSchema.fields.map(f => f.name -> f).toMap

    val added = newFields.keySet.diff(oldFields.keySet).toSeq.sorted
    val removed = oldFields.keySet.diff(newFields.keySet).toSeq.sorted
    val common = oldFields.keySet.intersect(newFields.keySet)

    val typeChanged = common.filter { name =>
      oldFields(name).dataType != newFields(name).dataType
    }.toSeq.sorted

    val nullabilityChanged = common.filter { name =>
      oldFields(name).nullable != newFields(name).nullable
    }.toSeq.sorted

    SchemaChanges(added, removed, typeChanged, nullabilityChanged)
  }

  /** Writes data with automatic schema merge enabled. */
  def writeWithSchemaMerge(df: DataFrame, path: String): Unit =
    df.write.format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(path)

  /** Writes data with schema overwrite (replaces entire schema). */
  def writeWithSchemaOverwrite(df: DataFrame, path: String): Unit =
    df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(path)

  /** Validates that a DataFrame conforms to an expected schema.
    *
    * Returns a list of validation errors (empty if valid).
    */
  def validateSchema(df: DataFrame, expectedSchema: StructType): Seq[String] = {
    val actual = df.schema
    val expected = expectedSchema

    val missingCols = expected.fieldNames.diff(actual.fieldNames).map { name =>
      s"Missing column: $name"
    }

    val typeMismatches = expected.fields.flatMap { expectedField =>
      actual.fields.find(_.name == expectedField.name).flatMap { actualField =>
        if (actualField.dataType != expectedField.dataType) {
          Some(s"Type mismatch for ${expectedField.name}: " +
            s"expected ${expectedField.dataType}, got ${actualField.dataType}")
        } else None
      }
    }

    val nullabilityIssues = expected.fields.flatMap { expectedField =>
      actual.fields.find(_.name == expectedField.name).flatMap { actualField =>
        if (!expectedField.nullable && actualField.nullable) {
          Some(s"Nullability mismatch for ${expectedField.name}: expected non-nullable")
        } else None
      }
    }

    missingCols ++ typeMismatches ++ nullabilityIssues
  }

  /** Adds new columns with default values to a DataFrame to match a target schema. */
  def evolveToSchema(
      df: DataFrame,
      targetSchema: StructType
  ): DataFrame = {
    val currentFields = df.schema.fieldNames.toSet
    targetSchema.fields.foldLeft(df) { (accDf, field) =>
      if (currentFields.contains(field.name)) accDf
      else {
        val defaultValue = defaultForType(field.dataType)
        accDf.withColumn(field.name, org.apache.spark.sql.functions.lit(defaultValue).cast(field.dataType))
      }
    }
  }

  /** Returns a reasonable default value for a Spark data type. */
  private def defaultForType(dt: DataType): Any = dt match {
    case StringType    => ""
    case IntegerType   => 0
    case LongType      => 0L
    case DoubleType    => 0.0
    case FloatType     => 0.0f
    case BooleanType   => false
    case _: DecimalType => java.math.BigDecimal.ZERO
    case _             => null
  }

  /** Checks if a schema evolution is safe (only additions, no type changes or removals). */
  def isSafeEvolution(changes: SchemaChanges): Boolean =
    changes.removed.isEmpty && changes.typeChanged.isEmpty
}

/** Represents the differences between two schemas. */
case class SchemaChanges(
    added: Seq[String],
    removed: Seq[String],
    typeChanged: Seq[String],
    nullabilityChanged: Seq[String]
) {
  def hasChanges: Boolean =
    added.nonEmpty || removed.nonEmpty || typeChanged.nonEmpty || nullabilityChanged.nonEmpty
}
