package com.paiml.databricks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** UDF registration patterns for Databricks.
  *
  * Provides typed UDF factories and batch registration for common string,
  * numeric, and date transformations.
  */
object UdfRegistry {

  /** Normalizes whitespace: trims and collapses multiple spaces to one. */
  val normalizeWhitespace: UserDefinedFunction = udf { (s: String) =>
    if (s == null) null
    else s.trim.replaceAll("\\s+", " ")
  }

  /** Masks PII by replacing all but the last 4 characters with asterisks. */
  val maskPii: UserDefinedFunction = udf { (s: String) =>
    if (s == null || s.length <= 4) s
    else "*" * (s.length - 4) + s.takeRight(4)
  }

  /** Extracts the domain from an email address. */
  val emailDomain: UserDefinedFunction = udf { (email: String) =>
    if (email == null || !email.contains("@")) null
    else email.split("@").last.toLowerCase
  }

  /** Converts a string to title case. */
  val titleCase: UserDefinedFunction = udf { (s: String) =>
    if (s == null) null
    else s.split("\\s+").map(w =>
      if (w.isEmpty) w
      else w.head.toUpper + w.tail.toLowerCase
    ).mkString(" ")
  }

  /** Validates that a string matches a basic phone pattern (digits, dashes, parens, spaces). */
  val isValidPhone: UserDefinedFunction = udf { (phone: String) =>
    if (phone == null) false
    else phone.replaceAll("[\\s\\-\\(\\)\\+]", "").matches("^\\d{7,15}$")
  }

  /** Clamps a numeric value to [min, max]. */
  def clampUdf(minVal: Double, maxVal: Double): UserDefinedFunction = udf { (v: Double) =>
    math.max(minVal, math.min(maxVal, v))
  }

  /** Registers all built-in UDFs with the given SparkSession. */
  def registerAll(spark: SparkSession): Unit = {
    val _ = (
      spark.udf.register("normalize_whitespace", normalizeWhitespace),
      spark.udf.register("mask_pii", maskPii),
      spark.udf.register("email_domain", emailDomain),
      spark.udf.register("title_case", titleCase),
      spark.udf.register("is_valid_phone", isValidPhone)
    )
  }
}
