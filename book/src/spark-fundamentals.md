# Spark Fundamentals

## DataFrameOps

Core DataFrame transformations including filtering, projection, aggregation, pivot/unpivot, and schema manipulation.

```scala
import com.paiml.databricks.spark.DataFrameOps

val result = DataFrameOps.filterRange(df, "price", 10.0, 100.0)
val pivoted = DataFrameOps.pivotAggregation(df, "region", "product", "revenue")
```

## SqlOps

Spark SQL operations with temp view management and parameterized queries.

```scala
import com.paiml.databricks.spark.SqlOps

SqlOps.registerTempView(df, "sales")
val result = SqlOps.parameterizedQuery(spark,
  "SELECT * FROM sales WHERE region = '{region}'",
  Map("region" -> "US"))
```

## UdfRegistry

Typed UDF factories for string normalization, PII masking, and validation.

```scala
import com.paiml.databricks.spark.UdfRegistry

UdfRegistry.registerAll(spark)
val masked = df.select(UdfRegistry.maskPii($"ssn"))
```

## WindowFunctions

Analytical window functions: ranking, running sums, moving averages, top-K per partition.

```scala
import com.paiml.databricks.spark.WindowFunctions

val ranked = WindowFunctions.withDenseRank(df, "department", "salary")
val topSellers = WindowFunctions.topKPerPartition(df, "region", "revenue", 5)
```

## JoinPatterns

Join strategies including broadcast, salted (skew handling), SCD Type 2, and anti/semi joins.

```scala
import com.paiml.databricks.spark.JoinPatterns

val enriched = JoinPatterns.broadcastJoin(orders, customers, "customer_id")
val newOnly = JoinPatterns.antiJoin(incoming, existing, "id")
```
