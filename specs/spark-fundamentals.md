# Spark Fundamentals Specification

## Scope
Core DataFrame transformations, SQL operations, UDFs, window functions, and join strategies for Apache Spark on Databricks.

## Modules
- **DataFrameOps**: 12 transformation functions (filter, rename, cast, pivot, unpivot, dedup)
- **SqlOps**: 9 SQL operations (temp views, parameterized queries, CTAS, explain)
- **UdfRegistry**: 7 UDF factories (whitespace, PII masking, email, phone, clamp)
- **WindowFunctions**: 9 window analytics (rank, dense_rank, running sum, moving avg, top-K)
- **JoinPatterns**: 7 join strategies (broadcast, salted, anti, semi, SCD Type 2, cross)

## Falsification Targets
- DataFrame operations must preserve row count unless filtering/deduplicating
- SQL parameterized queries must substitute all parameters
- UDFs must handle null inputs gracefully (return null, not throw)
- Window functions must respect partition boundaries
- Salted joins must remove salt column from output
