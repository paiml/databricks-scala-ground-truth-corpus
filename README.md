# Databricks Scala Ground Truth Corpus

<div align="center">

![Scala](https://img.shields.io/badge/Scala-2.12-red?style=for-the-badge&logo=scala)
![Spark](https://img.shields.io/badge/Spark-3.5.4-orange?style=for-the-badge&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.3.0-blue?style=for-the-badge)
![Coverage](https://img.shields.io/badge/Coverage-95%25+-brightgreen?style=for-the-badge)
![License](https://img.shields.io/badge/License-Apache_2.0-blue?style=for-the-badge)

**Production-ready Scala patterns for Databricks course content**

*Popperian falsification methodology with comprehensive test coverage*

</div>

---

| | |
|---|---|
| **Language** | Scala 2.12.20 |
| **Framework** | Apache Spark 3.5.4 + Delta Lake 3.3.0 |
| **Domains** | 4 (Spark, ML, Delta Lake, Streaming) |
| **Test Coverage** | 95%+ (enforced) |
| **Methodology** | Popperian Falsification + TDD |

---

## Overview

Ground truth corpus implementing idiomatic Scala patterns for Apache Spark, MLlib, Delta Lake, and Structured Streaming on Databricks. Each module provides:

- **Production-ready implementations** with comprehensive ScalaDoc
- **Property-based tests** via ScalaCheck for pure functions
- **Integration tests** with shared local SparkSession
- **Golden oracle outputs** for Popperian falsification

---

## Repository Structure

```
databricks-scala-ground-truth-corpus/
├── build.sbt                    # sbt build configuration
├── project/
│   ├── build.properties         # sbt version
│   └── plugins.sbt              # scoverage, scalafmt, wartremover
├── src/
│   ├── main/scala/com/paiml/databricks/
│   │   ├── spark/               # Domain 1: Spark Fundamentals
│   │   │   ├── DataFrameOps.scala
│   │   │   ├── SqlOps.scala
│   │   │   ├── UdfRegistry.scala
│   │   │   ├── WindowFunctions.scala
│   │   │   └── JoinPatterns.scala
│   │   ├── ml/                  # Domain 2: ML/MLflow
│   │   │   ├── FeatureEngineering.scala
│   │   │   ├── PipelineBuilder.scala
│   │   │   ├── ModelEvaluation.scala
│   │   │   └── HyperparamTuning.scala
│   │   ├── delta/               # Domain 3: Delta Lake
│   │   │   ├── DeltaTableOps.scala
│   │   │   ├── ChangeDataCapture.scala
│   │   │   └── SchemaEvolution.scala
│   │   └── streaming/           # Domain 4: Structured Streaming
│   │       ├── StreamProcessor.scala
│   │       ├── WindowedAggregation.scala
│   │       └── StreamingJoin.scala
│   └── test/scala/com/paiml/databricks/
│       ├── SharedSparkSession.scala
│       ├── spark/               # 5 spec files
│       ├── ml/                  # 4 spec files
│       ├── delta/               # 3 spec files
│       └── streaming/           # 3 spec files
├── oracle/                      # Golden outputs for falsification
├── specs/                       # Domain specifications
├── book/                        # mdBook documentation
├── pmat.toml                    # PMAT quality configuration
├── CLAUDE.md                    # Claude Code guidelines
└── Makefile                     # Quality gate tiers
```

---

## Domains

### Domain 1: Spark Fundamentals

| Module | Functions | Purpose |
|--------|-----------|---------|
| `DataFrameOps` | 12 | DataFrame transformations (filter, rename, pivot, unpivot) |
| `SqlOps` | 9 | Spark SQL operations (views, parameterized queries, CTAS) |
| `UdfRegistry` | 7 | UDF patterns (PII masking, validation, normalization) |
| `WindowFunctions` | 9 | Window analytics (rank, running sum, moving avg, top-K) |
| `JoinPatterns` | 7 | Join strategies (broadcast, salted, SCD Type 2, anti/semi) |

### Domain 2: ML/MLflow

| Module | Functions | Purpose |
|--------|-----------|---------|
| `FeatureEngineering` | 7 | Feature transforms (OHE, scaling, TF-IDF, imputation) |
| `PipelineBuilder` | 4 | MLlib pipeline construction (classification, regression) |
| `ModelEvaluation` | 9 | Evaluation metrics (AUROC, F1, RMSE, R2, reports) |
| `HyperparamTuning` | 8 | CV, train-val split, evaluator factories |

### Domain 3: Delta Lake

| Module | Functions | Purpose |
|--------|-----------|---------|
| `DeltaTableOps` | 12 | CRUD, MERGE, time travel, vacuum, compaction |
| `ChangeDataCapture` | 5 | CDC detection, SCD Type 1/2, batch processing |
| `SchemaEvolution` | 7 | Schema comparison, validation, safe evolution |

### Domain 4: Structured Streaming

| Module | Functions | Purpose |
|--------|-----------|---------|
| `StreamProcessor` | 10 | readStream/writeStream, watermarks, deduplication |
| `WindowedAggregation` | 4 | Tumbling, sliding, grouped windowed aggregation |
| `StreamingJoin` | 5 | Stream-static, stream-stream, broadcast enrichment |

---

## Quick Start

### Prerequisites

```bash
# Install Scala toolchain via Coursier
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz \
  | gzip -d > cs && chmod +x cs && ./cs setup --yes
```

### Build and Test

```bash
git clone https://github.com/paiml/databricks-scala-ground-truth-corpus
cd databricks-scala-ground-truth-corpus

# Compile
sbt compile

# Run all tests
sbt test

# Run specific domain tests
sbt "testOnly com.paiml.databricks.spark.*"
sbt "testOnly com.paiml.databricks.delta.*"

# Coverage report
sbt clean coverage test coverageReport
open target/scala-2.12/scoverage-report/index.html
```

### Quality Gates

```bash
make tier1    # Format check + compile (<30s)
make tier2    # + all tests (<5min)
make tier3    # + coverage check (<10min)
make tier4    # + docs (full CI)
```

---

## Quality Standards

| Metric | Target | Enforced |
|--------|--------|----------|
| Test coverage | 95% | sbt-scoverage `coverageFailOnMinimum` |
| Cyclomatic complexity | ≤15 | pmat.toml |
| SATD comments | 0 | pmat.toml |
| TDG grade | A | pmat.toml |
| Compiler warnings | 0 | `-Xlint -Ywarn-*` flags |

---

## References

- [Apache Spark 3.5 Documentation](https://spark.apache.org/docs/3.5.4/)
- [Delta Lake 3.3 Documentation](https://docs.delta.io/3.3.0/)
- [Spark MLlib Guide](https://spark.apache.org/docs/3.5.4/ml-guide.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/3.5.4/structured-streaming-programming-guide.html)
- Popper, K. (1959). The Logic of Scientific Discovery

---

## License

Apache 2.0 - See [LICENSE](LICENSE) file
