# QA Checklist — Databricks Scala Ground Truth Corpus

## Domain 1: Spark Fundamentals

### DataFrameOps
- [x] DSGTC-001: withStringLength computes correct lengths
- [x] DSGTC-002: filterRange is inclusive on both bounds
- [x] DSGTC-003: renameColumns handles multiple renames
- [x] DSGTC-004: castColumn changes data type correctly
- [x] DSGTC-005: dropNullsIn only drops rows with nulls in specified columns
- [x] DSGTC-006: fillNulls replaces null with correct defaults
- [x] DSGTC-007: withRowId generates unique IDs
- [x] DSGTC-008: flattenStruct expands nested fields
- [x] DSGTC-009: deduplicateBy keeps first occurrence per key
- [x] DSGTC-010: pivotAggregation produces correct pivot columns
- [x] DSGTC-011: unpivot melts columns into key-value rows
- [x] DSGTC-012: summaryStats returns 5 descriptive rows

### SqlOps
- [x] DSGTC-013: registerTempView makes DataFrame queryable via SQL
- [x] DSGTC-014: parameterizedQuery substitutes parameters correctly
- [x] DSGTC-015: createDatabase is idempotent
- [x] DSGTC-016: describeTable returns schema info
- [x] DSGTC-017: explainQuery returns non-empty plan
- [x] DSGTC-018: dropTable handles nonexistent tables
- [x] DSGTC-124: createTableAs exercises CTAS code path

### UdfRegistry
- [x] DSGTC-019: normalizeWhitespace trims and collapses spaces
- [x] DSGTC-020: maskPii preserves last 4 chars
- [x] DSGTC-021: emailDomain extracts lowercase domain
- [x] DSGTC-022: titleCase converts first letter of each word
- [x] DSGTC-023: isValidPhone accepts valid patterns, rejects invalid
- [x] DSGTC-024: clampUdf clamps to [min, max]
- [x] DSGTC-025: registerAll makes UDFs callable via SQL
- [x] DSGTC-026: normalizeWhitespace property: output is trimmed

### WindowFunctions
- [x] DSGTC-027: withRowNumber assigns sequential numbers per partition
- [x] DSGTC-028: withRank produces ties with gaps
- [x] DSGTC-029: withDenseRank produces ties without gaps
- [x] DSGTC-030: withRunningSum computes cumulative sums
- [x] DSGTC-031: withMovingAverage computes correct sliding mean
- [x] DSGTC-032: withLeadLag provides next/previous values
- [x] DSGTC-033: withPercentRank produces values in [0, 1]
- [x] DSGTC-034: withNtile distributes into N buckets
- [x] DSGTC-035: topKPerPartition ascending selects smallest K
- [x] DSGTC-036: topKPerPartition descending selects largest K

### JoinPatterns
- [x] DSGTC-037: broadcastJoin performs inner join with broadcast hint
- [x] DSGTC-038: antiJoin returns unmatched left rows
- [x] DSGTC-039: semiJoin returns matched left rows without right columns
- [x] DSGTC-040: multiColumnJoin joins on multiple keys
- [x] DSGTC-041: saltedJoin distributes skewed keys and removes salt
- [x] DSGTC-042: scdType2Detect classifies INSERT/UPDATE/NO_CHANGE
- [x] DSGTC-043: crossJoinFiltered applies filter to cartesian product
- [x] DSGTC-044: crossJoinFiltered without filter returns full product

## Domain 2: ML/MLflow

### FeatureEngineering
- [x] DSGTC-045: oneHotEncode produces OHE vector column
- [x] DSGTC-046: minMaxScale scales to [0, 1]
- [x] DSGTC-047: standardScale standardizes to zero mean
- [x] DSGTC-048: bucketize assigns correct bin indices
- [x] DSGTC-049: assembleFeatures creates feature vector
- [x] DSGTC-050: tfidfFeatures extracts TF-IDF from text
- [x] DSGTC-051: imputeMean fills missing with column mean

### PipelineBuilder
- [x] DSGTC-052: classificationPipeline builds logistic regression pipeline
- [x] DSGTC-053: classificationPipeline builds random forest pipeline
- [x] DSGTC-054: classificationPipeline builds GBT pipeline
- [x] DSGTC-055: regressionPipeline builds linear regression pipeline
- [x] DSGTC-056: regressionPipeline builds random forest regressor
- [x] DSGTC-057: regressionPipeline builds GBT regressor
- [x] DSGTC-058: preprocessingPipeline chains indexer+encoder+assembler

### ModelEvaluation
- [x] DSGTC-059: binaryAUROC returns value in [0, 1]
- [x] DSGTC-060: binaryAUPR returns value in [0, 1]
- [x] DSGTC-061: multiclassAccuracy returns value in [0, 1]
- [x] DSGTC-062: multiclassF1 returns value in [0, 1]
- [x] DSGTC-063: regressionRMSE returns non-negative value
- [x] DSGTC-064: regressionR2 returns value <= 1
- [x] DSGTC-065: regressionMAE returns non-negative value
- [x] DSGTC-066: classificationReport contains all 4 metrics
- [x] DSGTC-067: regressionReport contains all 4 metrics

### HyperparamTuning
- [x] DSGTC-068: crossValidate returns best model
- [x] DSGTC-069: bestParams extracts optimal configuration
- [x] DSGTC-070: trainValidationSplit returns model with metrics
- [x] DSGTC-071: binaryClassificationEvaluator defaults to AUROC
- [x] DSGTC-072: multiclassEvaluator accepts custom metric
- [x] DSGTC-073: regressionEvaluator accepts custom metric

## Domain 3: Delta Lake

### DeltaTableOps
- [x] DSGTC-074: createDeltaTable + readDelta roundtrip
- [x] DSGTC-075: appendToDelta adds rows
- [x] DSGTC-076: readDeltaVersion retrieves correct version
- [x] DSGTC-077: upsert merges inserts and updates
- [x] DSGTC-078: deleteWhere removes matching rows
- [x] DSGTC-079: history returns version log
- [x] DSGTC-080: currentVersion returns latest version number
- [x] DSGTC-081: compact reduces file count
- [x] DSGTC-116: updateWhere modifies matching rows
- [x] DSGTC-117: readDeltaTimestamp retrieves version at timestamp
- [x] DSGTC-118: vacuum removes old file versions

### ChangeDataCapture
- [x] DSGTC-082: detectChanges classifies insert/update/delete
- [x] DSGTC-083: scdType1Merge overwrites and inserts
- [x] DSGTC-084: changeSummary counts by type
- [x] DSGTC-085: applyCdcBatch processes mixed events
- [x] DSGTC-119: scdType2Merge closes old records and inserts new versions
- [x] DSGTC-120: applyCdcBatch with inserts only (no deletes)
- [x] DSGTC-121: applyCdcBatch with deletes only (no upserts)

### SchemaEvolution
- [x] DSGTC-086: compareSchemas detects added columns
- [x] DSGTC-087: compareSchemas detects removed columns
- [x] DSGTC-088: compareSchemas detects type changes
- [x] DSGTC-089: compareSchemas detects nullability changes
- [x] DSGTC-090: identical schemas report no changes
- [x] DSGTC-091: validateSchema passes for conforming DataFrame
- [x] DSGTC-092: validateSchema reports missing columns
- [x] DSGTC-093: validateSchema reports type mismatches
- [x] DSGTC-122: validateSchema reports nullability mismatches
- [x] DSGTC-094: evolveToSchema adds default columns
- [x] DSGTC-123: evolveToSchema handles all default types (String, Long, Double, Float, Decimal, Binary)
- [x] DSGTC-095: writeWithSchemaMerge adds new columns
- [x] DSGTC-096: writeWithSchemaOverwrite replaces schema
- [x] DSGTC-097: isSafeEvolution rejects removals and type changes

## Domain 4: Structured Streaming

### StreamProcessor
- [x] DSGTC-098: rateStream creates streaming DataFrame
- [x] DSGTC-099: writeToMemory enables SQL queries on stream
- [x] DSGTC-100: filterAndTimestamp adds processing_time column
- [x] DSGTC-101: deduplicateStream applies watermark
- [x] DSGTC-102: withWatermark sets watermark on event time
- [x] DSGTC-103: jsonFileStream creates streaming reader
- [x] DSGTC-104: csvFileStream creates streaming reader
- [x] DSGTC-105: deltaStream reads from Delta as stream
- [x] DSGTC-125: writeToDelta writes stream to Delta path
- [x] DSGTC-126: writeToConsole writes stream to console output

### WindowedAggregation
- [x] DSGTC-106: tumblingWindowCount produces count/total/average
- [x] DSGTC-107: slidingWindowStats produces 5-metric stats
- [x] DSGTC-108: windowedGroupBy groups by key within windows
- [x] DSGTC-109: windowedMultiMetric aggregates avg metric
- [x] DSGTC-127: windowedMultiMetric exercises all agg branches (count/sum/min/max/default)
- [x] DSGTC-110: tumbling window processes via memory sink

### StreamingJoin
- [x] DSGTC-111: streamStaticJoin enriches stream with static data
- [x] DSGTC-112: enrichWithLookup broadcasts lookup table
- [x] DSGTC-113: stream-static join writes to memory
- [x] DSGTC-114: dynamicLookupJoinFn creates foreachBatch function
- [x] DSGTC-128: dynamicLookupJoinFn lambda body executes join logic
- [x] DSGTC-115: left outer stream-static join preserves unmatched
- [x] DSGTC-129: streamStreamJoin performs watermarked inner join
- [x] DSGTC-130: streamStreamLeftJoin performs watermarked left outer join

---

**Total: 130 falsification tests across 4 domains**
