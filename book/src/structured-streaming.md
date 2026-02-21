# Structured Streaming

## StreamProcessor

readStream/writeStream patterns with watermarks and deduplication.

```scala
import com.paiml.databricks.streaming.StreamProcessor

val stream = StreamProcessor.deltaStream(spark, "/data/events")
val deduped = StreamProcessor.deduplicateStream(stream, "event_id", "timestamp", "10 minutes")
val query = StreamProcessor.writeToDelta(deduped, "/data/processed", "/checkpoints/proc")
```

## WindowedAggregation

Tumbling, sliding, and grouped window aggregations.

```scala
import com.paiml.databricks.streaming.WindowedAggregation

val stats = WindowedAggregation.tumblingWindowCount(
  stream, "timestamp", "5 minutes", "value", "10 minutes")
val sliding = WindowedAggregation.slidingWindowStats(
  stream, "timestamp", "30 seconds", "10 seconds", "value", "1 minute")
```

## StreamingJoin

Stream-static and stream-stream join patterns.

```scala
import com.paiml.databricks.streaming.StreamingJoin

val enriched = StreamingJoin.streamStaticJoin(stream, lookupTable, "customer_id")
val joined = StreamingJoin.streamStreamJoin(
  clicks, impressions, "ad_id", "click_time", "impression_time", 300)
```
