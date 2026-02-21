# Delta Lake

## DeltaTableOps

CRUD, MERGE (upsert), time travel, and table maintenance.

```scala
import com.paiml.databricks.delta.DeltaTableOps

DeltaTableOps.createDeltaTable(df, "/data/events")
val v0 = DeltaTableOps.readDeltaVersion(spark, "/data/events", 0)
DeltaTableOps.upsert(deltaTable, updates, "id")
```

## ChangeDataCapture

CDC detection, SCD Type 1/2 merges, and batch event processing.

```scala
import com.paiml.databricks.delta.ChangeDataCapture

val changes = ChangeDataCapture.detectChanges(source, target, "id", Seq("name", "email"))
ChangeDataCapture.scdType1Merge(deltaTable, updates, "id")
ChangeDataCapture.applyCdcBatch(deltaTable, cdcEvents, "id")
```

## SchemaEvolution

Schema comparison, validation, and safe evolution strategies.

```scala
import com.paiml.databricks.delta.SchemaEvolution

val changes = SchemaEvolution.compareSchemas(oldSchema, newSchema)
if (SchemaEvolution.isSafeEvolution(changes)) {
  SchemaEvolution.writeWithSchemaMerge(df, path)
}
```
