# Delta Lake Specification

## Scope
Delta table CRUD, MERGE (upsert), time travel, CDC patterns, and schema evolution.

## Modules
- **DeltaTableOps**: 12 operations (create, append, read, upsert, delete, history, vacuum, compact)
- **ChangeDataCapture**: 5 CDC operations (detect changes, SCD Type 1/2, batch apply, summary)
- **SchemaEvolution**: 7 schema operations (compare, validate, evolve, merge, overwrite, safe check)

## Falsification Targets
- Delta time travel must return correct version data
- Upsert must update existing and insert new rows (no duplicates)
- Delete must remove exactly matching rows
- CDC detection must classify all rows (no uncategorized)
- Schema comparison must be symmetric for add/remove
- Schema evolution must be idempotent (existing columns unchanged)
