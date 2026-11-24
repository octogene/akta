# Database schema

Four tables, created by the script below. Copy it into a Flyway migration. This page includes the library's own file, so it is always the current version.

| Table | One row per | Written |
|---|---|---|
| `saga` | Saga | At creation, then at each claim and record |
| `saga_step_result` | Completed step | When a step completes; marked when it is compensated |
| `saga_signal` | Signal received, per name | When a signal arrives; read when its step waits |
| `saga_log` | Event | At every change; never updated |

Indexes worth knowing:

| Index | Serves |
|---|---|
| `saga_due` (partial: unfinished, not stuck) | The recovery job's search for due sagas |
| `saga_stuck` (partial: stuck) | `findStuck`, by `stuck_at` |
| `saga_finished` (partial: finished) | The purge, by `updated_at` |
| `saga_one_running_per_key` (unique, partial: unfinished) | One unfinished saga per business key; with `saga_finished_key`, `findByKey` |
| `saga_finished_key` (partial: finished) | `findByKey` for finished sagas. A saga enters it once, when it finishes, and is never updated after |
| `saga_log_saga`, `saga_log_trace` | `history`, `historyByTrace` |

A query by business key names both states, `state IN ('RUNNING', 'COMPENSATING') OR state IN ('COMPLETED', 'FAILED')`, so that PostgreSQL combines the two partial indexes; on `business_key` alone, it scans the whole table.

## Upgrading

The script creates the schema from scratch. A database created by an earlier version needs, in a new migration:

| Since | Migration |
|---|---|
| `failure_reason` (why a saga failed, never overwritten) | `ALTER TABLE saga ADD COLUMN failure_reason text;` |
| `findByKey` | `CREATE INDEX saga_finished_key ON saga (business_key) WHERE state IN ('COMPLETED', 'FAILED');` (on a large table: `CREATE INDEX CONCURRENTLY`, outside a transaction) |

```sql title="akta-exposed/src/main/resources/dev/octogene/akta/schema.sql"
--8<-- "akta-exposed/src/main/resources/dev/octogene/akta/schema.sql"
```
