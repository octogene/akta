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
| `saga_one_running_per_key` (unique, partial: unfinished) | One unfinished saga per business key |
| `saga_log_saga`, `saga_log_trace` | `history`, `historyByTrace` |

```sql title="src/main/resources/com/example/saga/schema.sql"
--8<-- "src/main/resources/com/example/saga/schema.sql"
```
