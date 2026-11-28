# Admin API for Ktor

`akta-ktor` gives an operators' screen or tool an HTTP API over the engine: the reads to find what needs attention, and the manual actions to resolve it. It adds routes to your Ktor application, inside your own authentication, and installs nothing else.

```kotlin
dependencies {
    implementation("dev.octogene:akta-ktor:0.1.0")      // brings akta-core and Ktor's server core
}
```

## Mounting it

Two functions, so that reading and changing sagas can need different roles:

```kotlin
routing {
    authenticate("operators") {
        route("/admin/sagas") {
            sagaAdminRead(engine)
            authenticate("saga-admins") {                 // a stricter role to change sagas
                sagaAdminActions(engine, actor = { call -> call.principal<UserIdPrincipal>()?.name })
            }
        }
    }
}
```

- **Your application owns security.** The routes run inside whatever `authenticate` blocks you put them in. Without one, anyone who reaches them can retry or abort sagas.
- **The operator comes from your login, never from the request.** `actor` returns who is calling, from the principal; it is recorded with every action in `saga_log`. When it returns `null`, the action answers `401` and changes nothing.
- **Mount only the reads** where operators should look but not act: `sagaAdminRead` alone.
- **It needs no `ContentNegotiation`.** It writes and reads its own JSON, so it works whatever your application installed.

## Reading

| Request | Answers |
|---|---|
| `GET /stats?type=` | Counts and ages of the unfinished sagas: by state and step status, stuck ones by reason, the oldest unfinished, the oldest due. `type` may repeat; none means every type. |
| `GET /stuck?type=&limit=&after=` | Stuck sagas, stuck longest first: `{"sagas": […], "next": "…"}`. Pass `next` as `after` for the following page; it is `null` on the last one. `limit` is 50 by default, at most 500. |
| `GET /by-key?key=&limit=` | The sagas of a business key, newest first: the unfinished one if any, then the finished ones still kept. Where an operator starts from "why is this item stuck?". `limit` is 20 by default, at most 500. The key goes in the query, so a key holding a `/` needs no care: encode it as any query value. |
| `GET /{id}` | One saga: its state and step, `lastError`, `failureReason`, `stuckReason`, the cancellation, and its dates. While a call is in flight, `callAction` says which (`EXECUTE`, `REPLAY`, `CHECK`, `COMPENSATE`); after `RESUME_FAILING`, `stuckFrom` is the status a retry resumes. |
| `GET /{id}/history` | Everything that happened to it, oldest first, with who did each manual action. |
| `GET /trace/{traceId}?limit=` | What one request did to sagas, from its trace id. |

Every saga in a response (`/{id}`, `/stuck`, `/by-key`, and the answer to an action) carries **`actions`**: each manual action, whether the saga takes it now, and if not, why, in the words the action would answer `409` with:

```json
"actions": [
  {"action": "RETRY", "allowed": true, "refusal": null},
  {"action": "RETRY_WITH_NEW_KEY", "allowed": false, "refusal": "Step 'hold' of saga 0193… sends no idempotency key: retry it without newKey"},
  {"action": "MARK_DONE", "allowed": true, "refusal": null},
  …
]
```

| `action` | Request |
|---|---|
| `RETRY` | `POST /{id}/retry` |
| `RETRY_WITH_NEW_KEY` | `POST /{id}/retry` `{"newKey": true}` |
| `MARK_DONE` | `POST /{id}/mark-done` |
| `ABORT` | `POST /{id}/abort` `{"currentStepApplied": false}` |
| `ABORT_CURRENT_STEP_APPLIED` | `POST /{id}/abort` `{"currentStepApplied": true}` |
| `MARK_COMPENSATED` | `POST /{id}/mark-compensated` |
| `CANCEL` | `POST /{id}/cancel` |

They come from the checks the actions themselves make, so a screen that offers only the allowed ones never offers what the engine would refuse. An action that would change nothing (`cancel` on a saga already compensating) is reported refused, saying so. Two things can still make an allowed action answer `409`: the saga moved on since it was read, or the `output` given does not fit the step.

A saga's **input is never in a response**, nor any step output: they may hold personal data. The history shows what `logAs` returns, an identifier, as it does everywhere else.

## Acting

Each action is the engine method of the same name: read [Stuck sagas](stuck-sagas.md) for when to use which. Each answers with the saga after the action. The engine runs the saga on at once, so the answer can take up to 10 seconds when it calls external systems.

| Request | Body | Engine call |
|---|---|---|
| `POST /{id}/retry` | `{"newKey": false}`, or none | `retry` |
| `POST /{id}/mark-done` | `{"output": "STK-1"}`, or none | `markDone` |
| `POST /{id}/abort` | `{"currentStepApplied": true, "output": "STK-1"}` | `abort` |
| `POST /{id}/mark-compensated` | none | `markCompensated` |
| `POST /{id}/cancel` | `{"reason": "customer left"}` | `cancel` |

`output` is the step's output as a **JSON value**: a string, a number or an object, as `KotlinxJsonSagaSerializer` stores it. The engine checks it against the step's type before saving it. Left out, `markDone` keeps the output already saved for the step (after `RECORD_FAILED`), or there is none. For a signal step, `output` is the signal's value, delivered by hand: the step's `onSignal` accepts or refuses it as it would a signal received, so a refused value compensates instead of marking the step done. Left out, the signal the saga already keeps is used. If your engine uses another `SagaSerializer`, call the engine directly for `markDone` and `abort` with an output.

## Errors

Every error is `{"error": "…"}`, with a sentence that says what was wrong.

| Status | When |
|---|---|
| `400` | A malformed id, cursor, `limit` or body |
| `401` | `actor` returned `null` |
| `404` | No saga with that id |
| `409` | The engine refuses the action in the saga's current state (`markDone` on a saga that is not stuck, `cancel` on a finished one…), or the output does not fit the step's type. The message is the engine's. |
