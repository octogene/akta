# Saga data

A saga may carry an **input**, and each step an **output**. Both are Kotlin types, read typed by every step, check, compensation and hook.

```kotlin
val booking = saga<BookingRequest>("BOOKING") {                 // the input type
    val hold = step<Hold>("hold-item") {                          // this step's output type
        execute { stock.hold(input.itemId, until = input.end) }  // returns a Hold
        compensate { own -> own?.let { stock.release(it.id) } }  // its own output
    }
    step("notify") { execute { mail.send(input.customerId, holdId = requireResult(hold).id) } }
}
```

## How it is stored

| Value | Where | Written |
|---|---|---|
| Input | `saga.input` | Once, by `create`; never changed |
| A step's output | `saga_step_result.output` | When the step completes, or when a check finds it |
| A signal's value | `saga_signal.value`, then the step's output | When the signal arrives |

All three are text, encoded by one `SagaSerializer` set on the engine: JSON through kotlinx.serialization (`KotlinxJsonSagaSerializer`), so the types need `@Serializable`. The engine checks at startup that it can serialize every type it was given: a missing annotation stops the engine, not a saga in production.

## The input is fixed

Every step sees the request as it was made, not business data as it is when the step runs. A check, a compensation or a replay an hour later builds the same request as the first attempt. That matters for idempotency keys: services refuse a key reused with a different request.

## Size

`SagaConfig.maxDataBytes`, 16 KB by default, per input, output and signal value. Store parameters and identifiers; anything large belongs in a business table, referenced by id.

- An input over the limit is refused by `create`, with nothing written.
- An output over the limit makes the step stuck (`OUTPUT_UNSTORABLE`): the call has happened, so it must not run again.

The input lives on the `saga` row, which is updated about 4 times per saga. PostgreSQL writes a new version of the row at each update, so an input under about 2 KB (stored inside the row) takes several times its size until vacuum reclaims the old versions. Larger values are stored out of line and not copied. Moving the input to a table written once is proposed.

## Personal data

Values are stored only in `saga.input`, `saga_step_result.output` and `saga_signal.value`, never in the history (`saga_log`), in `last_error`, or in listener events. What the history shows about a value is what `logAs` returns: an identifier. Without `logAs`, nothing is shown, whatever the type: a `String` output may be an email or a token, so logging one that is an identifier takes `logAs { it }`. All of it is deleted with the saga by `purgeFinished`.

**Error messages are logged as they are.** When a step throws, its exception's text goes into the history, `last_error` and, for a refusal, `failure_reason`. The engine never puts a value there itself, but the messages are yours, and your clients': `StepRejected("customer jane@example.com is blocked")` logs the address, and some HTTP and SQL clients put the URL, the query or the response body in their messages. Keep personal data out of the messages you throw (name the customer by id), and wrap a client's exception in your own when its message may carry some.

## Changing a type

Running sagas hold data written by the previous version of the code. The default JSON ignores unknown keys:

- **Adding a field:** give it a default value. Both versions read each other's data.
- **Anything else** (a field renamed, its meaning changed): declare a new saga type, and keep the old one until its last saga has finished.

Data that cannot be read stops a saga before any call. It is postponed and retried, so that a version that can read it picks it up.
