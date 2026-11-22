# Reference

| Page | Content |
|---|---|
| [Events](events.md) | Every event written to `saga_log`, and when |
| [Database schema](schema.md) | The four tables, their indexes and constraints |

## The API at a glance

| Area | Calls |
|---|---|
| Declaring | `saga<I>(type) { … }`, `step<O>(name) { … }`, `awaitSignal<S>(name, signal, timeout) { … }`, `execute`, `compensate`, `check`, `checkByLookup`, `logAs`, `idempotent`, `onSignal`, `onCompleted`, `onFailed` |
| Step outcomes | return normally, `StepRejected`, `StepNotSent`, any other exception; `CheckResult.Done(output)`, `NotDone`, `CannotTell` |
| Inside a step | `input`, `key`, `sagaId`, `result(ref)`, `requireResult(ref)`, `idempotencyKey`, `idempotencyUuid` |
| Running | `create`, `run`, `resumeDue`, `signal`, `cancel` |
| Manual actions | `retry`, `markDone`, `abort`, `markCompensated` |
| Reading | `find`, `findStuck`, `history`, `historyByTrace`, `stats` |
| Retention | `purgeFinished` |
| Extension points | `SagaStep`, `SignalStep`, `SagaRepository`, `SagaTransactions`, `SagaSerializer`, `SagaListener` |

The KDoc of each public class and function is the detailed reference: it states what each call does, refuses and throws.
