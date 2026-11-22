# Use cases

The engine fits operations that change **several systems you do not control**, must end all done or all undone, and must survive a crash halfway. The pages in this section walk through three of them:

| Use case | What it shows |
|---|---|
| [Setting up an item in two systems](item-setup.md) | Two calls without idempotency keys: checks after a timeout, compensation, a synchronous answer |
| [A reservation that waits for a payment](reservation-payment.md) | A step that waits for a Kafka event, a timeout, a customer who cancels |
| [Services with idempotency keys](idempotent-services.md) | Replaying calls safely, and what to confirm with each service first |

## When it fits

- **Two or more external calls** that must all happen, or be undone: stock and booking, a reservation and a payment, an account and its access rights.
- **The external systems may time out**, and some of them offer no idempotency key, but can be searched ("find the entry for this EPC").
- **A user waits for an answer**, and should usually get it within seconds, without losing anything when it takes longer.
- **Pods are stopped and rescheduled** (Kubernetes, rolling deployments), possibly in the middle of a call.
- **What happened must be explainable later**, even when application logs are gone.
- **You already run PostgreSQL** and a periodic job runner, and want no new infrastructure.

## When it does not

| Situation | Better choice |
|---|---|
| One external call, or calls that may fail independently | A plain call, with a retry if it is idempotent |
| Every call goes to your own database | One database transaction |
| Long workflows with branches, parallel steps, many timers | A workflow engine (Temporal or similar); the steps here run one after the other |
| Every external system accepts idempotency keys and you already run a workflow engine | That engine: its main drawback for this case, replaying unknown outcomes, is safe with keys |
| No user waits, and an event-driven flow suits the team | Choreography over Kafka, with an outbox |
| A database other than PostgreSQL | A `SagaRepository` of your own (it needs row locks with skip-locked, and partial indexes) |

!!! tip "The deciding question"
    Can each external system tell you whether a call was applied, either by a lookup or by an idempotency key? If neither, every timeout becomes manual work: the engine stops instead of guessing. That is still safer than guessing, but count the cost.
