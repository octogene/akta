package dev.octogene.akta

import org.junit.jupiter.api.Test
import kotlin.reflect.typeOf
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * The engine's decisions on their own: a row in, a row out, no store, clock or coroutine. The engine's behaviour as a
 * whole is tested elsewhere (the engine's tests, and the stress test against PostgreSQL); these pin the rules that are
 * easy to get wrong.
 */
class SagaMachineTest {
    private val config = SagaConfig(maxAttempts = 3)
    private val now = Instant.parse("2026-11-25T10:00:00Z")
    private val call = object : SagaStep<Unit, String> {
        override val name = "call"
        override val outputType = null
        override suspend fun execute(ctx: SagaContext<Unit>) = "out"
    }
    private val idempotentCall = object : SagaStep<Unit, String> {
        override val name = "keyed"
        override val outputType = null
        override val idempotency = Idempotency(1.hours)
        override suspend fun execute(ctx: SagaContext<Unit>) = "out"
    }

    /** A machine that knows the type "T", with these steps and no hooks. */
    private fun machine(vararg steps: SagaStep<Unit, *>): SagaMachine {
        val definition = SagaDefinition("T", null, steps.toList())
        return SagaMachine(config) { definition }
    }

    private val machine = machine(call, idempotentCall)

    /** The same steps, the idempotent one first: a saga on step 0 calls it. */
    private val idempotentFirst = machine(idempotentCall, call)

    private fun saga(stepIndex: Int = 0, status: StepStatus = StepStatus.READY, attempts: Int = 1) = SagaRecord(
        id = Uuid.parse("01900000-0000-7000-8000-000000000001"), type = "T", key = "k", state = SagaState.RUNNING,
        stepIndex = stepIndex, stepStatus = status, attempts = attempts, nextAttemptAt = now, lockedUntil = null,
        lastError = "earlier", traceparent = null, createdAt = now,
    )

    /** A row in, typed as the engine reads it. */
    private fun SagaRecord.typed() = phase() as SagaPhase.Unfinished

    /** The row a decision writes. */
    private val Decision.record get() = saga.record()

    private fun decide(outcome: Outcome, row: SagaRecord = saga(), machine: SagaMachine = this.machine, replay: Boolean = false) =
        row.typed().let { machine.decide(it, outcome, replay, unclaimed = it, now) }

    private fun stuck(row: SagaRecord, reason: StuckReason, error: String) = machine.stuck(row.typed(), reason, error, now).record()

    @Test
    fun `a step done saves its output and moves on, or completes the saga at the last step`() {
        val moved = decide(Outcome.Done("\"out\"", logged = null))
        assertEquals(listOf<Effect>(Effect.SaveResult("\"out\"")), moved.effects)
        assertEquals(1, moved.record.stepIndex)
        assertEquals(0, moved.record.attempts)
        assertNull(moved.record.lastError)

        assertEquals(SagaState.COMPLETED, decide(Outcome.Done(null, null), saga(stepIndex = 1)).record.state)
    }

    @Test
    fun `a step done after a cancellation was requested is undone too`() {
        val row = saga(stepIndex = 1).copy(cancelRequestedAt = now, cancelReason = "changed my mind")
        val undoing = decide(Outcome.Done(null, null), row).record
        assertEquals(SagaState.COMPENSATING, undoing.state)
        assertEquals(1, undoing.stepIndex)                              // this step included
        assertEquals("Cancelled: changed my mind", undoing.failureReason)
    }

    @Test
    fun `an unknown outcome is checked after the settle delay, or replayed with its key`() {
        val checked = decide(Outcome.Unknown("timeout")).record
        assertEquals(StepStatus.CHECKING, checked.stepStatus)
        assertEquals(now + config.settleDelay, checked.nextAttemptAt)

        val replayed = decide(Outcome.Unknown("timeout"), machine = idempotentFirst).record
        assertEquals(now + config.backoff(1), replayed.nextAttemptAt)  // no settle delay
        assertEquals("timeout; retrying with the same key", replayed.lastError)
    }

    @Test
    fun `a request not sent is retried, unless an earlier attempt may have been applied`() {
        val row = saga().copy(keySince = now)
        val first = decide(Outcome.NotSent("refused"), row, idempotentFirst).record
        assertEquals(StepStatus.READY, first.stepStatus)
        assertNull(first.keySince)                                      // the key was not used

        val replay = decide(Outcome.NotSent("refused"), row, idempotentFirst, replay = true).record
        assertEquals(StepStatus.REPLAYING, replay.stepStatus)
        assertEquals(now, replay.keySince)
    }

    @Test
    fun `attempts are limited`() {
        val stuck = decide(Outcome.NotSent("refused"), saga(attempts = 3)).record
        assertEquals(StuckReason.TOO_MANY_ATTEMPTS, stuck.stuckReason)
        assertEquals(now, stuck.stuckAt)
    }

    @Test
    fun `a refusal undoes the steps before, keeping why`() {
        val undoing = decide(Outcome.Rejected("no stock"), saga(stepIndex = 1)).record
        assertEquals(SagaState.COMPENSATING, undoing.state)
        assertEquals(0, undoing.stepIndex)
        assertEquals("no stock", undoing.failureReason)

        assertEquals(SagaState.FAILED, decide(Outcome.Rejected("no stock")).record.state)   // nothing before
    }

    @Test
    fun `an expired lock is checked, replayed, run again, or stopped after too many checks`() {
        val since = now - 1.minutes
        fun expired(row: SagaRecord, idempotent: Boolean = false) =
            machine.lockExpired(
                row.copy(stepStatus = StepStatus.IN_PROGRESS, lockedUntil = since, callAction = row.callAction ?: CallAction.EXECUTE).typed(), now,
            ) { idempotent }.record()

        assertEquals(since + config.settleDelay, expired(saga()).nextAttemptAt)
        assertEquals("Lock expired during the check", expired(saga().copy(checkFailures = 1, callAction = CallAction.CHECK)).lastError)
        assertEquals(since, expired(saga(), idempotent = true).nextAttemptAt)
        assertEquals(StuckReason.CHECK_FAILING, expired(saga().copy(checkFailures = 3, callAction = CallAction.CHECK)).stuckReason)
        assertEquals(StepStatus.CHECKING, expired(saga().copy(checkFailures = 3)).stepStatus)   // an execution: checked
        assertNull(expired(saga()).callAction)
        val undo = expired(saga().copy(state = SagaState.COMPENSATING, failureReason = "refused", callAction = CallAction.COMPENSATE))
        assertEquals(StepStatus.READY, undo.stepStatus)
        assertNull(undo.lockedUntil)
    }

    @Test
    fun `each step's progress comes from where the saga is and what was saved for it`() {
        val running = saga(stepIndex = 1).phase()
        assertEquals(
            listOf(StepProgress.DONE, StepProgress.CURRENT, StepProgress.PENDING, StepProgress.PENDING),
            machine.progress(running, stepCount = 4, undone = mapOf(0 to false)),
        )
        // Undoing step 0, step 1 undone, step 2 refused: it and step 3 will never run.
        val compensating = saga().copy(state = SagaState.COMPENSATING, failureReason = "refused").phase()
        assertEquals(
            listOf(StepProgress.CURRENT, StepProgress.UNDONE, StepProgress.NOT_APPLIED, StepProgress.NOT_APPLIED),
            machine.progress(compensating, stepCount = 4, undone = mapOf(0 to false, 1 to true)),
        )
        val failed = saga(stepIndex = 0, attempts = 0).copy(state = SagaState.FAILED, failureReason = "refused", lastError = null).phase()
        assertEquals(
            listOf(StepProgress.UNDONE, StepProgress.NOT_APPLIED),
            machine.progress(failed, stepCount = 2, undone = mapOf(0 to true)),
        )
        // A finishing saga has no current step: only its hook is left.
        val finishing = saga(stepIndex = 1).copy(state = SagaState.FINISHING, lastError = null).phase()
        assertEquals(listOf(StepProgress.DONE, StepProgress.DONE), machine.progress(finishing, stepCount = 2, undone = mapOf(0 to false, 1 to false)))
    }

    @Test
    fun `a manual action refused leaves the decision unmade`() {
        assertFailsWith<IllegalArgumentException> { machine.retry(saga().phase(), newKey = false, now) { false } }
        val expired = stuck(saga(stepIndex = 1), StuckReason.KEY_EXPIRED, "forgotten").phase()   // the idempotent step
        assertFailsWith<IllegalArgumentException> { machine.retry(expired, newKey = false, now) { false } }
        assertEquals(1, machine.retry(expired, newKey = true, now) { false }.record.keyGeneration)
        // A fresh key only means something for a step that sends one.
        val notIdempotent = stuck(saga(), StuckReason.TOO_MANY_ATTEMPTS, "down").phase()
        assertFailsWith<IllegalArgumentException> { machine.retry(notIdempotent, newKey = true, now) { false } }

        val refused = stuck(saga(), StuckReason.RECORD_FAILED, "hook").copy(failureReason = "refused").phase()
        assertFailsWith<IllegalArgumentException> { machine.checkMarkDone(refused) }
        assertFailsWith<IllegalArgumentException> { machine.checkAbort(refused, currentStepApplied = true) }
        machine.checkAbort(refused, currentStepApplied = false)
    }

    @Test
    fun `retry after resume failures resumes a waiting signal step with its timeout`() {
        val deadline = now + 30.seconds
        val signalStep = object : SignalStep<Unit, String> {
            override val name = "wait"
            override val signal = "payment"
            override val timeout = 1.hours
            override val checksOnTimeout = true
            override val outputType = typeOf<String>()
        }
        val stopped = stuck(saga().copy(nextAttemptAt = deadline), StuckReason.RESUME_FAILING, "unreadable")
            .copy(stuckFrom = StepStatus.WAITING).phase()

        val machine = machine(signalStep)
        val waiting = machine.retry(stopped, newKey = false, now) { false }.record
        assertEquals(StepStatus.WAITING, waiting.stepStatus)
        assertEquals(deadline, waiting.nextAttemptAt)
        assertEquals(StepStatus.READY, machine.retry(stopped, newKey = false, now) { true }.record.stepStatus)
    }

    @Test
    fun `a claim is ours only if the same call is in flight, not the undo claimed at the same instant`() {
        val lock = now + 35.seconds
        val execution = saga(stepIndex = 1, status = StepStatus.IN_PROGRESS).copy(lockedUntil = lock, callAction = CallAction.EXECUTE)
        assertEquals(true, machine.stillClaimed(execution.phase(), execution.typed()))
        // Its outcome was recorded, the saga cancelled meanwhile undoes it, and the undo was claimed in the same
        // transaction, at the same time, which committed but was reported as failed.
        val undo = execution.copy(state = SagaState.COMPENSATING, callAction = CallAction.COMPENSATE, failureReason = "Cancelled: x")
        assertEquals(false, machine.stillClaimed(undo.phase(), execution.typed()))
        assertEquals(false, machine.stillClaimed(execution.copy(callAction = CallAction.CHECK).phase(), execution.typed()))
    }

    @Test
    fun `a row the engine must never write is refused, not decided on`() {
        // A call in flight without its lock, and a refusal kept by a running saga that is not stuck.
        assertFailsWith<IllegalStateException> { saga(status = StepStatus.IN_PROGRESS).phase() }
        assertFailsWith<IllegalStateException> { saga().copy(failureReason = "refused").phase() }
        // A decision the phase it is given cannot take.
        assertFailsWith<IllegalStateException> { machine.next(saga().copy(state = SagaState.COMPENSATING, failureReason = "x").typed(), 2, now) }
    }

    @Test
    fun `a second cancellation changes nothing`() {
        val first = machine.cancel(saga(status = StepStatus.IN_PROGRESS).copy(lockedUntil = now, callAction = CallAction.EXECUTE).phase(), "first", now).record
        assertEquals("first", first.cancelReason)                       // a call in flight: compensates later
        assertEquals(SagaState.RUNNING, first.state)
        assertEquals("first", machine.cancel(first.phase(), "second", now).record.cancelReason)
    }
}
