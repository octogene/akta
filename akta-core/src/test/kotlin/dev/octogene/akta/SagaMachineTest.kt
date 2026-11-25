package dev.octogene.akta

import org.junit.jupiter.api.Test
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
 * whole is tested elsewhere (DifferentialTest among others); these pin the rules that are easy to get wrong.
 */
class SagaMachineTest {
    private val config = SagaConfig(maxAttempts = 3)
    private val machine = SagaMachine(config)
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

    private fun saga(stepIndex: Int = 0, status: StepStatus = StepStatus.READY, attempts: Int = 1) = SagaRecord(
        id = Uuid.parse("01900000-0000-7000-8000-000000000001"), type = "T", key = "k", state = SagaState.RUNNING,
        stepIndex = stepIndex, stepStatus = status, attempts = attempts, nextAttemptAt = now, lockedUntil = null,
        lastError = "earlier", traceparent = null, createdAt = now,
    )

    private fun decide(outcome: Outcome, r: SagaRecord = saga(), step: SagaStep<*, *> = call, steps: Int = 2, replay: Boolean = false) =
        machine.decide(r, r.checkFailures, outcome, step, steps, replay, unclaimed = r, now)

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
        val r = saga(stepIndex = 1).copy(cancelRequestedAt = now, cancelReason = "changed my mind")
        val undoing = decide(Outcome.Done(null, null), r).record
        assertEquals(SagaState.COMPENSATING, undoing.state)
        assertEquals(1, undoing.stepIndex)                              // this step included
        assertEquals("Cancelled: changed my mind", undoing.failureReason)
    }

    @Test
    fun `an unknown outcome is checked after the settle delay, or replayed with its key`() {
        val checked = decide(Outcome.Unknown("timeout")).record
        assertEquals(StepStatus.UNKNOWN, checked.stepStatus)
        assertEquals(now + config.settleDelay, checked.nextAttemptAt)

        val replayed = decide(Outcome.Unknown("timeout"), step = idempotentCall).record
        assertEquals(now + config.backoff(1), replayed.nextAttemptAt)  // no settle delay
        assertEquals("timeout; retrying with the same key", replayed.lastError)
    }

    @Test
    fun `a request not sent is retried, unless an earlier attempt may have been applied`() {
        val r = saga().copy(keySince = now)
        val first = decide(Outcome.NotSent("refused"), r, idempotentCall).record
        assertEquals(StepStatus.READY, first.stepStatus)
        assertNull(first.keySince)                                      // the key was not used

        val replay = decide(Outcome.NotSent("refused"), r, idempotentCall, replay = true).record
        assertEquals(StepStatus.UNKNOWN, replay.stepStatus)
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
        fun expired(r: SagaRecord, idempotent: Boolean = false) =
            machine.lockExpired(r.copy(stepStatus = StepStatus.IN_PROGRESS, lockedUntil = since, callAction = r.callAction ?: CallAction.EXECUTE), since, now) { idempotent }

        assertEquals(since + config.settleDelay, expired(saga()).nextAttemptAt)
        assertEquals("Lock expired during the check", expired(saga().copy(checkFailures = 1, callAction = CallAction.CHECK)).lastError)
        assertEquals(since, expired(saga(), idempotent = true).nextAttemptAt)
        assertEquals(StuckReason.CHECK_FAILING, expired(saga().copy(checkFailures = 3, callAction = CallAction.CHECK)).stuckReason)
        assertEquals(StepStatus.UNKNOWN, expired(saga().copy(checkFailures = 3)).stepStatus)   // an execution: checked
        assertNull(expired(saga()).callAction)
        val undo = expired(saga().copy(state = SagaState.COMPENSATING))
        assertEquals(StepStatus.READY, undo.stepStatus)
        assertNull(undo.lockedUntil)
    }

    @Test
    fun `a manual action refused leaves the decision unmade`() {
        assertFailsWith<IllegalArgumentException> { machine.retry(saga(), newKey = false, now, { call }) { false } }
        val expired = machine.stuck(saga(), StuckReason.KEY_EXPIRED, "forgotten", now)
        assertFailsWith<IllegalArgumentException> { machine.retry(expired, newKey = false, now, { call }) { false } }
        assertEquals(1, machine.retry(expired, newKey = true, now, { call }) { false }.record.keyGeneration)

        val refused = machine.stuck(saga(), StuckReason.RECORD_FAILED, "hook", now).copy(failureReason = "refused")
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
            override val outputType = null
        }
        val stopped = machine.stuck(saga().copy(nextAttemptAt = deadline), StuckReason.RESUME_FAILING, "unreadable", now)
            .copy(stuckFrom = StepStatus.WAITING)

        val waiting = machine.retry(stopped, newKey = false, now, { signalStep }) { false }.record
        assertEquals(StepStatus.WAITING, waiting.stepStatus)
        assertEquals(deadline, waiting.nextAttemptAt)
        assertEquals(StepStatus.READY, machine.retry(stopped, newKey = false, now, { signalStep }) { true }.record.stepStatus)
    }

    @Test
    fun `a second cancellation changes nothing`() {
        val first = machine.cancel(saga(status = StepStatus.IN_PROGRESS).copy(lockedUntil = now), "first", now).record
        assertEquals("first", first.cancelReason)                       // a call in flight: compensates later
        assertEquals(SagaState.RUNNING, first.state)
        assertEquals("first", machine.cancel(first, "second", now).record.cancelReason)
    }
}
