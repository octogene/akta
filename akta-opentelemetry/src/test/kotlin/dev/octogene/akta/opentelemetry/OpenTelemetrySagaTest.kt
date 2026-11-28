package dev.octogene.akta.opentelemetry

import dev.octogene.akta.KotlinxJsonSagaSerializer
import dev.octogene.akta.SagaConfig
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.StepNotSent
import dev.octogene.akta.StepRejected
import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import dev.octogene.akta.saga
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class OpenTelemetrySagaTest {
    private val spans = InMemorySpanExporter.create()
    private val metrics = InMemoryMetricReader.create()
    private val openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
        .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metrics).build())
        .build()
    private val http = openTelemetry.getTracer("an-http-client")

    private val repository = InMemorySagaRepository()
    private val transactions = InMemorySagaTransactions(repository)
    private var stockDown = false

    private val order = saga("ORDER") {
        step<String>("hold") {
            execute {
                // What an instrumented HTTP client does: a span, under whatever is current.
                http.spanBuilder("POST /holds").startSpan().end()
                if (stockDown) throw StepNotSent("stock unavailable") else "H-1"
            }
            compensate { }
        }
        step("pay") { execute { throw StepRejected("card declined") } }
    }

    private val engine = SagaEngine(
        transactions, repository, listOf(order), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 1),
        traceId = ::openTelemetryTraceId,
        listeners = listOf(OpenTelemetrySagaListener(openTelemetry)),
        tracer = OpenTelemetrySagaTracer(openTelemetry),
    )

    private fun named(name: String): SpanData = spans.finishedSpanItems.single { it.name == name }

    /** Creates the saga in a request's span, as an HTTP handler would; runs it there too if [runThere]. */
    private fun startInRequest(key: String, runThere: Boolean): Pair<Uuid, SpanData> {
        val request = openTelemetry.getTracer("an-http-server").spanBuilder("POST /orders").startSpan()
        val id = runBlocking(Context.current().with(request).asContextElement()) {
            val id = transactions.inTransaction { engine.create(order, key, traceparent = currentTraceparent()) }
            if (runThere) engine.run(id, Clock.System.now() + 10.seconds)
            id
        }
        request.end()
        return id to named("POST /orders")
    }

    @Test
    fun `a run from a request traces each call under it, and the step's own calls under the call`() {
        val (id, request) = startInRequest("order:1", runThere = true)

        val hold = named("saga.execute ORDER hold")
        assertEquals(request.traceId, hold.traceId)
        assertEquals(request.spanId, hold.parentSpanId)
        assertEquals(hold.spanId, named("POST /holds").parentSpanId)                   // the HTTP call, under it
        assertEquals(id.toString(), hold.attributes.get(AttributeKey.stringKey("saga.id")))
        assertEquals("order:1", hold.attributes.get(AttributeKey.stringKey("saga.key")))
        assertEquals("EXECUTE", hold.attributes.get(AttributeKey.stringKey("saga.action")))
        assertEquals(1L, hold.attributes.get(AttributeKey.longKey("saga.attempt")))
        assertEquals("Done", hold.attributes.get(AttributeKey.stringKey("saga.outcome")))
        assertEquals(StatusCode.UNSET, hold.status.statusCode)

        // A refusal is not an error of the call: the saga compensates, as it should.
        val pay = named("saga.execute ORDER pay")
        assertEquals("Rejected", pay.attributes.get(AttributeKey.stringKey("saga.outcome")))
        assertEquals(StatusCode.UNSET, pay.status.statusCode)
        assertNotNull(named("saga.compensate ORDER hold"))

        // The history names the trace, so that a saga leads to it.
        val history = runBlocking { engine.history(id) }
        assertEquals(request.traceId, history.single { it.event == "EXECUTE:Done" }.traceId)
    }

    @Test
    fun `a run with no request around it starts a trace of its own, linked to the one that created the saga`() {
        val (id, request) = startInRequest("order:2", runThere = false)
        runBlocking { engine.run(id, Clock.System.now() + 10.seconds) }                  // as the recovery job does

        val hold = named("saga.execute ORDER hold")
        assertFalse(hold.parentSpanContext.isValid)                                       // a root
        assertTrue(hold.traceId != request.traceId)
        assertEquals(listOf(request.traceId to request.spanId), hold.links.map { it.spanContext.traceId to it.spanContext.spanId })
        // Its history entry names its own trace.
        assertEquals(hold.traceId, runBlocking { engine.history(id) }.single { it.event == "EXECUTE:Done" }.traceId)
    }

    @Test
    fun `a failed call sets its span's status to error, with its error`() {
        stockDown = true
        startInRequest("order:3", runThere = true)
        val hold = named("saga.execute ORDER hold")
        assertEquals("NotSent", hold.attributes.get(AttributeKey.stringKey("saga.outcome")))
        assertEquals(StatusCode.ERROR, hold.status.statusCode)
        assertTrue(hold.status.description.contains("stock unavailable"), hold.status.description)
    }

    @Test
    fun `the history is counted by type and event, and the stats are gauges`() {
        stockDown = true
        startInRequest("order:4", runThere = true)                                       // stuck: TOO_MANY_ATTEMPTS
        val counted = metrics.collectAllMetrics().single { it.name == "saga.events" }.longSumData.points
            .associate { it.attributes.get(AttributeKey.stringKey("saga.event")) to it.value }
        assertEquals(1L, counted["EXECUTE:NotSent"])

        runBlocking {
            val sampling = sagaGauges(engine, openTelemetry, this, period = 10.milliseconds)
            delay(100)
            sampling.cancel()
        }
        val stuck: Map<String?, Long> = metrics.collectAllMetrics().single { it.name == "saga.stuck" }.longGaugeData.points
            .associate { it.attributes.get(AttributeKey.stringKey("saga.stuck_reason")) to it.value }
        assertEquals(mapOf<String?, Long>("TOO_MANY_ATTEMPTS" to 1L), stuck)
    }

    @Test
    fun `without the key, spans carry no saga key`() {
        val quiet = SagaEngine(
            transactions, repository, listOf(order), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 1),
            tracer = OpenTelemetrySagaTracer(openTelemetry, withKey = false),
        )
        runBlocking { quiet.run(transactions.inTransaction { quiet.create(order, "customer:jane") }, Clock.System.now() + 10.seconds) }
        assertTrue(spans.finishedSpanItems.filter { it.name.startsWith("saga.") }.none { it.attributes.get(AttributeKey.stringKey("saga.key")) != null })
    }
}
