package dev.octogene.akta.console

import dev.octogene.akta.ManualAction
import dev.octogene.akta.ManualActionCheck
import dev.octogene.akta.OutputNeed
import dev.octogene.akta.SagaLogEntry
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaStats
import dev.octogene.akta.SagaStepProgress
import dev.octogene.akta.StepProgress
import kotlinx.html.ButtonType
import kotlinx.html.DIV
import kotlinx.html.FlowContent
import kotlinx.html.FormMethod
import kotlinx.html.InputType
import kotlinx.html.MAIN
import kotlinx.html.a
import kotlinx.html.body
import kotlinx.html.button
import kotlinx.html.code
import kotlinx.html.dd
import kotlinx.html.details
import kotlinx.html.div
import kotlinx.html.dl
import kotlinx.html.dt
import kotlinx.html.form
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.head
import kotlinx.html.header
import kotlinx.html.html
import kotlinx.html.id
import kotlinx.html.input
import kotlinx.html.label
import kotlinx.html.lang
import kotlinx.html.li
import kotlinx.html.link
import kotlinx.html.main
import kotlinx.html.meta
import kotlinx.html.nav
import kotlinx.html.ol
import kotlinx.html.p
import kotlinx.html.pre
import kotlinx.html.script
import kotlinx.html.section
import kotlinx.html.span
import kotlinx.html.stream.createHTML
import kotlinx.html.summary
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.textArea
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.title
import kotlinx.html.tr
import kotlinx.html.ul
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

/*
 * The console's HTML, built with kotlinx.html: every text it writes is escaped, attributes included. No saga input
 * or step output is ever shown, only what saga_log and the saga row show (logAs references, errors, reasons).
 */

internal const val HTMX = "htmx-2.0.11.min.js"

/** A message above the saga, after an action: what happened, or why the engine refused it. */
internal class Flash(val ok: Boolean, val text: String)

/** Everything the saga's page shows. [steps] is null for a type this engine does not know. */
internal class SagaPanel(
    val saga: SagaRecord,
    val steps: List<SagaStepProgress>?,
    val actions: List<ManualActionCheck>,
    val history: List<SagaLogEntry>,
)

internal fun page(base: String, title: String, content: MAIN.() -> Unit): String = "<!doctype html>\n" + createHTML().html {
    lang = "en"
    head {
        meta(charset = "utf-8")
        meta(name = "viewport", content = "width=device-width, initial-scale=1")
        // No inline style or script, and nothing evaluated: the Content-Security-Policy allows neither.
        meta(name = "htmx-config", content = """{"includeIndicatorStyles":false,"allowEval":false,"allowScriptTags":false}""")
        title { +title }
        link(rel = "stylesheet", href = "$base/assets/console.css")
        script(src = "$base/assets/$HTMX") { defer = true }
    }
    body {
        header {
            nav {
                a(href = base) { +"Sagas" }
                a(href = "$base/queue") { +"Stuck" }
            }
            form(action = "$base/search", method = FormMethod.get, classes = "search") {
                input(type = InputType.search, name = "q") {
                    placeholder = "Business key, saga id or trace id"
                    required = true
                    attributes["aria-label"] = "Search"
                }
                button(type = ButtonType.submit) { +"Search" }
            }
        }
        main { content() }
    }
}

internal fun MAIN.overview(base: String, stats: SagaStats, now: Instant) {
    h1 { +"Sagas" }
    section {
        h2 { +"Stuck" }
        if (stats.stuckByReason.isEmpty()) {
            p(classes = "quiet") { +"No saga is stuck." }
        } else {
            table {
                thead { tr { th { +"Reason" }; th(classes = "number") { +"Sagas" } } }
                tbody {
                    for ((reason, count) in stats.stuckByReason.entries.sortedByDescending { it.value }) {
                        tr {
                            td { a(href = "$base/queue#$reason") { +reason.name } }
                            td(classes = "number") { +count.toString() }
                        }
                    }
                }
            }
        }
    }
    section {
        h2 { +"Recovery" }
        dl {
            dt { +"Behind by" }
            dd { +(stats.oldestDueAt?.let { "${shortly(now - it)} (the oldest saga due)" } ?: "nothing due") }
            dt { +"Oldest unfinished" }
            dd { +(stats.oldestUnfinishedAt?.let { at(it, now) } ?: "none") }
        }
    }
    section {
        h2 { +"Unfinished" }
        if (stats.unfinished.isEmpty()) {
            p(classes = "quiet") { +"None." }
        } else {
            table {
                thead { tr { th { +"State" }; th { +"Step" }; th(classes = "number") { +"Sagas" } } }
                tbody {
                    for ((status, count) in stats.unfinished.entries.sortedWith(compareBy({ it.key.first }, { it.key.second }))) {
                        tr {
                            td { +status.first.name }
                            td { +status.second.name }
                            td(classes = "number") { +count.toString() }
                        }
                    }
                }
            }
        }
    }
}

internal fun MAIN.queue(base: String, sagas: List<SagaRecord>, next: String?, now: Instant) {
    h1 { +"Stuck sagas" }
    if (sagas.isEmpty()) {
        p(classes = "quiet") { +"No saga is stuck." }
        return
    }
    p(classes = "quiet") { +"Stuck longest first, grouped by reason: sagas stuck for the same cause are often handled together." }
    for ((reason, group) in sagas.groupBy { it.stuckReason }) {
        section {
            h2 { id = reason?.name ?: "UNKNOWN"; +"${reason?.name ?: "?"} (${group.size})" }
            sagaTable(base, group, now, stuck = true)
        }
    }
    if (next != null) p { a(href = "$base/queue?after=$next") { +"Next page" } }
}

internal fun MAIN.searchResults(base: String, query: String, sagas: List<SagaRecord>, now: Instant) {
    h1 { +"Search" }
    p {
        +"Sagas for "
        code { +query }
        +": by business key, newest first, then those a trace touched."
    }
    if (sagas.isEmpty()) p(classes = "quiet") { +"None found." } else sagaTable(base, sagas, now, stuck = false)
}

private fun FlowContent.sagaTable(base: String, sagas: List<SagaRecord>, now: Instant, stuck: Boolean) {
    table {
        thead {
            tr {
                th { +(if (stuck) "Stuck since" else "Created") }
                th { +"Type" }
                th { +"Key" }
                th { +"State" }
                th { +"Last error" }
            }
        }
        tbody {
            for (saga in sagas) {
                tr {
                    td { a(href = "$base/saga/${saga.id}") { +at(if (stuck) saga.stuckAt ?: saga.createdAt else saga.createdAt, now) } }
                    td { +saga.type }
                    td { code { +saga.key } }
                    td { +listOfNotNull(saga.state.name, saga.stepStatus.name, saga.stuckReason?.name).joinToString(" · ") }
                    td(classes = "error") { +(saga.lastError?.let { if (it.length > 140) it.take(140) + "…" else it } ?: "") }
                }
            }
        }
    }
}

/** The saga's page, as one block htmx swaps after an action. [canAct]: the operator may change sagas. */
internal fun FlowContent.sagaPanel(base: String, panel: SagaPanel, canAct: Boolean, flash: Flash?, now: Instant) {
    div { sagaBlock(base, panel, canAct, flash, now) }
}

/** The same block alone, as the answer to an action: htmx puts it in place of the one on the page. */
internal fun sagaFragment(base: String, panel: SagaPanel, canAct: Boolean, flash: Flash?, now: Instant): String =
    createHTML().div { sagaBlock(base, panel, canAct, flash, now) }

private fun DIV.sagaBlock(base: String, panel: SagaPanel, canAct: Boolean, flash: Flash?, now: Instant) {
    val saga = panel.saga
    run {
        id = "saga"
        if (flash != null) p(classes = if (flash.ok) "flash ok" else "flash refused") { +flash.text }
        h1 {
            +"${saga.type} "
            code { +saga.key }
        }
        p(classes = "badges") {
            span(classes = "badge") { +saga.state.name }
            span(classes = "badge") { +saga.stepStatus.name }
            saga.stuckReason?.let { span(classes = "badge stuck") { +it.name } }
            span(classes = "quiet") {
                +" Saga "
                code { +saga.id.toString() }
                +", created ${at(saga.createdAt, now)}"
            }
        }
        steps(panel.steps, saga)
        happened(saga, panel.steps, now)
        if (canAct) actions(base, panel, now)
        history(panel.history, now)
    }
}

private fun FlowContent.steps(steps: List<SagaStepProgress>?, saga: SagaRecord) {
    section {
        h2 { +"Steps" }
        if (steps == null) {
            p(classes = "quiet") { +"This engine does not know the type ${saga.type}: its steps cannot be shown." }
            return@section
        }
        ol(classes = "steps") {
            for (step in steps) {
                li(classes = "step ${step.progress.name.lowercase()}") {
                    span(classes = "mark") { +mark(step.progress) }
                    span(classes = "name") { +step.name }
                    step.ref?.let { span(classes = "ref") { +"ref=$it" } }
                    span(classes = "progress") {
                        +(
                            if (step.progress ==
                                StepProgress.CURRENT
                            ) {
                                "current, ${saga.attempts} attempt(s)"
                            } else {
                                step.progress.name.lowercase().replace('_', ' ')
                            }
                            )
                    }
                }
            }
        }
    }
}

private fun mark(progress: StepProgress) = when (progress) {
    StepProgress.DONE -> "✔"
    StepProgress.CURRENT -> "●"
    StepProgress.PENDING -> "○"
    StepProgress.UNDONE -> "↶"
    StepProgress.NOT_APPLIED -> "–"
}

private fun FlowContent.happened(saga: SagaRecord, steps: List<SagaStepProgress>?, now: Instant) {
    section {
        h2 { +"What happened" }
        val current = steps?.firstOrNull { it.progress == StepProgress.CURRENT }?.name ?: "step ${saga.stepIndex}"
        saga.stuckReason?.let { reason ->
            val (what, check) = guidance(reason, current)
            p { +what }
            p(classes = "check") { +check }
        }
        dl {
            saga.lastError?.let {
                dt { +"Last error" }
                dd { pre { +it } }
            }
            saga.failureReason?.let {
                dt { +"Failed because" }
                dd { +it }
            }
            saga.cancelReason?.let {
                dt { +"Cancelled" }
                dd { +"$it (${saga.cancelRequestedAt?.let { at -> at(at, now) } ?: ""})" }
            }
            saga.lockedUntil?.let {
                dt { +"Call in flight" }
                dd { +"${saga.callAction ?: "a call"}, its lock until ${at(it, now)}" }
            }
            if (!saga.state.isFinished && saga.stuckReason == null && saga.lockedUntil == null) {
                dt { +"Next attempt" }
                dd { +at(saga.nextAttemptAt, now) }
            }
        }
    }
}

private fun FlowContent.actions(base: String, panel: SagaPanel, now: Instant) {
    val saga = panel.saga
    val allowed = panel.actions.filter { it.allowed }
    val output = panel.steps?.firstOrNull { it.progress == StepProgress.CURRENT }?.output
    section {
        h2 { +"What to do" }
        if (allowed.isEmpty()) p(classes = "quiet") { +"No action can be taken now." }
        for (check in allowed) {
            val action = check.action
            val consequence = consequence(action, saga, panel.steps)
            form(classes = "action") {
                attributes["hx-post"] = "$base/saga/${saga.id}/${action.slug}"
                attributes["hx-target"] = "#saga"
                attributes["hx-swap"] = "outerHTML"
                attributes["hx-confirm"] = "${action.label}: $consequence"
                attributes["hx-disabled-elt"] = "find button"
                button(type = ButtonType.submit) { +action.label }
                p(classes = "consequence") { +consequence }
                when (action) {
                    ManualAction.MARK_DONE -> outputField(output?.markDone, output?.type, output?.saved == true, output?.signalKept == true)
                    ManualAction.ABORT_CURRENT_STEP_APPLIED -> outputField(output?.abort, output?.type, output?.saved == true, signalKept = false)
                    ManualAction.CANCEL -> label {
                        +"Reason "
                        input(type = InputType.text, name = "reason") { required = true }
                    }
                    else -> Unit
                }
            }
        }
        val refused = panel.actions.filter { !it.allowed }
        if (refused.isNotEmpty()) {
            details {
                summary { +"Not available now (${refused.size})" }
                ul {
                    for (check in refused) {
                        li {
                            span(classes = "name") { +check.action.label }
                            +": ${check.refusal}"
                        }
                    }
                }
            }
        }
    }
}

/** The output an action needs, as the engine said: none, optional, or required. */
private fun FlowContent.outputField(need: OutputNeed?, type: String?, saved: Boolean, signalKept: Boolean) {
    if (need == null || need == OutputNeed.NONE) return
    label {
        +"Output, as JSON${type?.let { " ($it)" } ?: ""}"
        textArea(rows = "3") {
            name = "output"
            if (need == OutputNeed.REQUIRED) required = true
            // The shape, never a value: an example would be taken for one, and sent as is.
            placeholder = if (type?.removeSuffix("?") == "String") "\"…\"" else "{ … }"
        }
    }
    if (need == OutputNeed.OPTIONAL) {
        p(classes = "quiet") {
            +when {
                signalKept -> "Leave it empty to use the signal received."
                saved -> "Leave it empty to keep the output already saved."
                else -> "Leave it empty for none."
            }
        }
    }
}

private fun FlowContent.history(entries: List<SagaLogEntry>, now: Instant) {
    section {
        h2 { +"History" }
        table {
            thead { tr { th { +"At" }; th { +"Step" }; th { +"Event" }; th { +"Detail" }; th { +"By" } } }
            tbody {
                for (entry in entries) {
                    tr {
                        td { +at(entry.at, now) }
                        td { +(entry.stepName ?: "—") }
                        td { code { +entry.event } }
                        td { +(entry.detail ?: "") }
                        td { +(entry.actor ?: "") }
                    }
                }
            }
        }
    }
}

internal fun MAIN.problem(title: String, text: String) {
    h1 { +title }
    p { +text }
}

/** An instant to the second, in UTC, with how long ago it was. */
internal fun at(instant: Instant, now: Instant): String {
    val text = instant.toString().substringBefore('.').removeSuffix("Z").replace('T', ' ') + " UTC"
    val ago = now - instant
    return when {
        ago.absoluteValue < 1.seconds -> "$text (just now)"
        ago.isNegative() -> "$text (in ${shortly(-ago)})"
        else -> "$text (${shortly(ago)} ago)"
    }
}

private fun shortly(d: Duration): String = when {
    d < 1.seconds -> "now"
    d.inWholeMinutes < 1 -> "${d.inWholeSeconds} s"
    d.inWholeHours < 1 -> "${d.inWholeMinutes} min"
    d.inWholeDays < 1 -> "${d.inWholeHours} h"
    else -> "${d.inWholeDays} d"
}

internal fun now(): Instant = Clock.System.now()
