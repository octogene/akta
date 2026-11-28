package dev.octogene.akta

import kotlin.reflect.KClass
import kotlin.reflect.KType

/*
 * Small helpers shared by the engine's parts (SagaEngine, SagaManualActions, SagaStore): the erased types the engine
 * handles every saga through, and how it reads stored data.
 */

internal typealias AnyDefinition = SagaDefinition<Any?>
internal typealias AnyStep = SagaStep<Any?, Any?>
internal typealias AnySignalStep = SignalStep<Any?, Any?>

/** The engine handles every step through its erased type: each one only ever sees its own values. */
@Suppress("UNCHECKED_CAST")
internal fun AnyDefinition.stepAt(index: Int): AnyStep = steps[index] as AnyStep

@Suppress("UNCHECKED_CAST")
internal fun AnyDefinition.signalStepAt(index: Int): AnySignalStep = steps[index] as AnySignalStep

/** [text], stored as [type], decoded; [what] names it in the [SagaDataException] thrown when it cannot be read. */
internal fun SagaSerializer.decodeSaved(text: String, type: KType, what: () -> String): Any? =
    try {
        decode(text, type)
    } catch (e: Exception) {
        throw SagaDataException("Cannot read ${what()} as $type: $e", e)
    }

/** What saga_log may record about [output]: the step's describe(), never the value. Null if describe() throws. */
internal fun describe(step: AnyStep, output: Any?): String? =
    try {
        step.describe(output)
    } catch (_: Exception) {
        null
    }

internal fun String.utf8Size(): Int = encodeToByteArray().size

/**
 * The type as an operator reads it: `String`, `Payment?`, `List<Payment>`. Built from its classes' simple names:
 * [KType.toString] needs kotlin-reflect for that, and says so instead.
 */
internal fun KType.displayName(): String {
    val name = (classifier as? KClass<*>)?.simpleName ?: classifier.toString()
    val arguments = if (arguments.isEmpty()) "" else arguments.joinToString(", ", "<", ">") { it.type?.displayName() ?: "*" }
    return name + arguments + if (isMarkedNullable) "?" else ""
}

/** Errors the JVM cannot recover from (out of memory, stack overflow): never caught. */
internal fun Throwable.isFatal(): Boolean = this is VirtualMachineError

/**
 * A saga's input or a step's output cannot be read: it was written by another version of the code, or
 * the serializer's configuration changed. Thrown before any call is made, so nothing is left half done.
 */
public class SagaDataException(message: String, cause: Throwable? = null) : IllegalStateException(message, cause)
