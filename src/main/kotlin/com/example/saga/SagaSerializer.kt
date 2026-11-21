package com.example.saga

import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import kotlinx.serialization.serializerOrNull
import kotlin.reflect.KType

/**
 * Turns saga inputs and step outputs into text and back: the one format of all saga data, given to
 * the [SagaEngine]. The engine asks [supports] for every declared type when it is built, so a type
 * it cannot handle fails at startup, not in a running saga.
 */
interface SagaSerializer {
    /** Whether values of [type] can be written and read. */
    fun supports(type: KType): Boolean
    fun encode(value: Any?, type: KType): String
    fun decode(text: String, type: KType): Any?
}

/**
 * JSON through kotlinx.serialization: the types need `@Serializable`, or a serializer registered in
 * the [json]'s `serializersModule` (for a type with its own representation).
 *
 * The default [Json] ignores unknown keys, so data written by a newer version (a field added) can
 * still be read by the previous one during a rolling deploy. Give added fields a default value, so
 * the newer version can read data written by the previous one.
 */
class KotlinxJsonSagaSerializer(private val json: Json = DefaultJson) : SagaSerializer {
    override fun supports(type: KType): Boolean = json.serializersModule.serializerOrNull(type) != null

    override fun encode(value: Any?, type: KType): String =
        json.encodeToString(json.serializersModule.serializer(type), value)

    override fun decode(text: String, type: KType): Any? =
        json.decodeFromString(json.serializersModule.serializer(type), text)

    companion object {
        val DefaultJson: Json = Json { ignoreUnknownKeys = true }
    }
}
