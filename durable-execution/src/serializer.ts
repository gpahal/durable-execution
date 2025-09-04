import { Context, Effect, Option } from 'effect'
import superjson from 'superjson'

import { DurableExecutionError } from './errors'
import { convertMaybePromiseOrEffectToEffect } from './utils'

/**
 * Interface for serializing and deserializing task inputs and outputs.
 *
 * The serializer is responsible for converting JavaScript values to/from strings for storage
 * persistence. The executor uses this to store task inputs and outputs in the database.
 *
 * ## Requirements
 *
 * - Must handle all input and output types used by the application
 * - Should preserve type information (Dates, etc.)
 * - Must be deterministic (same input → same output)
 * - Should handle circular references gracefully if they can be part of the input or output
 *
 * @example
 * ```ts
 * // Custom superjson serializer
 * const customSerializer: Serializer = {
 *   serialize: (value) => superjson.stringify(value),
 *   deserialize: (str) => superjson.parse(str)
 * }
 *
 * const executor = await DurableExecutor.make(storage, {
 *   serializer: customSerializer
 * })
 * ```
 *
 * @category Serializer
 */
export type Serializer = {
  serialize: <T>(value: T) => string | Promise<string> | Effect.Effect<string, unknown>
  deserialize: <T>(value: string) => T | Promise<T> | Effect.Effect<T, unknown>
}

/**
 * Service for the serializer.
 *
 * @category Serializer
 */
export class SerializerService extends Context.Tag('SerializerService')<
  SerializerService,
  Serializer
>() {}

/**
 * Create a serializer using Superjson for enhanced type preservation.
 *
 * Superjson extends JSON serialization to handle additional JavaScript types:
 * - Dates, RegExp, undefined, BigInt
 * - Sets, Maps, Arrays with holes
 * - Circular references
 * - Class instances (with transformers)
 *
 * This is the default serializer used by the executor when none is specified.
 *
 * @example
 * ```ts
 * const serializer = createSuperjsonSerializer()
 *
 * // Can serialize complex types
 * const data = {
 *   date: new Date(),
 *   map: new Map([['key', 'value']]),
 *   set: new Set([1, 2, 3]),
 *   regex: /pattern/gi
 * }
 *
 * const serialized = serializer.serialize(data)
 * const deserialized = serializer.deserialize(serialized)
 * // All types are preserved correctly
 * ```
 *
 * @returns A Serializer instance using Superjson
 *
 * @see [Superjson](https://github.com/blitz-js/superjson) for more information.
 *
 * @category Serializer
 */
export function createSuperjsonSerializer(): Serializer {
  return {
    serialize: superjson.stringify,
    deserialize: superjson.parse,
  }
}

/**
 * Internal serializer factory that creates Effect-based serialization functions.
 *
 * This function:
 * 1. Gets the serializer service (or defaults to superjson)
 * 2. Wraps serialize/deserialize with Effect error handling
 * 3. Adds size validation for serialized data
 *
 * The size check uses a quick approximation (length * 4) for performance, then falls back to
 * accurate byte counting if needed.
 *
 * @category Serializer
 * @internal
 */
export const makeSerializerInternal = Effect.gen(function* () {
  const serializerOption = yield* Effect.serviceOption(SerializerService)
  const serializer = Option.isNone(serializerOption)
    ? createSuperjsonSerializer()
    : serializerOption.value

  const serialize = Effect.fn(function* <T>(value: T, maxSerializedDataSize?: number) {
    const result = yield* convertMaybePromiseOrEffectToEffect(() => serializer.serialize(value))

    // Quick size check: assume worst case of 4 bytes per character (UTF-8). This avoids expensive
    // Buffer.byteLength() call in most cases
    if (maxSerializedDataSize == null || result.length * 4 <= maxSerializedDataSize) {
      return result
    }

    // Accurate byte counting for edge cases where quick check fails
    const sizeInBytes = Buffer.byteLength(result, 'utf8')
    if (sizeInBytes > maxSerializedDataSize) {
      return yield* Effect.fail(
        DurableExecutionError.nonRetryable(
          `Serialized data size exceeds maximum allowed size [serializedDataSize=${result.length}B] [maxSerializedDataSize=${maxSerializedDataSize}B]`,
        ),
      )
    }

    return result
  })

  const deserialize = Effect.fn(function* <T>(value: string) {
    const result = yield* convertMaybePromiseOrEffectToEffect(() => serializer.deserialize(value))
    return result as T
  })

  return { serialize, deserialize }
})

export type SerializerInternal = Effect.Effect.Success<typeof makeSerializerInternal>
