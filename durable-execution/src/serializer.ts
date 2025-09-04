import superjson from 'superjson'

import { getErrorMessage } from '@gpahal/std/errors'

import { DurableExecutionError } from './errors'

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
 * const executor = new DurableExecutor(storage, {
 *   serializer: customSerializer
 * })
 * ```
 *
 * @category Serializer
 */
export type Serializer = {
  serialize: <T>(value: T) => string
  deserialize: <T>(value: string) => T
}

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
 * @see https://github.com/blitz-js/superjson for more information
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
 * Internal serializer class that can be used to serialize and deserialize values.
 *
 * This class is used to serialize and deserialize values in the database. It is used by the
 * executor to store task inputs and outputs in the database.
 *
 * @category Serializer
 * @internal
 */
export class SerializerInternal {
  private readonly serializer: Serializer

  constructor(serializer?: Serializer | null) {
    this.serializer = serializer ?? createSuperjsonSerializer()
  }

  serialize<T>(value: T, maxSerializedDataSize?: number): string {
    try {
      const result = this.serializer.serialize(value)
      if (maxSerializedDataSize == null || result.length * 4 <= maxSerializedDataSize) {
        return result
      }

      const sizeInBytes = Buffer.byteLength(result, 'utf8')
      if (sizeInBytes > maxSerializedDataSize) {
        throw DurableExecutionError.nonRetryable(
          `Serialized data size (${sizeInBytes} bytes) exceeds maximum allowed size (${maxSerializedDataSize} bytes)`,
        )
      }

      return result
    } catch (error) {
      if (error instanceof DurableExecutionError) {
        throw error
      }
      throw DurableExecutionError.nonRetryable(`Error serializing value: ${getErrorMessage(error)}`)
    }
  }

  deserialize<T>(value: string): T {
    try {
      return this.serializer.deserialize(value)
    } catch (error) {
      throw DurableExecutionError.nonRetryable(
        `Error deserializing value: ${getErrorMessage(error)}`,
      )
    }
  }
}
