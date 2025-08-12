import superjson from 'superjson'

import { getErrorMessage } from '@gpahal/std/errors'

import { DurableExecutionError } from './errors'

/**
 * A serializer.
 *
 * @category Serializer
 */
export type Serializer = {
  serialize: <T>(value: T) => string
  deserialize: <T>(value: string) => T
}

/**
 * Create a superjson serializer.
 *
 * @returns A serializer.
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
 * Wrap a serializer to catch errors and throw a {@link DurableExecutionError}.
 *
 * @category Serializer
 */
export class WrappedSerializer {
  private readonly serializer: Serializer

  constructor(serializer: Serializer) {
    this.serializer = serializer
  }

  serialize<T>(value: T, maxSerializedDataSize?: number): string {
    try {
      const result = this.serializer.serialize(value)
      if (maxSerializedDataSize == null) {
        return result
      }

      const sizeInBytes = Buffer.byteLength(result, 'utf8')
      if (sizeInBytes > maxSerializedDataSize) {
        throw new DurableExecutionError(
          `Serialized data size (${sizeInBytes} bytes) exceeds maximum allowed size (${maxSerializedDataSize} bytes)`,
          false,
        )
      }

      return result
    } catch (error) {
      if (error instanceof DurableExecutionError) {
        throw error
      }
      throw new DurableExecutionError(`Error serializing value: ${getErrorMessage(error)}`, false)
    }
  }

  deserialize<T>(value: string): T {
    try {
      return this.serializer.deserialize(value)
    } catch (error) {
      throw new DurableExecutionError(`Error deserializing value: ${getErrorMessage(error)}`, false)
    }
  }
}
