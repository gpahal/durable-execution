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
 * @param serializer - The serializer to wrap.
 * @returns The wrapped serializer.
 *
 * @category Serializer
 */
export function wrapSerializer(serializer: Serializer): Serializer {
  return {
    serialize: (value) => {
      try {
        return serializer.serialize(value)
      } catch (error) {
        throw new DurableExecutionError(`Error serializing value: ${getErrorMessage(error)}`, false)
      }
    },
    deserialize: (value) => {
      try {
        return serializer.deserialize(value)
      } catch (error) {
        throw new DurableExecutionError(
          `Error deserializing value: ${getErrorMessage(error)}`,
          false,
        )
      }
    },
  }
}
