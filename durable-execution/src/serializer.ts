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
export class WrappedSerializer implements Serializer {
  private readonly serializer: Serializer

  constructor(serializer: Serializer) {
    this.serializer = serializer
  }

  serialize<T>(value: T): string {
    try {
      return this.serializer.serialize(value)
    } catch (error) {
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
