import type { StandardSchemaV1 } from '@standard-schema/spec'
import { getDotPath } from '@standard-schema/utils'
import { Duration, Effect } from 'effect'
import { customAlphabet } from 'nanoid'

import { convertErrorToDurableExecutionError } from './errors'

/**
 * Converts a promise-returning function to an Effect with proper error handling.
 *
 * @param promiseFn - Function that returns a Promise, optionally accepting an AbortSignal
 * @returns An Effect that wraps the promise with durable execution error conversion
 *
 * @internal
 */
export const convertPromiseToEffect = Effect.fn(
  <T>(promiseFn: (signal?: AbortSignal) => Promise<T>) =>
    Effect.tryPromise({
      try: promiseFn,
      catch: (error) => convertErrorToDurableExecutionError(error),
    }),
)

/**
 * Converts a function that may return a value or Promise to an Effect.
 *
 * This handles both synchronous and asynchronous functions uniformly by wrapping them in an async
 * context and converting errors appropriately.
 *
 * @param maybePromiseFn - Function that returns T or Promise<T>
 * @returns An Effect that wraps the function with error handling
 *
 * @internal
 */
export const convertMaybePromiseToEffect = Effect.fn(
  <T>(maybePromiseFn: (signal?: AbortSignal) => T | Promise<T>) =>
    Effect.tryPromise({
      try: async (signal: AbortSignal) => {
        return await maybePromiseFn(signal)
      },
      catch: (error) => convertErrorToDurableExecutionError(error),
    }),
)

/**
 * Converts a function that may return a value, Promise, or Effect to an Effect.
 *
 * This is the most flexible converter that handles all three return types:
 * 1. Direct values: wrapped in Effect.succeed
 * 2. Promises: converted with error handling
 * 3. Effects: passed through with error mapping
 *
 * Used extensively for task run functions that can return any of these types.
 *
 * @param maybePromiseFn - Function that returns T, Promise<T>, or Effect<T>
 * @returns An Effect that wraps the function with unified error handling
 *
 * @internal
 */
export const convertMaybePromiseOrEffectToEffect = Effect.fn(
  <T>(maybePromiseFn: (signal?: AbortSignal) => T | Promise<T> | Effect.Effect<T, unknown>) =>
    Effect.gen(function* () {
      const returnValue = yield* Effect.tryPromise({
        try: async (signal: AbortSignal) => {
          return await maybePromiseFn(signal)
        },
        catch: (error) => convertErrorToDurableExecutionError(error),
      })

      // If the function returned an Effect, we need to run it and map any errors
      if (Effect.isEffect(returnValue)) {
        return yield* returnValue.pipe(Effect.mapError(convertErrorToDurableExecutionError))
      }

      return returnValue
    }),
)

/**
 * Converts an Effect to a Promise with error handling.
 *
 * This is used to bridge between Effect-based internal code and Promise-based public apis in the
 * DurableExecutor class.
 *
 * @param effect - The Effect to convert
 * @returns A Promise that resolves/rejects with proper error conversion
 *
 * @internal
 */
export async function convertEffectToPromise<T, E>(effect: Effect.Effect<T, E>): Promise<T> {
  try {
    return await Effect.runPromise(effect)
  } catch (error) {
    throw convertErrorToDurableExecutionError(error)
  }
}

export function getDurationWithJitter(duration: Duration.DurationInput, jitterRatio: number) {
  return Duration.millis(Duration.toMillis(duration) * (1 + jitterRatio * (Math.random() * 2 - 1)))
}

const _ALPHABET = '0123456789ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz'

/**
 * Generate a URL-safe random identifier using a custom alphabet.
 *
 * Uses a carefully chosen alphabet that excludes visually similar characters:
 * - No 0/O or I/l/1 to avoid confusion
 * - URL-safe characters only (no +, /, =)
 * - Good distribution for short IDs
 *
 * Used for generating task execution IDs, which must be unique and readable.
 *
 * @param size - The length of the id. If not provided, it will be 24 characters.
 * @returns A random id using the safe alphabet
 *
 * @example
 * ```ts
 * const id = generateId()        // 24 chars: "AbC3dEf7GhJ9kLmN2pQr5tUv"
 * const shortId = generateId(8)  // 8 chars: "AbC3dEf7"
 * ```
 *
 * @internal
 */
export const generateId = customAlphabet(_ALPHABET, 24)

/**
 * Converts schema validation issues into a human-readable error summary.
 *
 * Takes validation issues from Standard Schema validation and formats them with clear error
 * markers and property paths for better debugging.
 *
 * @param issues - Array of validation issues from Standard Schema
 * @returns Formatted multi-line error summary
 *
 * @example
 * ```ts
 * const issues = [
 *   { message: 'Required property missing', path: ['user', 'email'] },
 *   { message: 'Invalid format', path: ['user', 'age'] }
 * ]
 * console.log(summarizeStandardSchemaIssues(issues))
 * // × Required property missing
 * //   → at user.email
 * // × Invalid format
 * //   → at user.age
 * ```
 *
 * @internal
 */
export function summarizeStandardSchemaIssues(
  issues: ReadonlyArray<StandardSchemaV1.Issue>,
): string {
  let summary = ''
  for (const issue of issues) {
    if (summary) {
      summary += '\n'
    }

    summary += `× ${issue.message}`
    const dotPath = getDotPath(issue)
    if (dotPath) {
      summary += `\n  → at ${dotPath}`
    }
  }

  return summary
}
