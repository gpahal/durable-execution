/* eslint-disable unicorn/throw-new-error */

import { Schema } from 'effect'

import { getErrorMessage } from '@gpahal/std/errors'

/**
 * Classification of durable execution errors for appropriate handling.
 *
 * - `generic`: General execution errors (default)
 * - `not_found`: Task or execution not found
 * - `timed_out`: Task exceeded its timeout limit
 * - `cancelled`: Task was cancelled by user or system
 *
 * @category Errors
 */
export type DurableExecutionErrorType = 'generic' | 'not_found' | 'timed_out' | 'cancelled'

/**
 * Generic error class for all durable execution failures with retry control.
 *
 * This error class provides fine-grained control over error handling behavior
 * through the `isRetryable` flag. Tasks can throw specific error types to
 * control whether failures should trigger retries.
 *
 * ## Error Behavior in Tasks
 *
 * - **Retryable errors**: Task will be retried according to retry configuration
 * - **Non-retryable errors**: Task fails immediately without retries
 * - **Internal errors**: Used for system-level failures (not user errors). Can be retryable or
 *   non-retryable.
 *
 * ## Factory Methods
 *
 * - `new DurableExecutionError()`: For generic retry behavior
 * - `DurableExecutionError.retryable()`: For transient failures (network issues, etc.)
 * - `DurableExecutionError.nonRetryable()`: For permanent failures (validation errors, etc.)
 *
 * @example
 * ```ts
 * const task = executor.task({
 *   id: 'apiCall',
 *   run: async (ctx, input) => {
 *     try {
 *       return await api.call(input.endpoint)
 *     } catch (error) {
 *       if (error.status === 429) {
 *         // Rate limited - retry with backoff
 *         if (ctx.attempt < 3) {
 *           throw DurableExecutionError.retryable('Rate limited, will retry')
 *         } else {
 *           throw DurableExecutionError.nonRetryable('Rate limited multiple times, will not retry')
 *         }
 *       } else if (error.status === 400) {
 *         // Bad request - don't retry
 *         throw DurableExecutionError.nonRetryable('Invalid request data')
 *       } else {
 *         // Unknown error - retry
 *         throw DurableExecutionError.retryable(error.message)
 *       }
 *     }
 *   }
 * })
 * ```
 *
 * @category Errors
 */
export class DurableExecutionError extends Schema.TaggedError<DurableExecutionError>(
  'DurableExecutionError',
)('DurableExecutionError', {
  message: Schema.String,
  isRetryable: Schema.Boolean,
  isInternal: Schema.Boolean,
  cause: Schema.optionalWith(Schema.Unknown, { nullable: true }),
}) {
  constructor(
    message: string,
    options?: { isRetryable?: boolean; isInternal?: boolean; cause?: unknown },
  ) {
    super({
      message,
      isRetryable: options?.isRetryable ?? false,
      isInternal: options?.isInternal ?? false,
      cause: options?.cause,
    })
  }

  static retryable(
    message: string,
    options?: { isInternal?: boolean; cause?: unknown },
  ): DurableExecutionError {
    return new DurableExecutionError(message, {
      isRetryable: true,
      isInternal: options?.isInternal ?? false,
      cause: options?.cause,
    })
  }

  static nonRetryable(
    message: string,
    options?: { isInternal?: boolean; cause?: unknown },
  ): DurableExecutionError {
    return new DurableExecutionError(message, {
      isRetryable: false,
      isInternal: options?.isInternal ?? false,
      cause: options?.cause,
    })
  }

  getErrorType(): DurableExecutionErrorType {
    return 'generic'
  }
}

/**
 * Error thrown when attempting to access a task, execution, or another resource that doesn't
 * exist.
 *
 * This error is automatically non-retryable since missing resources won't appear by retrying the
 * operation.
 *
 * Common causes:
 * - Invalid task id in executor client
 * - Invalid execution id when getting task handle
 * - Task or execution was deleted from storage
 *
 * @example
 * ```ts
 * try {
 *   const handle = await executor.getTaskExecutionHandle('nonexistent', 'te_invalid')
 * } catch (error) {
 *   if (error instanceof DurableExecutionNotFoundError) {
 *     console.log('Task execution not found')
 *   }
 * }
 * ```
 *
 * @category Errors
 */
export class DurableExecutionNotFoundError extends DurableExecutionError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, { isRetryable: false, isInternal: false, cause: options?.cause })
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'not_found'
  }
}

/**
 * Error for task timeout scenarios, automatically retryable by default.
 *
 * When thrown from within a task's run function, this marks the task execution as `timed_out`
 * rather than `failed`. Timeout errors are retryable by default since timeouts are often
 * transient.
 *
 * ## Automatic vs Manual Timeouts
 *
 * - **Automatic**: The executor automatically throws this when `timeoutMs` is exceeded
 * - **Manual**: Tasks can throw this to indicate they've detected a timeout condition
 *
 * @example
 * ```ts
 * const task = executor.task({
 *   id: 'longOperation',
 *   timeoutMs: 30_000,
 *   run: async (ctx, input) => {
 *     const controller = new AbortController()
 *
 *     // Set up our own timeout detection
 *     const timeout = setTimeout(() => {
 *       controller.abort()
 *     }, 25_000) // Timeout before the executor does
 *
 *     try {
 *       const result = await fetch(input.url, {
 *         signal: controller.signal
 *       })
 *       clearTimeout(timeout)
 *       return result
 *     } catch (error) {
 *       if (error.name === 'AbortError') {
 *         throw new DurableExecutionTimedOutError('Custom timeout reached')
 *       }
 *       throw error
 *     }
 *   }
 * })
 * ```
 *
 * @category Errors
 */
export class DurableExecutionTimedOutError extends DurableExecutionError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message ?? 'Task execution timed out', {
      isRetryable: true,
      isInternal: false,
      cause: options?.cause,
    })
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'timed_out'
  }
}

/**
 * Error for task cancellation, never retryable.
 *
 * When thrown from within a task's run function, this marks the task execution
 * as `cancelled` rather than `failed`. Cancelled tasks are never retried.
 *
 * ## Cancellation Sources
 *
 * - **Manual**: User calls `handle.cancel()` or throws this error in the task's run function
 * - **Parent failure**: Parent task failed, cancelling all children
 *
 * @example
 * ```ts
 * const task = executor.task({
 *   id: 'cancellableWork',
 *   run: async (ctx, input) => {
 *     for (let i = 0; i < 100; i++) {
 *       // Check for shutdown periodically
 *       if (ctx.shutdownSignal.isCancelled()) {
 *         // Clean up resources
 *         await cleanup()
 *         throw new DurableExecutionCancelledError('Work was cancelled')
 *       }
 *
 *       await processItem(i)
 *     }
 *
 *     return { processed: 100 }
 *   }
 * })
 *
 * // Cancel the task from outside
 * const handle = await executor.enqueueTask(task, {})
 * setTimeout(() => handle.cancel(), 5000)
 * ```
 *
 * @category Errors
 */
export class DurableExecutionCancelledError extends DurableExecutionError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string, options?: { cause?: unknown }) {
    super(message ?? 'Task execution cancelled', {
      isRetryable: false,
      isInternal: false,
      cause: options?.cause,
    })
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'cancelled'
  }
}

export function convertErrorToDurableExecutionError(
  error: unknown,
  prefix?: string,
): DurableExecutionError {
  if (error instanceof DurableExecutionError) {
    return error
  }
  return DurableExecutionError.retryable(
    prefix ? `${prefix}: ${getErrorMessage(error)}` : getErrorMessage(error),
    { cause: error },
  )
}

/**
 * Serialized representation of a DurableExecutionError for storage persistence.
 *
 * This type represents how errors are stored in the database, containing all necessary information
 * to reconstruct error state and behavior.
 *
 * Used internally by the executor and storage implementations.
 *
 * @category Errors
 */
export type DurableExecutionErrorStorageValue = {
  errorType: DurableExecutionErrorType
  message: string
  isRetryable: boolean
  isInternal: boolean
}

export function convertDurableExecutionErrorToStorageValue(
  error: DurableExecutionError,
): DurableExecutionErrorStorageValue {
  return {
    errorType: error.getErrorType(),
    message: error.message,
    isRetryable: error.isRetryable,
    isInternal: error.isInternal,
  }
}
