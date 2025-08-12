import { CustomError } from '@gpahal/std/errors'

/**
 * The type of a durable execution error.
 *
 * @category Errors
 */
export type DurableExecutionErrorType = 'generic' | 'not_found' | 'timed_out' | 'cancelled'

/**
 * Base class for all errors thrown by {@link DurableExecutor} and {@link Task}.
 *
 * @category Errors
 */
export class DurableExecutionError extends CustomError {
  /**
   * Whether the error is retryable.
   */
  readonly isRetryable: boolean

  /**
   * Whether the error is internal.
   */
  readonly isInternal: boolean

  /**
   * @param message - The error message.
   * @param isRetryable - Whether the error is retryable.
   * @param isInternal - Whether the error is internal.
   */
  constructor(message: string, isRetryable = true, isInternal = false) {
    super(message)
    this.isRetryable = isRetryable
    this.isInternal = isInternal
  }

  getErrorType(): DurableExecutionErrorType {
    return 'generic'
  }
}

/**
 * Error thrown when a task or execution is not found.
 *
 * @category Errors
 */
export class DurableExecutionNotFoundError extends DurableExecutionError {
  /**
   * @param message - The error message.
   */
  constructor(message: string) {
    super(message, false, false)
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'not_found'
  }
}

/**
 * Error thrown when a task is timed out. If this error is thrown in a task's run method, the task
 * will be marked as timed out. It might be retried based on the task's configuration and the
 * `isRetryable` flag.
 *
 * @category Errors
 */
export class DurableExecutionTimedOutError extends DurableExecutionError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string, isRetryable = true) {
    super(message ?? 'Task timed out', isRetryable, false)
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'timed_out'
  }
}

/**
 * Error thrown when a task is cancelled. If this error is thrown in a task's run method, the task
 * will be marked as cancelled. It will never be retried.
 *
 * @category Errors
 */
export class DurableExecutionCancelledError extends DurableExecutionError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string) {
    super(message ?? 'Task cancelled', false, false)
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'cancelled'
  }
}

/**
 * A storage value for a durable execution error.
 *
 * @category Errors
 */
export type DurableExecutionErrorStorageValue = {
  errorType: DurableExecutionErrorType
  isRetryable: boolean
  isInternal: boolean
  message: string
}

export function convertDurableExecutionErrorToStorageValue(
  error: DurableExecutionError,
): DurableExecutionErrorStorageValue {
  return {
    errorType: error.getErrorType(),
    isRetryable: error.isRetryable,
    isInternal: error.isInternal,
    message: error.message,
  }
}
