import { CustomError } from 'ts-custom-error'

/**
 * Base class for all errors thrown by the Durable Executor.
 *
 * @category Errors
 */
export class DurableExecutorError extends CustomError {
  /**
   * The tag for the error.
   */
  tag: string
  /**
   * Whether the error is retryable.
   */
  readonly isRetryable: boolean

  /**
   * @param message - The error message.
   * @param isRetryable - Whether the error is retryable.
   */
  constructor(message: string, isRetryable = true) {
    super(message)
    this.tag = 'DurableExecutorError'
    this.isRetryable = isRetryable
  }
}

/**
 * Error thrown when an operation is timed out. If this error is thrown in a function's execute
 * method, the function will be marked as timed out. It might be retried based on the function's
 * retry configuration and `isRetryable` flag.
 *
 * @category Errors
 */
export class DurableExecutorTimedOutError extends DurableExecutorError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string, isRetryable = true) {
    super(message ?? 'Operation timed out', isRetryable)
    this.tag = 'DurableExecutorTimedOutError'
  }
}

/**
 * Error thrown when an operation is cancelled. If this error is thrown in a function's execute
 * method, the function will be marked as cancelled. It will never be retried.
 *
 * @category Errors
 */
export class DurableExecutorCancelledError extends DurableExecutorError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string) {
    super(message ?? 'Operation cancelled', false)
    this.tag = 'DurableExecutorCancelledError'
  }
}

export function createDurableExecutorError(
  message: string,
  tag = 'DurableExecutorError',
  isRetryable = true,
) {
  if (tag === 'DurableExecutorTimedOutError') {
    return new DurableExecutorTimedOutError(message, isRetryable)
  }
  if (tag === 'DurableExecutorCancelledError') {
    return new DurableExecutorCancelledError(message)
  }
  return new DurableExecutorError(message, isRetryable)
}
