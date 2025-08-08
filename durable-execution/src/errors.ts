import { CustomError } from 'ts-custom-error'

/**
 * The type of a durable execution error.
 *
 * @category Errors
 */
export type DurableExecutionErrorType = 'generic' | 'timed_out' | 'cancelled'

/**
 * Base class for all errors thrown by {@link DurableExecutor} and {@link DurableTask}.
 *
 * @category Errors
 */
export class DurableExecutionError extends CustomError {
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
    this.isRetryable = isRetryable
  }

  getErrorType(): DurableExecutionErrorType {
    return 'generic'
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
    super(message ?? 'Task timed out', isRetryable)
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
    super(message ?? 'Task cancelled', false)
  }

  override getErrorType(): DurableExecutionErrorType {
    return 'cancelled'
  }
}

/**
 * A storage object for a durable execution error.
 *
 * @category Errors
 */
export type DurableExecutionErrorStorageObject = {
  errorType: DurableExecutionErrorType
  message: string
  isRetryable: boolean
}

export function convertDurableExecutionErrorToStorageObject(
  error: DurableExecutionError,
): DurableExecutionErrorStorageObject {
  return {
    errorType: error.getErrorType(),
    message: error.message,
    isRetryable: error.isRetryable,
  }
}
