import { CustomError } from 'ts-custom-error'

export const DURABLE_TASK_ERROR_TAG = 'DurableTaskError'
export const DURABLE_TASK_TIMED_OUT_ERROR_TAG = 'DurableTaskTimedOutError'
export const DURABLE_TASK_CANCELLED_ERROR_TAG = 'DurableTaskCancelledError'

/**
 * Base class for all errors thrown by {@link DurableTask} and {@link DurableExecutor}.
 *
 * @category Errors
 */
export class DurableTaskError extends CustomError {
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
    this.tag = DURABLE_TASK_ERROR_TAG
    this.isRetryable = isRetryable
  }
}

/**
 * Error thrown when a task is timed out. If this error is thrown in a task's run method, the task
 * will be marked as timed out. It might be retried based on the task's configuration and the
 * `isRetryable` flag.
 *
 * @category Errors
 */
export class DurableTaskTimedOutError extends DurableTaskError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string, isRetryable = true) {
    super(message ?? 'Task timed out', isRetryable)
    this.tag = DURABLE_TASK_TIMED_OUT_ERROR_TAG
  }
}

/**
 * Error thrown when a task is cancelled. If this error is thrown in a task's run method, the task
 * will be marked as cancelled. It will never be retried.
 *
 * @category Errors
 */
export class DurableTaskCancelledError extends DurableTaskError {
  /**
   * @param message - The error message.
   */
  constructor(message?: string) {
    super(message ?? 'Task cancelled', false)
    this.tag = DURABLE_TASK_CANCELLED_ERROR_TAG
  }
}
