import { isFunction } from '@gpahal/std/functions'

import type { CancelSignal } from './cancel'
import type { DurableExecutorError } from './errors'

/**
 * A durable function that is executed by the durable executor. It is resilient to function
 * failures, process failures, network connectivity issues, and other transient errors.
 * The function should be idempotent as it may be executed multiple times if there is a process
 * failure or if the function is retried.
 *
 * When enqueued with an executor, it supports getting the status, waiting for the function to
 * complete, and cancelling the function.
 *
 * @example
 * ```ts
 * durableExecutor.addFunction({
 *   id: 'uploadFile',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... upload a file
 *   },
 * }
 * ```
 *
 * @category Functions
 */
export type DurableFunction = {
  /**
   * A unique identifier for the function. Can only contain alphanumeric characters and
   * underscores. The identifier must be unique among all the durable functions and workflows
   * in the same durable executor.
   */
  id: string
  /**
   * The timeout for the function. If a function is provided, it will be called with the attempt
   * number and should return the timeout in milliseconds. For the first attempt, the attempt
   * number would be 0, and then for further retries, it would be 1, 2, etc.
   *
   * If a value < 0 is returned, the function will be marked as failed.
   */
  timeoutMs: number | ((attempt: number) => number)
  /**
   * The maximum number of times to retry the function.
   *
   * If the value is 0, the function will not be retried. If the value is < 0 or undefined, it will
   * be treated as 0.
   */
  maxRetryAttempts?: number
  /**
   * The delay after a failed attempt. If a function is provided, it will be called with the
   * attempt number and should return the delay in milliseconds. After the failure of the first
   * attempt, the attempt number would be 0, and then for further retries, it would be 1, 2, etc.
   *
   * If the value is < 0 or undefined, it will be treated as 0.
   */
  delayMsAfterAttempt?: number | ((attempt: number) => number)
  /**
   * The function execution logic.
   *
   * Behavior on errors:
   * - If the function throws an error or a `{@link DurableExecutorError}`, the function will be
   * marked as failed
   * - If the function throws a `{@link DurableExecutorTimedOutError}`, it will be marked as timed
   * out
   * - If the function throws a `{@link DurableExecutorCancelledError}`, it will be marked as
   * cancelled
   * - Failed and timed out functions might be retried based on the function's retry configuration
   * and `isRetryable` flag
   * - Cancelled functions will not be retried
   */
  execute: (ctx: DurableFunctionContext) => Promise<void>
}

/**
 * The context object to a durable function when it is executed.
 *
 * @category Functions
 */
export type DurableFunctionContext = {
  /**
   * The attempt number of the function. The first attempt is 0, the second attempt is 1, etc.
   */
  attempt: number
  /**
   * The error of the previous attempt.
   */
  prevError?: DurableExecutorError
  /**
   * The root workflow of the function. It is only present for durable workflow functions.
   */
  rootWorkflow?: {
    id: string
    executionId: string
  }
  /**
   * The parent workflow of the function. It is only present for durable workflow functions.
   */
  parentWorkflow?: {
    id: string
    executionId: string
  }
  /**
   * The shutdown signal of the executor. It is cancelled when the executor is shutting down.
   * The function can be used to gracefully shutdown when executor is shutting down.
   */
  shutdownSignal: CancelSignal
}

export function getDurableFunctionTimeoutMs(
  fn: DurableFunction,
  attempt: number,
): number | undefined {
  const timeoutMs = (
    fn.timeoutMs && isFunction(fn.timeoutMs) ? fn.timeoutMs(attempt) : fn.timeoutMs
  ) as number | undefined
  if (timeoutMs == null || timeoutMs <= 0) {
    return undefined
  }
  return timeoutMs
}

export function getDurableFunctionDelayMsAfterAttempt(
  fn: DurableFunction,
  attempt: number,
): number {
  const delayMs = (
    fn.delayMsAfterAttempt && isFunction(fn.delayMsAfterAttempt)
      ? fn.delayMsAfterAttempt(attempt)
      : fn.delayMsAfterAttempt
  ) as number | undefined
  if (delayMs == null || delayMs <= 0) {
    return 0
  }
  return delayMs
}

/**
 * A durable workflow that is executed by the durable executor. It contains a collection of
 * functions or workflows that are executed sequentially or in parallel. It is marked completed
 * when all the children are completed. If any child fails for any reason including timeout and
 * cancellation, the workflow is marked with the status of the first failed child.
 *
 * @example
 * ```ts
 * durableExecutor.addFunction({
 *   id: 'uploadFile',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... upload a file
 *   },
 * })
 *
 * durableExecutor.addFunction({
 *   id: 'extractFileTitle',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... extract the file title
 *   },
 * })
 *
 * durableExecutor.addFunction({
 *   id: 'summarizeFile',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... summarize the file
 *   },
 * })
 *
 * durableExecutor.addWorkflow({
 *   id: 'uploadedFileWorkflow',
 *   children: [
 *     { type: 'function', fnId: 'uploadFile' },
 *     { type: 'function', fnId: 'extractFileTitle' },
 *     { type: 'function', fnId: 'summarizeFile' },
 *   ],
 *   isParallel: true,
 * })
 *
 * durableExecutor.addWorkflow({
 *   id: 'fileWorkflow',
 *   children: [
 *     { type: 'function', fnId: 'uploadFile' },
 *     { type: 'workflow', workflowId: 'uploadedFileWorkflow' },
 *   ],
 *   isParallel: false,
 * })
 * ```
 *
 * @category Workflows
 */
export type DurableWorkflow = {
  /**
   * A unique identifier for the workflow. Can only contain alphanumeric characters and
   * underscores. The identifier must be unique among all the durable functions and workflows
   * in the same durable executor.
   */
  id: string
  /**
   * The children of the workflow.
   */
  children: Array<DurableWorkflowChild>
  /**
   * Whether the workflow is parallel. If `true`, the workflow children will be executed in
   * parallel. If `false`, the workflow children will be executed sequentially.
   */
  isParallel?: boolean
}

/**
 * A child of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowChild = DurableWorkflowFunctionChild | DurableWorkflowWorkflowChild

/**
 * A child function of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowFunctionChild = {
  type: 'function'
  fnId: string
}

/**
 * A child workflow of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowWorkflowChild = {
  type: 'workflow'
  workflowId: string
}

/**
 * A ready execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionReadyExecution = {
  rootWorkflow?: {
    id: string
    executionId: string
  }

  parentWorkflow?: {
    id: string
    executionId: string
  }

  fnId: string
  executionId: string
  status: 'ready'
  error?: DurableExecutorError
  retryAttempts: number
  expiresAt?: Date
  createdAt: Date
  updatedAt: Date
}

/**
 * An execution of a durable function. See
 * [Durable function execution](https://gpahal.github.io/durable-execution/index.html#durable-function-execution)
 * docs for more details on how function executions work.
 *
 * @category Functions
 */
export type DurableFunctionExecution =
  | DurableFunctionReadyExecution
  | DurableFunctionRunningExecution
  | DurableFunctionCompletedExecution
  | DurableFunctionFailedExecution
  | DurableFunctionTimedOutExecution
  | DurableFunctionCancelledExecution

/**
 * A finished execution of a durable function. It is either completed, failed, timed out, or
 * cancelled. See
 * [Durable function execution](https://gpahal.github.io/durable-execution/index.html#durable-function-execution)
 * docs for more details on how function executions work.
 *
 * @category Functions
 */
export type DurableFunctionFinishedExecution =
  | DurableFunctionCompletedExecution
  | DurableFunctionFailedExecution
  | DurableFunctionTimedOutExecution
  | DurableFunctionCancelledExecution

/**
 * A running execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionRunningExecution = Omit<DurableFunctionReadyExecution, 'status'> & {
  status: 'running'
  startedAt: Date
}

/**
 * A completed execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionCompletedExecution = Omit<DurableFunctionRunningExecution, 'status'> & {
  status: 'completed'
  finishedAt: Date
}

/**
 * A failed execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionFailedExecution = Omit<
  DurableFunctionRunningExecution,
  'status' | 'error'
> & {
  status: 'failed'
  error: DurableExecutorError
  finishedAt: Date
}

/**
 * A timed out execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionTimedOutExecution = Omit<DurableFunctionRunningExecution, 'status'> & {
  status: 'timed_out'
  finishedAt: Date
}

/**
 * A cancelled execution of a durable function.
 *
 * @category Functions
 */
export type DurableFunctionCancelledExecution = Omit<DurableFunctionRunningExecution, 'status'> & {
  status: 'cancelled'
  finishedAt: Date
}

/**
 * A child execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowChildExecution = {
  type: 'function' | 'workflow'
  id: string
  executionId?: string
}

/**
 * An execution of a durable workflow. See
 * [Durable workflow execution](https://gpahal.github.io/durable-execution/index.html#durable-workflow-execution)
 * docs for more details on how workflow executions work.
 *
 * @category Workflows
 */
export type DurableWorkflowExecution =
  | DurableWorkflowReadyExecution
  | DurableWorkflowRunningExecution
  | DurableWorkflowCompletedExecution
  | DurableWorkflowFailedExecution
  | DurableWorkflowTimedOutExecution
  | DurableWorkflowCancelledExecution

/**
 * A finished execution of a durable workflow. It is either completed, failed, timed out, or
 * cancelled. See
 * [Durable workflow execution](https://gpahal.github.io/durable-execution/index.html#durable-workflow-execution)
 * docs for more details on how workflow executions work.
 *
 * @category Workflows
 */
export type DurableWorkflowFinishedExecution =
  | DurableWorkflowCompletedExecution
  | DurableWorkflowFailedExecution
  | DurableWorkflowTimedOutExecution
  | DurableWorkflowCancelledExecution

/**
 * A ready execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowReadyExecution = {
  rootWorkflow?: {
    id: string
    executionId: string
  }

  parentWorkflow?: {
    id: string
    executionId: string
  }

  workflowId: string
  executionId: string
  children: Array<DurableWorkflowChildExecution>
  childrenCompletedCount: number
  isParallel: boolean
  status: 'ready'
  error?: DurableExecutorError
  retryAttempts: number
  expiresAt?: Date
  createdAt: Date
  updatedAt: Date
}

/**
 * A running execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowRunningExecution = Omit<DurableWorkflowReadyExecution, 'status'> & {
  status: 'running'
  startedAt: Date
}

/**
 * A completed execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowCompletedExecution = Omit<DurableWorkflowRunningExecution, 'status'> & {
  status: 'completed'
  finishedAt: Date
}

/**
 * A failed execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowFailedExecution = Omit<
  DurableWorkflowRunningExecution,
  'status' | 'error'
> & {
  status: 'failed'
  error: DurableExecutorError
  finishedAt: Date
}

/**
 * A timed out execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowTimedOutExecution = Omit<DurableWorkflowRunningExecution, 'status'> & {
  status: 'timed_out'
  finishedAt: Date
}

/**
 * A cancelled execution of a durable workflow.
 *
 * @category Workflows
 */
export type DurableWorkflowCancelledExecution = Omit<DurableWorkflowRunningExecution, 'status'> & {
  status: 'cancelled'
  finishedAt: Date
}
