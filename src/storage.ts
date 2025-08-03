import {
  DURABLE_TASK_CANCELLED_ERROR_TAG,
  DURABLE_TASK_TIMED_OUT_ERROR_TAG,
  DurableTaskCancelledError,
  DurableTaskError,
  DurableTaskTimedOutError,
} from './errors'
import { type Serializer } from './serializer'
import { type DurableTaskExecution, type DurableTaskExecutionStatus } from './task'

/**
 * A durable storage with support for transactions.
 *
 * @category Storage
 */
export type DurableStorage = {
  withTransaction: <T>(fn: (tx: DurableStorageTx) => Promise<T>) => Promise<T>
}

/**
 * A durable storage transaction. It is used to perform multiple operations on the storage in a
 * single transaction.
 *
 * @category Storage
 */
export type DurableStorageTx = {
  /**
   * Insert durable task executions.
   *
   * @param executions - The durable task executions to insert.
   */
  insertTaskExecutions: (executions: Array<DurableTaskExecutionStorageObject>) => Promise<void>
  /**
   * Get durable task executions.
   *
   * @param where - The where clause to filter the durable task executions.
   * @param limit - The maximum number of durable task executions to return.
   * @returns The durable executions.
   */
  getTaskExecutions: (
    where: DurableTaskExecutionStorageWhere,
    limit?: number,
  ) => Promise<Array<DurableTaskExecutionStorageObject>>
  /**
   * Update durable task executions.
   *
   * @param where - The where clause to filter the durable task executions.
   * @param update - The update object.
   * @param limit - The maximum number of durable task executions to update.
   * @returns The ids of the durable task executions that were updated.
   */
  updateTaskExecutions: (
    where: DurableTaskExecutionStorageWhere,
    update: DurableTaskExecutionStorageObjectUpdate,
    limit?: number,
  ) => Promise<Array<string>>
}

/**
 * A storage object for a durable task execution.
 *
 * @category Storage
 */
export type DurableTaskExecutionStorageObject = {
  /**
   * The root task of the execution.
   */
  rootTask?: {
    taskId: string
    executionId: string
  }

  /**
   * The parent task of the execution.
   */
  parentTask?: {
    taskId: string
    executionId: string
    isOnRunAndChildrenCompleteChild?: boolean
  }

  /**
   * The id of the task.
   */
  taskId: string
  /**
   * The id of the execution.
   */
  executionId: string
  /**
   * The run input of the task execution.
   */
  runInput: string
  /**
   * The run output of the task execution.
   */
  runOutput?: string
  /**
   * The output of the task execution.
   */
  output?: string
  /**
   * The number of children that have been completed.
   */
  childrenCompletedCount: number
  /**
   * The children task executions of the execution.
   */
  children?: Array<DurableTaskChildExecutionStorageObject>
  /**
   * The errors of the children task executions. It is only present for children_failed status.
   */
  childrenErrors?: Array<DurableTaskChildErrorStorageObject>
  /**
   * The on run and children complete task child execution of the execution.
   */
  onRunAndChildrenComplete?: DurableTaskChildExecutionStorageObject
  /**
   * The error of the on run and children complete task execution. It is only present for
   * on_run_and_children_complete_failed status.
   */
  onRunAndChildrenCompleteError?: DurableTaskErrorStorageObject
  /**
   * The error of the execution.
   */
  error?: DurableTaskErrorStorageObject
  /**
   * The status of the execution.
   */
  status: DurableTaskExecutionStatus
  /**
   * Whether the execution is closed. Once the execution is finished, a background process
   * will update the status of its parent task if present and children if present.
   */
  isClosed: boolean
  /**
   * Whether the execution needs a promise cancellation.
   */
  needsPromiseCancellation: boolean
  /**
   * The number of attempts the execution has been retried.
   */
  retryAttempts: number
  /**
   * The start time of the task execution. Used for delaying the execution.
   */
  startAt: Date
  /**
   * The time the task execution started.
   */
  startedAt?: Date
  /**
   * The time the task execution finished.
   */
  finishedAt?: Date
  /**
   * The time the task execution expires. It is used to recover from process failures.
   */
  expiresAt: Date
  /**
   * The time the task execution was created.
   */
  createdAt: Date
  /**
   * The time the task execution was updated.
   */
  updatedAt: Date
}

/**
 * A storage object for a child task of a durable task execution.
 *
 * @category Storage
 */
export type DurableTaskChildExecutionStorageObject = {
  taskId: string
  executionId: string
}

/**
 * A storage object for a child task error of a durable task execution.
 *
 * @category Storage
 */
export type DurableTaskChildErrorStorageObject = {
  index: number
  taskId: string
  executionId: string
  error: DurableTaskErrorStorageObject
}

export function createDurableTaskExecutionStorageObject({
  now,
  rootTask,
  parentTask,
  taskId,
  executionId,
  runInput,
  sleepMsBeforeAttempt,
}: {
  now: Date
  rootTask?: {
    taskId: string
    executionId: string
  }
  parentTask?: {
    taskId: string
    executionId: string
    isOnRunAndChildrenCompleteChild?: boolean
  }
  taskId: string
  executionId: string
  runInput: string
  sleepMsBeforeAttempt: number
}): DurableTaskExecutionStorageObject {
  sleepMsBeforeAttempt = sleepMsBeforeAttempt && sleepMsBeforeAttempt > 0 ? sleepMsBeforeAttempt : 0
  return {
    rootTask,
    parentTask,
    taskId,
    executionId,
    runInput,
    childrenCompletedCount: 0,
    status: 'ready',
    isClosed: false,
    needsPromiseCancellation: false,
    retryAttempts: 0,
    startAt: new Date(now.getTime() + sleepMsBeforeAttempt),
    expiresAt: EXPIRES_AT_INFINITY,
    createdAt: now,
    updatedAt: now,
  }
}

/**
 * The where clause for durable task execution storage. Storage objects are filtered by the where
 * clause before being returned.
 *
 * Storage implementations should index based on these clauses to optimize the performance of
 * the storage operations.
 *
 * @category Storage
 */
export type DurableTaskExecutionStorageWhere =
  | {
      type: 'by_ids'
      ids: Array<string>
      statuses?: Array<DurableTaskExecutionStatus>
      needsPromiseCancellation?: boolean
    }
  | {
      type: 'by_statuses'
      statuses: Array<DurableTaskExecutionStatus>
      isClosed?: boolean
      expiresAtLessThan?: Date
    }
  | {
      type: 'by_start_at_less_than'
      startAtLessThan: Date
      statuses?: Array<DurableTaskExecutionStatus>
    }

/**
 * An expires at date that never expires.
 *
 * @category Storage
 */
export const EXPIRES_AT_INFINITY = new Date(Number.MAX_SAFE_INTEGER)

/**
 * The update object for a durable task execution. See {@link DurableTaskExecutionStorageObject}
 * for more details about the fields.
 *
 * @category Storage
 */
export type DurableTaskExecutionStorageObjectUpdate = {
  runOutput?: string
  output?: string
  childrenCompletedCount?: number
  children?: Array<DurableTaskChildExecutionStorageObject>
  childrenErrors?: Array<DurableTaskChildErrorStorageObject>
  onRunAndChildrenComplete?: DurableTaskChildExecutionStorageObject
  onRunAndChildrenCompleteError?: DurableTaskErrorStorageObject
  error?: DurableTaskErrorStorageObject
  /**
   * Whether to unset the error. If true, the error will be set to undefined.
   */
  unsetError?: boolean
  status?: DurableTaskExecutionStatus
  isClosed?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  finishedAt?: Date
  expiresAt?: Date
  updatedAt?: Date
}

/**
 * Convert a durable task execution storage object to a durable task execution.
 *
 * @category Storage
 */
export function convertTaskExecutionStorageObjectToTaskExecution<TOutput, TRunInput>(
  execution: DurableTaskExecutionStorageObject,
  serializer: Serializer,
): DurableTaskExecution<TOutput, TRunInput> {
  const runInput = serializer.deserialize<TRunInput>(execution.runInput)
  const runOutput = execution.runOutput
    ? serializer.deserialize<unknown>(execution.runOutput)
    : undefined
  const output = execution.output ? serializer.deserialize<TOutput>(execution.output) : undefined
  const children = execution.children
    ? execution.children.map((child) => ({
        taskId: child.taskId,
        executionId: child.executionId,
      }))
    : undefined
  const childrenErrors = execution.childrenErrors
    ? execution.childrenErrors.map((childError) => ({
        index: childError.index,
        taskId: childError.taskId,
        executionId: childError.executionId,
        error: convertDurableTaskErrorStorageObjectToError(childError.error),
      }))
    : undefined
  const onRunAndChildrenComplete = execution.onRunAndChildrenComplete
    ? {
        taskId: execution.onRunAndChildrenComplete.taskId,
        executionId: execution.onRunAndChildrenComplete.executionId,
      }
    : undefined
  const onRunAndChildrenCompleteError = execution.onRunAndChildrenCompleteError
    ? convertDurableTaskErrorStorageObjectToError(execution.onRunAndChildrenCompleteError)
    : undefined
  const error = execution.error
    ? convertDurableTaskErrorStorageObjectToError(execution.error)
    : undefined

  switch (execution.status) {
    case 'ready': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        error,
        status: 'ready',
        retryAttempts: execution.retryAttempts,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'running': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        error,
        status: 'running',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'failed': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        error: error!,
        status: 'failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'timed_out': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        error: error!,
        status: 'timed_out',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_children': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput: runOutput!,
        children: children ?? [],
        status: 'waiting_for_children',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'children_failed': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput: runOutput!,
        children: children ?? [],
        childrenErrors: childrenErrors ?? [],
        status: 'children_failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_on_run_and_children_complete': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput: runOutput!,
        children: children ?? [],
        onRunAndChildrenComplete: onRunAndChildrenComplete!,
        status: 'waiting_for_on_run_and_children_complete',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'on_run_and_children_complete_failed': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput: runOutput!,
        children: children ?? [],
        onRunAndChildrenComplete: onRunAndChildrenComplete!,
        onRunAndChildrenCompleteError: onRunAndChildrenCompleteError!,
        status: 'on_run_and_children_complete_failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'completed': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput: runOutput!,
        output: output!,
        children: children ?? [],
        status: 'completed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'cancelled': {
      return {
        rootTask: execution.rootTask,
        parentTask: execution.parentTask,
        taskId: execution.taskId,
        executionId: execution.executionId,
        runInput,
        runOutput,
        children,
        error: error!,
        status: 'cancelled',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    default: {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw new DurableTaskError(`Unknown execution status: ${execution.status}`, false)
    }
  }
}

/**
 * A storage object for a durable task error.
 *
 * @category Storage
 */
export type DurableTaskErrorStorageObject = {
  tag: string
  message: string
  isRetryable: boolean
}

export function convertDurableTaskErrorStorageObjectToError(
  error: DurableTaskErrorStorageObject,
): DurableTaskError {
  const tag = error.tag
  if (tag === DURABLE_TASK_TIMED_OUT_ERROR_TAG) {
    return new DurableTaskTimedOutError(error.message, error.isRetryable)
  }
  if (tag === DURABLE_TASK_CANCELLED_ERROR_TAG) {
    return new DurableTaskCancelledError(error.message)
  }
  return new DurableTaskError(error.message, error.isRetryable)
}

export function convertDurableTaskErrorToStorageObject(
  error: DurableTaskError,
): DurableTaskErrorStorageObject {
  return { tag: error.tag, message: error.message, isRetryable: error.isRetryable }
}
