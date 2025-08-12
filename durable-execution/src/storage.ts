import z from 'zod'

import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionError,
  type DurableExecutionErrorStorageValue,
} from './errors'
import { type Logger } from './logger'
import type { WrappedSerializer } from './serializer'
import {
  type ChildTaskExecution,
  type ChildTaskExecutionError,
  type ChildTaskExecutionErrorStorageValue,
  type CompletedChildTaskExecution,
  type TaskExecution,
  type TaskExecutionStatusStorageValue,
  type TaskRetryOptions,
} from './task'
import { sleepWithJitter } from './utils'

/**
 * Storage with support for transactions. Running multiple transactions in parallel must be
 * supported. If that is not possible, use something like `createMutex` from
 * `@gpahal/std/promises` to run transactions sequentially.
 *
 * All the operations should be atomic. All the operations should be ordered by `updatedAt`
 * ascending.
 *
 * @category Storage
 */
export type Storage = StorageTx & {
  /**
   * Run a transaction.
   *
   * @param fn - The function to run in the transaction.
   * @returns The result of the transaction.
   */
  withTransaction: <T>(fn: (tx: StorageTx) => Promise<T>) => Promise<T>
}

/**
 * A storage transaction. It is used to perform multiple operations on the storage in a single
 * transaction.
 *
 * @category Storage
 */
export type StorageTx = {
  /**
   * Insert task executions.
   *
   * @param executions - The task executions to insert.
   */
  insertTaskExecutions: (executions: Array<TaskExecutionStorageValue>) => void | Promise<void>
  /**
   * Get task executions.
   *
   * @param where - The where clause to filter the task executions.
   * @param limit - The maximum number of task executions to return.
   * @returns The task executions.
   */
  getTaskExecutions: (
    where: TaskExecutionStorageWhere,
    limit?: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>
  /**
   * Update task executions and return the task executions that were updated.
   *
   * @param where - The where clause to filter the task executions.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateTaskExecutionsAndReturn: (
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>
  /**
   * Update all task executions.
   *
   * @param where - The where clause to filter the task executions.
   * @param update - The update object.
   * @returns The count of the task executions that were updated.
   */
  updateAllTaskExecutions: (
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ) => number | Promise<number>
  /**
   * Insert finished child task execution if it does not exist.
   *
   * @param finishedChildTaskExecution - The finished child task execution to insert.
   */
  insertFinishedChildTaskExecutionIfNotExists: (
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ) => void | Promise<void>
  /**
   * Delete finished child task executions and return the deleted finished child task executions.
   *
   * @param limit - The maximum number of finished child task executions to delete.
   */
  deleteFinishedChildTaskExecutionsAndReturn: (
    limit: number,
  ) =>
    | Array<FinishedChildTaskExecutionStorageValue>
    | Promise<Array<FinishedChildTaskExecutionStorageValue>>
}

export const zStorageMaxRetryAttempts = z
  .number()
  .min(1)
  .max(10)
  .nullish()
  .transform((val) => val ?? 1)

export class StorageInternal {
  private readonly logger: Logger
  private readonly storage: Storage
  private readonly maxRetryAttempts: number

  constructor(logger: Logger, storage: Storage, maxRetryAttempts?: number) {
    const parsedMaxRetryAttempts = zStorageMaxRetryAttempts.safeParse(maxRetryAttempts)
    if (!parsedMaxRetryAttempts.success) {
      throw new DurableExecutionError(
        `Invalid storage max retry attempts: ${z.prettifyError(parsedMaxRetryAttempts.error)}`,
        false,
      )
    }

    this.logger = logger
    this.storage = storage
    this.maxRetryAttempts = parsedMaxRetryAttempts.data
  }

  private getResolvedMaxRetryAttempts(maxRetryAttempts?: number): number {
    if (maxRetryAttempts != null) {
      const parsedMaxRetryAttempts = zStorageMaxRetryAttempts.safeParse(maxRetryAttempts)
      if (!parsedMaxRetryAttempts.success) {
        throw new DurableExecutionError(
          `Invalid storage max retry attempts: ${z.prettifyError(parsedMaxRetryAttempts.error)}`,
          false,
        )
      }
      return parsedMaxRetryAttempts.data
    }
    return this.maxRetryAttempts
  }

  private async retry<T>(
    fnName: string,
    fn: () => T | Promise<T>,
    maxRetryAttempts?: number,
  ): Promise<T> {
    const resolvedMaxRetryAttempts = this.getResolvedMaxRetryAttempts(maxRetryAttempts)
    if (resolvedMaxRetryAttempts <= 0) {
      return await fn()
    }

    for (let i = 0; ; i++) {
      try {
        return await fn()
      } catch (error) {
        if (error instanceof DurableExecutionError && !error.isRetryable) {
          throw error
        }
        if (i >= resolvedMaxRetryAttempts) {
          throw error
        }

        this.logger.error(`Error while retrying ${fnName}`, error)
        await sleepWithJitter(Math.min(25 * 2 ** i, 1000))
      }
    }
  }

  async withTransaction<T>(
    fn: (tx: StorageTxInternal) => Promise<T>,
    maxRetryAttempts?: number,
  ): Promise<T> {
    return await this.retry(
      'withTransaction',
      () => this.storage.withTransaction((tx) => fn(new StorageTxInternal(tx))),
      maxRetryAttempts,
    )
  }

  async insertTaskExecutions(
    executions: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (executions.length === 0) {
      return
    }

    return await this.retry(
      'insertTaskExecutions',
      () => this.storage.insertTaskExecutions(executions),
      maxRetryAttempts,
    )
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    if (limit != null && limit <= 0) {
      return []
    }

    return await this.retry(
      'getTaskExecutions',
      () => this.storage.getTaskExecutions(where, limit),
      maxRetryAttempts,
    )
  }

  async getTaskExecutionById(executionId: string): Promise<TaskExecutionStorageValue | undefined> {
    const executions = await this.getTaskExecutions({
      type: 'by_execution_ids',
      executionIds: [executionId],
    })
    if (executions.length === 0) {
      return undefined
    }
    return executions[0]!
  }

  async updateTaskExecutionsAndReturn(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateTaskExecutionsAndReturn',
      () => this.storage.updateTaskExecutionsAndReturn(where, update, limit),
      maxRetryAttempts,
    )
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateAllTaskExecutions',
      () => this.storage.updateAllTaskExecutions(where, update),
      maxRetryAttempts,
    )
  }

  async updateAllTaskExecutionsAndInsertTaskExecutionsIfAnyUpdated(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    executions: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
    await this.withTransaction(async (tx) => {
      await tx.updateAllTaskExecutionsAndInsertTaskExecutionsIfAnyUpdated(where, update, executions)
    }, maxRetryAttempts)
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
    maxRetryAttempts?: number,
  ): Promise<void> {
    return await this.retry(
      'insertFinishedChildTaskExecutionIfNotExists',
      () => this.storage.insertFinishedChildTaskExecutionIfNotExists(finishedChildTaskExecution),
      maxRetryAttempts,
    )
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    return await this.retry(
      'deleteFinishedChildTaskExecutionsAndReturn',
      () => this.storage.deleteFinishedChildTaskExecutionsAndReturn(limit),
      maxRetryAttempts,
    )
  }
}

export class StorageTxInternal {
  private readonly tx: StorageTx

  constructor(tx: StorageTx) {
    this.tx = tx
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    return await this.tx.insertTaskExecutions(executions)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    if (limit != null && limit <= 0) {
      return []
    }

    return await this.tx.getTaskExecutions(where, limit)
  }

  async getTaskExecutionById(executionId: string): Promise<TaskExecutionStorageValue | undefined> {
    const executions = await this.getTaskExecutions({
      type: 'by_execution_ids',
      executionIds: [executionId],
    })
    if (executions.length === 0) {
      return undefined
    }
    return executions[0]!
  }

  async updateTaskExecutionsAndReturn(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.tx.updateTaskExecutionsAndReturn(where, update, limit)
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    return await this.tx.updateAllTaskExecutions(where, update)
  }

  async updateAllTaskExecutionsAndInsertTaskExecutionsIfAnyUpdated(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    executions: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    const updatedCount = await this.tx.updateAllTaskExecutions(where, update)
    if (updatedCount === 0 || executions.length === 0) {
      return
    }
    await this.tx.insertTaskExecutions(executions)
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    return await this.tx.insertFinishedChildTaskExecutionIfNotExists(finishedChildTaskExecution)
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    return await this.tx.deleteFinishedChildTaskExecutionsAndReturn(limit)
  }
}

/**
 * A storage value for a task execution.
 *
 * Storage implementations should index based on these clauses to optimize the performance of the
 * storage operations. Suggested indexes:
 * - uniqueIndex(execution_id)
 * - index(execution_id, updatedAt)
 * - index(status, updatedAt)
 * - index(status, startAt, updatedAt)
 * - index(status, expiresAt, updatedAt)
 * - index(isClosed, status, updatedAt)
 *
 * @category Storage
 */
export type TaskExecutionStorageValue = {
  /**
   * The root task execution.
   */
  rootTaskExecution?: {
    taskId: string
    executionId: string
  }

  /**
   * The parent task execution.
   */
  parentTaskExecution?: {
    taskId: string
    executionId: string
    parentChildTaskExecutionIndex: number
    isFinalizeTask?: boolean
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
   * The retry options of the task execution.
   */
  retryOptions: TaskRetryOptions
  /**
   * The sleep ms before run of the task execution.
   */
  sleepMsBeforeRun: number
  /**
   * The timeout ms of the task execution.
   */
  timeoutMs: number

  /**
   * The status of the execution.
   */
  status: TaskExecutionStatusStorageValue
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
   * The error of the execution.
   */
  error?: DurableExecutionErrorStorageValue
  /**
   * Whether the execution needs a promise cancellation.
   */
  needsPromiseCancellation: boolean
  /**
   * The number of attempts the execution has been retried.
   */
  retryAttempts: number
  /**
   * The start time of the task execution. Used for delaying the execution. Set on enqueue.
   */
  startAt: Date
  /**
   * The time the task execution started. Set on start.
   */
  startedAt?: Date
  /**
   * The time the task execution expires. It is used to recover from process failures. Set on
   * start.
   */
  expiresAt?: Date
  /**
   * The time the task execution finished. Set on finish.
   */
  finishedAt?: Date

  /**
   * The children task executions of the execution. It is only present for
   * `waiting_for_children_tasks` status.
   */
  childrenTaskExecutions?: Array<ChildTaskExecution>
  /**
   * The completed children task executions of the execution. It is only present for
   * `waiting_for_children_tasks` status.
   */
  completedChildrenTaskExecutions?: Array<ChildTaskExecution>
  /**
   * The errors of the children task executions. It is only present for
   * `children_tasks_failed` status. In case of multiple errors, the order of errors is not defined.
   */
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue>

  /**
   * The finalize task execution of the execution.
   */
  finalizeTaskExecution?: ChildTaskExecution
  /**
   * The error of the finalize task execution. It is only present for `finalize_task_failed` status.
   */
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue

  /**
   * Whether the execution is closed. Once the execution is finished, the closing process will
   * update this field in the background.
   */
  isClosed: boolean
  /**
   * The time the task execution was closed. Set on closing process finish.
   */
  closedAt?: Date

  /**
   * The time the task execution was created.
   */
  createdAt: Date
  /**
   * The time the task execution was updated.
   */
  updatedAt: Date
}

export function createTaskExecutionStorageValue({
  now,
  rootTaskExecution,
  parentTaskExecution,
  taskId,
  executionId,
  retryOptions,
  sleepMsBeforeRun,
  timeoutMs,
  runInput,
}: {
  now: Date
  rootTaskExecution?: {
    taskId: string
    executionId: string
  }
  parentTaskExecution?: {
    taskId: string
    executionId: string
    parentChildTaskExecutionIndex: number
    isFinalizeTask?: boolean
  }
  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  runInput: string
}): TaskExecutionStorageValue {
  return {
    rootTaskExecution,
    parentTaskExecution,
    taskId,
    executionId,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    status: 'ready',
    runInput,
    needsPromiseCancellation: false,
    retryAttempts: 0,
    startAt: new Date(now.getTime() + sleepMsBeforeRun),
    isClosed: false,
    createdAt: now,
    updatedAt: now,
  }
}

/**
 * The where clause for task execution storage. Storage values are filtered by the where clause
 * before being returned.
 *
 * @category Storage
 */
export type TaskExecutionStorageWhere =
  | {
      type: 'by_execution_ids'
      executionIds: Array<string>
      statuses?: Array<TaskExecutionStatusStorageValue>
      needsPromiseCancellation?: boolean
    }
  | {
      type: 'by_statuses'
      statuses: Array<TaskExecutionStatusStorageValue>
    }
  | {
      type: 'by_status_and_start_at_less_than'
      status: TaskExecutionStatusStorageValue
      startAtLessThan: Date
    }
  | {
      type: 'by_status_and_expires_at_less_than'
      status: TaskExecutionStatusStorageValue
      expiresAtLessThan: Date
    }
  | {
      type: 'by_is_closed'
      isClosed: boolean
      statuses?: Array<TaskExecutionStatusStorageValue>
    }

/**
 * The update for a task execution. See {@link TaskExecutionStorageValue} for more details about
 * the fields.
 *
 * @category Storage
 */
export type TaskExecutionStorageUpdate = {
  status?: TaskExecutionStatusStorageValue
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue
  unsetError?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  expiresAt?: Date
  unsetExpiresAt?: boolean
  finishedAt?: Date

  childrenTaskExecutions?: Array<ChildTaskExecution>
  completedChildrenTaskExecutions?: Array<ChildTaskExecution>
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue>
  processChildrenTaskExecutionsAt?: Date

  finalizeTaskExecution?: ChildTaskExecution
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue

  isClosed?: boolean
  closedAt?: Date

  updatedAt: Date
}

/**
 * Convert a task execution storage value to a task execution.
 *
 * @category Storage
 */
export function convertTaskExecutionStorageValueToTaskExecution<TOutput>(
  execution: TaskExecutionStorageValue,
  serializer: WrappedSerializer,
): TaskExecution<TOutput> {
  const runInput = serializer.deserialize(execution.runInput)
  const runOutput = execution.runOutput
    ? serializer.deserialize<unknown>(execution.runOutput)
    : undefined
  const output = execution.output ? serializer.deserialize<TOutput>(execution.output) : undefined
  const error = execution.error
  const childrenTaskExecutions = execution.childrenTaskExecutions
    ? execution.childrenTaskExecutions.map((child) => ({
        taskId: child.taskId,
        executionId: child.executionId,
      }))
    : undefined
  const completedChildrenTaskExecutions = execution.completedChildrenTaskExecutions
    ? (execution.completedChildrenTaskExecutions
        .map((completedChildTaskExecution) => {
          const index = execution.childrenTaskExecutions?.findIndex(
            (childTaskExecution) =>
              childTaskExecution.executionId === completedChildTaskExecution.executionId,
          )
          if (index == null || index < 0) {
            return undefined
          }

          return {
            index,
            taskId: completedChildTaskExecution.taskId,
            executionId: completedChildTaskExecution.executionId,
          }
        })
        .filter(Boolean) as Array<CompletedChildTaskExecution>)
    : undefined
  const completedChildrenTaskExecutionsCount =
    execution.completedChildrenTaskExecutions?.length ?? 0
  const childrenTaskExecutionsErrors = execution.childrenTaskExecutionsErrors
    ? (execution.childrenTaskExecutionsErrors
        .map((childTaskExecutionError) => {
          const index = execution.childrenTaskExecutions?.findIndex(
            (childTaskExecution) =>
              childTaskExecution.executionId === childTaskExecutionError.executionId,
          )
          if (index == null || index < 0) {
            return undefined
          }

          return {
            index,
            taskId: childTaskExecutionError.taskId,
            executionId: childTaskExecutionError.executionId,
            status: childTaskExecutionError.status,
            error: childTaskExecutionError.error,
          }
        })
        .filter(Boolean) as Array<ChildTaskExecutionError>)
    : undefined
  const finalizeTaskExecution = execution.finalizeTaskExecution
    ? {
        taskId: execution.finalizeTaskExecution.taskId,
        executionId: execution.finalizeTaskExecution.executionId,
      }
    : undefined
  const finalizeTaskExecutionError = execution.finalizeTaskExecutionError

  switch (execution.status) {
    case 'ready': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'ready',
        runInput,
        error,
        retryAttempts: execution.retryAttempts,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'running': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'running',
        runInput,
        error,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'failed': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'failed',
        runInput,
        error: error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'timed_out': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'timed_out',
        runInput,
        error: error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_children_tasks': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'waiting_for_children_tasks',
        runInput,
        runOutput: runOutput!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'children_tasks_failed': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'children_tasks_failed',
        runInput,
        runOutput: runOutput!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        childrenTaskExecutionsErrors: childrenTaskExecutionsErrors ?? [],
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_finalize_task': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'waiting_for_finalize_task',
        runInput,
        runOutput: runOutput!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'finalize_task_failed': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'finalize_task_failed',
        runInput,
        runOutput: runOutput!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        finalizeTaskExecutionError: finalizeTaskExecutionError!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'completed': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'completed',
        runInput,
        runOutput: runOutput!,
        output: output!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'cancelled': {
      return {
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        status: 'cancelled',
        runInput,
        runOutput,
        error: error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        childrenTaskExecutions,
        completedChildrenTaskExecutionsCount,
        completedChildrenTaskExecutions: completedChildrenTaskExecutions ?? [],
        finalizeTaskExecution,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    default: {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw new DurableExecutionError(`Unknown task execution status: ${execution.status}`, false)
    }
  }
}

export function getTaskExecutionStorageValueParentExecutionError(
  execution: TaskExecutionStorageValue,
): DurableExecutionErrorStorageValue {
  if (execution.error) {
    return execution.error
  }
  if (execution.finalizeTaskExecutionError) {
    return convertDurableExecutionErrorToStorageValue(
      new DurableExecutionError(
        `Finalize task with id ${execution.finalizeTaskExecution?.taskId} failed: ${execution.finalizeTaskExecutionError.message}`,
        false,
      ),
    )
  }
  if (execution.childrenTaskExecutionsErrors) {
    return convertDurableExecutionErrorToStorageValue(
      new DurableExecutionError(
        `Children task errors:\n${execution.childrenTaskExecutionsErrors.map((e) => `  Child task with id ${e.taskId} failed: ${e.error.message}`).join('\n')}`,
        false,
      ),
    )
  }
  return convertDurableExecutionErrorToStorageValue(
    new DurableExecutionError('Unknown durable execution error', false),
  )
}

/**
 * The storage value of the finished child task execution. The `parentExecutionId` is the id of the
 * parent execution and it should be unique.
 *
 * Storage implementations should index based on these clauses to optimize the performance of the
 * storage operations. Suggested indexes:
 * - uniqueIndex(parentExecutionId)
 * - index(updatedAt)
 *
 * @category Storage
 */
export type FinishedChildTaskExecutionStorageValue = {
  /**
   * The id of the parent execution. It should be unique.
   */
  parentExecutionId: string
  /**
   * The time the finished child task execution was created.
   */
  createdAt: Date
  /**
   * The time the finished child task execution was updated.
   */
  updatedAt: Date
}
