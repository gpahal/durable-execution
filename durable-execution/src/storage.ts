import z from 'zod'

import { createMutex, type Mutex } from '@gpahal/std/promises'

import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionError,
  type DurableExecutionErrorStorageValue,
} from './errors'
import { createConsoleLogger, createLoggerWithDebugDisabled, type Logger } from './logger'
import { type Serializer } from './serializer'
import {
  type ChildTaskExecution,
  type ChildTaskExecutionErrorStorageValue,
  type TaskExecution,
  type TaskExecutionStatusStorageValue,
  type TaskRetryOptions,
} from './task'
import { sleepWithJitter } from './utils'

/**
 * Storage with support for transactions. Running multiple transactions in parallel must be
 * supported. If that is not possible, use something like `createMutex` from `@gpahal/std/mutex`
 * to run transactions sequentially.
 *
 * @category Storage
 */
export type Storage = StorageTx & {
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
  updateTaskExecutionsReturningTaskExecutions: (
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
}

export const zStorageMaxRetryAttempts = z
  .number()
  .min(1)
  .max(10)
  .nullish()
  .transform((val) => val ?? 1)

export class StorageInternal implements Storage {
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

  private getRsolvedMaxRetryAttempts(maxRetryAttempts?: number): number {
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
    const resolvedMaxRetryAttempts = this.getRsolvedMaxRetryAttempts(maxRetryAttempts)
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
        await sleepWithJitter(25)
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

  async withExistingTransaction<T>(
    tx: StorageTx | undefined | null,
    fn: (tx: StorageTxInternal) => Promise<T>,
    maxRetryAttempts?: number,
  ): Promise<T> {
    if (tx == null) {
      return await this.withTransaction(fn, maxRetryAttempts)
    }
    return await fn(new StorageTxInternal(tx))
  }

  async insertTaskExecutions(
    executions: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
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
    return await this.retry(
      'getTaskExecutions',
      () => this.storage.getTaskExecutions(where, limit),
      maxRetryAttempts,
    )
  }

  async getTaskExecutionById(executionId: string): Promise<TaskExecutionStorageValue | undefined> {
    const executions = await this.storage.getTaskExecutions({
      type: 'by_execution_ids',
      executionIds: [executionId],
    })
    if (executions.length === 0) {
      return undefined
    }
    return executions[0]!
  }

  async updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateTaskExecutionsReturningTaskExecutions',
      () => this.storage.updateTaskExecutionsReturningTaskExecutions(where, update, limit),
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
}

export class StorageTxInternal implements StorageTx {
  private readonly tx: StorageTx

  constructor(tx: StorageTx) {
    this.tx = tx
  }

  insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): void | Promise<void> {
    return this.tx.insertTaskExecutions(executions)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.tx.getTaskExecutions(where, limit)
  }

  async getTaskExecutionById(executionId: string): Promise<TaskExecutionStorageValue | undefined> {
    const executions = await this.tx.getTaskExecutions({
      type: 'by_execution_ids',
      executionIds: [executionId],
    })
    if (executions.length === 0) {
      return undefined
    }
    return executions[0]!
  }

  async updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.tx.updateTaskExecutionsReturningTaskExecutions(where, update, limit)
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    return await this.tx.updateAllTaskExecutions(where, update)
  }

  async updateTaskExecutionWithOptimisticLocking(
    executionId: string,
    updateFn: (
      currentExecution: TaskExecutionStorageValue,
    ) => Omit<TaskExecutionStorageUpdate, 'version'>,
    maxAttempts = 3,
  ): Promise<void> {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const executions = await this.tx.getTaskExecutions({
        type: 'by_execution_ids',
        executionIds: [executionId],
      })
      if (executions.length === 0) {
        return
      }

      const execution = executions[0]!
      const update = updateFn(execution)
      const updatedCount = await this.tx.updateAllTaskExecutions(
        {
          type: 'by_execution_ids',
          executionIds: [execution.executionId],
          version: execution.version,
        },
        {
          ...update,
          version: execution.version + 1,
        },
      )
      if (updatedCount > 0) {
        break
      }

      if (attempt >= maxAttempts - 1) {
        throw new DurableExecutionError(
          `Failed to update task executions after ${maxAttempts} attempts due to version conflicts`,
          false,
        )
      }
    }
  }
}

/**
 * A storage value for a task execution.
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
   * The number of children task executions that have been completed.
   */
  childrenTaskExecutionsCompletedCount: number
  /**
   * The children task executions of the execution. It is only present for
   * `waiting_for_children_tasks` status.
   */
  childrenTaskExecutions?: Array<ChildTaskExecution>
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
   * The error of the execution.
   */
  error?: DurableExecutionErrorStorageValue
  /**
   * The status of the execution.
   */
  status: TaskExecutionStatusStorageValue
  /**
   * Whether the execution is closed. Once the execution is finished, a background process will
   * update the status of its parent task execution if present and children task executions if
   * present.
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
   * The start time of the task execution. Used for delaying the execution. Set on enqueue.
   */
  startAt: Date
  /**
   * The time the task execution started. Set on start.
   */
  startedAt?: Date
  /**
   * The time the task execution finished. Set on finish.
   */
  finishedAt?: Date
  /**
   * The time the task execution expires. It is used to recover from process failures. Set on
   * start.
   */
  expiresAt?: Date
  /**
   * Version for optimistic locking. Incremented on operations that might conflict with other
   * operations.
   */
  version: number
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
    runInput,
    childrenTaskExecutionsCompletedCount: 0,
    status: 'ready',
    isClosed: false,
    needsPromiseCancellation: false,
    retryAttempts: 0,
    startAt: new Date(now.getTime() + sleepMsBeforeRun),
    version: 0,
    createdAt: now,
    updatedAt: now,
  }
}

/**
 * The where clause for task execution storage. Storage values are filtered by the where clause
 * before being returned.
 *
 * Storage implementations should index based on these clauses to optimize the performance of the
 * storage operations. Suggested indexes:
 * - uniqueIndex(execution_id)
 * - index(status, isClosed, expiresAt)
 * - index(status, startAt)
 *
 * @category Storage
 */
export type TaskExecutionStorageWhere =
  | {
      type: 'by_execution_ids'
      executionIds: Array<string>
      statuses?: Array<TaskExecutionStatusStorageValue>
      needsPromiseCancellation?: boolean
      version?: number
    }
  | {
      type: 'by_statuses'
      statuses: Array<TaskExecutionStatusStorageValue>
      isClosed?: boolean
      expiresAtLessThan?: Date
    }
  | {
      type: 'by_start_at_less_than'
      statuses: Array<TaskExecutionStatusStorageValue>
      startAtLessThan: Date
    }

/**
 * The update for a task execution. See {@link TaskExecutionStorageValue} for more details about
 * the fields.
 *
 * @category Storage
 */
export type TaskExecutionStorageUpdate = {
  runOutput?: string
  output?: string
  childrenTaskExecutionsCompletedCount?: number
  childrenTaskExecutions?: Array<ChildTaskExecution>
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue>
  finalizeTaskExecution?: ChildTaskExecution
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue
  error?: DurableExecutionErrorStorageValue
  unsetError?: boolean
  status?: TaskExecutionStatusStorageValue
  isClosed?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  finishedAt?: Date
  expiresAt?: Date
  unsetExpiresAt?: boolean
  updatedAt: Date
  version?: number
}

/**
 * Convert a task execution storage value to a task execution.
 *
 * @category Storage
 */
export function convertTaskExecutionStorageValueToTaskExecution<TOutput>(
  execution: TaskExecutionStorageValue,
  serializer: Serializer,
): TaskExecution<TOutput> {
  const runInput = serializer.deserialize(execution.runInput)
  const runOutput = execution.runOutput
    ? serializer.deserialize<unknown>(execution.runOutput)
    : undefined
  const output = execution.output ? serializer.deserialize<TOutput>(execution.output) : undefined
  const childrenTaskExecutions = execution.childrenTaskExecutions
    ? execution.childrenTaskExecutions.map((child) => ({
        taskId: child.taskId,
        executionId: child.executionId,
      }))
    : undefined
  const childrenTaskExecutionsErrors = execution.childrenTaskExecutionsErrors
    ? execution.childrenTaskExecutionsErrors.map((childError) => ({
        index: childError.index,
        taskId: childError.taskId,
        executionId: childError.executionId,
        error: childError.error,
      }))
    : undefined
  const finalizeTaskExecution = execution.finalizeTaskExecution
    ? {
        taskId: execution.finalizeTaskExecution.taskId,
        executionId: execution.finalizeTaskExecution.executionId,
      }
    : undefined
  const finalizeTaskExecutionError = execution.finalizeTaskExecutionError
  const error = execution.error

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
        rootTaskExecution: execution.rootTaskExecution,
        parentTaskExecution: execution.parentTaskExecution,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        runInput,
        error,
        status: 'running',
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
        runInput,
        error: error!,
        status: 'failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        error: error!,
        status: 'timed_out',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput: runOutput!,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        status: 'waiting_for_children_tasks',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput: runOutput!,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        childrenTaskExecutionsErrors: childrenTaskExecutionsErrors ?? [],
        status: 'children_tasks_failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput: runOutput!,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        status: 'waiting_for_finalize_task',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput: runOutput!,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        finalizeTaskExecutionError: finalizeTaskExecutionError!,
        status: 'finalize_task_failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput: runOutput!,
        output: output!,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions: childrenTaskExecutions ?? [],
        finalizeTaskExecution: finalizeTaskExecution!,
        status: 'completed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
        runInput,
        runOutput,
        childrenTaskExecutionsCompletedCount: execution.childrenTaskExecutionsCompletedCount,
        childrenTaskExecutions,
        finalizeTaskExecution,
        error: error!,
        status: 'cancelled',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt!,
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
 * A storage that stores the task executions in memory. This is useful for testing and for simple
 * use cases. Do not use this for production. It is not persistent.
 *
 * @category Storage
 */
export class InMemoryStorage implements Storage {
  private logger: Logger
  private taskExecutions: Map<string, TaskExecutionStorageValue>
  private transactionMutex: Mutex

  constructor({ enableDebug = false }: { enableDebug?: boolean } = {}) {
    this.logger = createConsoleLogger('InMemoryStorage')
    if (!enableDebug) {
      this.logger = createLoggerWithDebugDisabled(this.logger)
    }
    this.taskExecutions = new Map()
    this.transactionMutex = createMutex()
  }

  async withTransaction<T>(fn: (tx: StorageTx) => Promise<T>): Promise<T> {
    await this.transactionMutex.acquire()
    try {
      const tx = new InMemoryStorageTx(this.logger, this.taskExecutions)
      const output = await fn(tx)
      this.taskExecutions = tx.taskExecutions
      return output
    } finally {
      this.transactionMutex.release()
    }
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    await this.withTransaction(async (tx) => {
      await tx.insertTaskExecutions(executions)
    })
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.getTaskExecutions(where, limit)
    })
  }

  async updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.updateTaskExecutionsReturningTaskExecutions(where, update, limit)
    })
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    return await this.withTransaction(async (tx) => {
      return await tx.updateAllTaskExecutions(where, update)
    })
  }

  async save(saveFn: (s: string) => Promise<void>): Promise<void> {
    await saveFn(JSON.stringify(this.taskExecutions, null, 2))
  }

  async load(loadFn: () => Promise<string>): Promise<void> {
    try {
      const data = await loadFn()
      if (!data.trim()) {
        this.taskExecutions = new Map()
        return
      }

      this.taskExecutions = new Map(JSON.parse(data) as Array<[string, TaskExecutionStorageValue]>)
    } catch {
      this.taskExecutions = new Map()
    }
  }

  logAllTaskExecutions(): void {
    this.logger.info('------\n\nAll task executions:')
    for (const execution of this.taskExecutions.values()) {
      this.logger.info(
        `Task execution: ${execution.executionId}\nJSON: ${JSON.stringify(execution, null, 2)}\n\n`,
      )
    }
    this.logger.info('------')
  }
}

/**
 * The transaction for the in-memory storage.
 *
 * @category Storage
 */
export class InMemoryStorageTx implements StorageTx {
  private logger: Logger
  readonly taskExecutions: Map<string, TaskExecutionStorageValue>

  constructor(logger: Logger, executions: Map<string, TaskExecutionStorageValue>) {
    this.logger = logger
    this.taskExecutions = new Map<string, TaskExecutionStorageValue>()
    for (const [key, value] of executions) {
      this.taskExecutions.set(key, { ...value })
    }
  }

  insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): void {
    this.logger.debug(
      `Inserting ${executions.length} task executions: executions=${executions.map((e) => e.executionId).join(', ')}`,
    )
    for (const execution of executions) {
      if (this.taskExecutions.has(execution.executionId)) {
        throw new Error(`Task execution ${execution.executionId} already exists`)
      }
      this.taskExecutions.set(execution.executionId, execution)
    }
  }

  getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Array<TaskExecutionStorageValue> {
    const filteredTaskExecutions = getTaskExecutions(this.taskExecutions, where, limit)
    this.logger.debug(
      `Got ${filteredTaskExecutions.length} task executions: where=${JSON.stringify(where)} limit=${limit} executions=${filteredTaskExecutions.map((e) => e.executionId).join(', ')}`,
    )
    return filteredTaskExecutions
  }

  updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>> {
    const executions = this.getTaskExecutions(where, limit)
    for (const execution of executions) {
      updateTaskExecution(execution, update)
    }
    return executions
  }

  updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): number {
    const executions = this.getTaskExecutions(where)
    for (const execution of executions) {
      updateTaskExecution(execution, update)
    }
    return executions.length
  }
}

function getTaskExecutions(
  taskExecutions: Map<string, TaskExecutionStorageValue>,
  where: TaskExecutionStorageWhere,
  limit?: number,
) {
  let filteredTaskExecutions = [...taskExecutions.values()].filter((execution) => {
    if (
      where.type === 'by_execution_ids' &&
      where.executionIds.includes(execution.executionId) &&
      (!where.statuses || where.statuses.includes(execution.status)) &&
      (!where.needsPromiseCancellation ||
        execution.needsPromiseCancellation === where.needsPromiseCancellation) &&
      (where.version == null || execution.version === where.version)
    ) {
      return true
    }
    if (
      where.type === 'by_statuses' &&
      where.statuses.includes(execution.status) &&
      (where.isClosed == null || execution.isClosed === where.isClosed) &&
      (where.expiresAtLessThan == null ||
        (execution.expiresAt && execution.expiresAt < where.expiresAtLessThan))
    ) {
      return true
    }
    if (
      where.type === 'by_start_at_less_than' &&
      execution.startAt < where.startAtLessThan &&
      (!where.statuses || where.statuses.includes(execution.status))
    ) {
      return true
    }
    return false
  })
  if (limit != null && limit >= 0) {
    filteredTaskExecutions = filteredTaskExecutions.slice(0, limit)
  }
  return filteredTaskExecutions
}

function updateTaskExecution(
  taskExecution: TaskExecutionStorageValue,
  update: TaskExecutionStorageUpdate,
) {
  for (const key in update) {
    if (key === 'unsetError') {
      taskExecution.error = undefined
    }
    if (key === 'unsetExpiresAt') {
      taskExecution.expiresAt = undefined
    }
    if (key != null) {
      // @ts-expect-error - This is safe because we know the key is valid
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      taskExecution[key] = update[key]
    }
  }
}
