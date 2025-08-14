import z from 'zod'

import { omitUndefinedValues } from '@gpahal/std/objects'
import { createMutex, type Mutex } from '@gpahal/std/promises'

import { DurableExecutionError, type DurableExecutionErrorStorageValue } from './errors'
import type { LoggerInternal } from './logger'
import type { SerializerInternal } from './serializer'
import {
  ERRORED_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  type TaskExecution,
  type TaskExecutionStatus,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from './task'
import { sleepWithJitter } from './utils'

/**
 * The storage interface for persisting task execution state. Implementations must ensure
 * all operations are atomic to maintain consistency in distributed environments. If the
 * implementation is not atomic, use {@link TaskExecutionsStorageWithMutex} to wrap the storage
 * implementation and make all operations atomic.
 *
 * ## Implementation Requirements
 *
 * - **Atomicity**: All operations must be atomic (use transactions where available)
 * - **Concurrency**: Support multiple parallel transactions without deadlocks
 * - **Consistency**: Ensure data consistency across all operations
 * - **Durability**: Data must persist across process restarts
 *
 * ## Required Database Indexes
 *
 * For optimal performance, create these indexes in your storage backend:
 * - uniqueIndex(execution_id)
 * - index(execution_id, status)
 * - index(status, startAt)
 * - index(status, onChildrenFinishedProcessingStatus, activeChildrenCount, updatedAt)
 * - index(status, closeStatus, updatedAt)
 * - index(expiresAt)
 * - index(onChildrenFinishedProcessingExpiresAt)
 * - index(closeExpiresAt)
 * - index(needsPromiseCancellation, execution_id, updatedAt)
 *
 * ## Available Implementations
 *
 * - {@link InMemoryTaskExecutionsStorage} - For development/testing only
 * - [Drizzle ORM Storage](https://github.com/gpahal/durable-execution/tree/main/durable-execution-storage-drizzle) -
 *   Production-ready PostgreSQL/MySQL support
 *
 * @example
 * ```ts
 * // Custom storage implementation
 * class MongoDBStorage implements TaskExecutionsStorage {
 *   async insert(executions: TaskExecutionStorageValue[]) {
 *     await this.collection.insertMany(executions)
 *   }
 *
 *   async getByIds(executionIds: string[]) {
 *     const docs = await this.collection.find({
 *       execution_id: { $in: executionIds }
 *     })
 *     // Map results to maintain order
 *     return executionIds.map(id =>
 *       docs.find(d => d.execution_id === id)
 *     )
 *   }
 *   // ... implement other methods
 * }
 * ```
 *
 * @category Storage
 */
export type TaskExecutionsStorage = {
  /**
   * Insert task executions.
   *
   * @param executions - The task executions to insert.
   */
  insert: (executions: Array<TaskExecutionStorageValue>) => void | Promise<void>

  /**
   * Get task executions by ids. The task executions are in the same order as the execution ids. If
   * the task execution is not found, the result for that execution id will be `undefined`.
   *
   * @param executionIds - The ids of the task executions to get.
   * @returns The task executions.
   */
  getByIds: (
    executionIds: Array<string>,
  ) =>
    | Array<TaskExecutionStorageValue | undefined>
    | Promise<Array<TaskExecutionStorageValue | undefined>>

  /**
   * Get task execution by id.
   *
   * @param executionId - The id of the task execution to get.
   * @param filters - The filters to filter the task execution.
   * @returns The task execution.
   */
  getById: (
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
  ) => TaskExecutionStorageValue | undefined | Promise<TaskExecutionStorageValue | undefined>

  /**
   * Update task execution by id.
   *
   * @param executionId - The id of the task execution to update.
   * @param filters - The filters to filter the task execution.
   * @param update - The update object.
   */
  updateById: (
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
  ) => void | Promise<void>

  /**
   * Update task execution by id and insert task executions if updated.
   *
   * @param executionId - The id of the task execution to update.
   * @param filters - The filters to filter the task execution.
   * @param update - The update object.
   * @param executionsToInsertIfAnyUpdated - The executions to insert if the task execution was
   *   updated.
   */
  updateByIdAndInsertIfUpdated: (
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ) => void | Promise<void>

  /**
   * Update task executions by ids and statuses.
   *
   * @param executionIds - The ids of the task executions to update.
   * @param statuses - The statuses of the task executions to update.
   * @param update - The update object.
   */
  updateByIdsAndStatuses: (
    executionIds: Array<string>,
    statuses: Array<TaskExecutionStatus>,
    update: TaskExecutionStorageUpdate,
  ) => void | Promise<void>

  /**
   * Update task executions by status and start at less than and return the task executions that
   * were updated. The task executions are ordered by `startAt` ascending.
   *
   * @param status - The status of the task executions to update.
   * @param startAtLessThan - The start at less than of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndStartAtLessThanAndReturn: (
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by status and on children finished processing status and active
   * children task executions count less than and return the task executions that were updated. The
   * task executions are ordered by `updatedAt` ascending.
   *
   * @param status - The status of the task executions to update.
   * @param onChildrenFinishedProcessingStatus - The on children finished processing status of the
   *   task executions to update.
   * @param activeChildrenCountLessThan - The active children task executions count less than of the
   *   task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn: (
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by statuses and close status and return the task executions that were
   * updated. The task executions are ordered by `updatedAt` ascending.
   *
   * @param statuses - The statuses of the task executions to update.
   * @param closeStatus - The close status of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusesAndCloseStatusAndReturn: (
    statuses: Array<TaskExecutionStatus>,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by expires at less than and return the task executions that were
   * updated. The task executions are ordered by `expiresAt` ascending.
   *
   * @param expiresAtLessThan - The expires at less than of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByExpiresAtLessThanAndReturn: (
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by on children finished processing expires at less than and return the
   * task executions that were updated. The task executions are ordered by
   * `onChildrenFinishedProcessingExpiresAt` ascending.
   *
   * @param onChildrenFinishedProcessingExpiresAtLessThan - The on children finished processing
   *   expires at less than of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn: (
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by close expires at less than and return the task executions that were
   * updated. The task executions are ordered by `closeExpiresAt` ascending.
   *
   * @param closeExpiresAtLessThan - The close expires at less than of the task executions to
   *   update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByCloseExpiresAtLessThanAndReturn: (
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Get task executions by needs promise cancellation and ids. The task executions are ordered by
   * `updatedAt` ascending.
   *
   * @param needsPromiseCancellation - The needs promise cancellation of the task executions to
   *   get.
   * @param executionIds - The ids of the task executions to get.
   * @param limit - The maximum number of task executions to get.
   * @returns The task executions.
   */
  getByNeedsPromiseCancellationAndIds: (
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by needs promise cancellation and ids.
   *
   * @param needsPromiseCancellation - The needs promise cancellation of the task executions to
   *   update.
   * @param executionIds - The ids of the task executions to update.
   * @param update - The update object.
   */
  updateByNeedsPromiseCancellationAndIds: (
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    update: TaskExecutionStorageUpdate,
  ) => void | Promise<void>
}

export const zStorageMaxRetryAttempts = z
  .number()
  .min(1)
  .max(10)
  .nullish()
  .transform((val) => val ?? 1)

export function validateStorageMaxRetryAttempts(maxRetryAttempts?: number | null): number {
  const parsedMaxRetryAttempts = zStorageMaxRetryAttempts.safeParse(maxRetryAttempts)
  if (!parsedMaxRetryAttempts.success) {
    throw DurableExecutionError.nonRetryable(
      `Invalid storage max retry attempts: ${z.prettifyError(parsedMaxRetryAttempts.error)}`,
    )
  }
  return parsedMaxRetryAttempts.data
}

export class TaskExecutionsStorageInternal {
  private readonly logger: LoggerInternal
  private readonly storage: TaskExecutionsStorage
  private readonly maxRetryAttempts: number

  constructor(logger: LoggerInternal, storage: TaskExecutionsStorage, maxRetryAttempts?: number) {
    this.logger = logger
    this.storage = storage
    this.maxRetryAttempts = validateStorageMaxRetryAttempts(maxRetryAttempts)
  }

  private getResolvedMaxRetryAttempts(maxRetryAttempts?: number): number {
    if (maxRetryAttempts != null) {
      return validateStorageMaxRetryAttempts(maxRetryAttempts)
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

  async insert(
    executions: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (executions.length === 0) {
      return
    }

    return await this.retry('insert', () => this.storage.insert(executions), maxRetryAttempts)
  }

  async getByIds(
    executionIds: Array<string>,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    if (executionIds.length === 0) {
      return []
    }

    return await this.retry('getByIds', () => this.storage.getByIds(executionIds), maxRetryAttempts)
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    maxRetryAttempts?: number,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.retry(
      'getById',
      () => this.storage.getById(executionId, filters),
      maxRetryAttempts,
    )
  }

  async updateById(
    now: Date,
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdateInternal,
    maxRetryAttempts?: number,
  ): Promise<void> {
    return await this.retry(
      'updateById',
      () =>
        this.storage.updateById(executionId, filters, getTaskExecutionStorageUpdate(now, update)),
      maxRetryAttempts,
    )
  }

  async updateByIdAndInsertIfUpdated(
    now: Date,
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdateInternal,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (executionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(now, executionId, filters, update, maxRetryAttempts)
    }

    return await this.retry(
      'updateByIdAndInsertIfUpdated',
      () =>
        this.storage.updateByIdAndInsertIfUpdated(
          executionId,
          filters,
          getTaskExecutionStorageUpdate(now, update),
          executionsToInsertIfAnyUpdated,
        ),
      maxRetryAttempts,
    )
  }

  async updateByIdsAndStatuses(
    now: Date,
    executionIds: Array<string>,
    statuses: Array<TaskExecutionStatus>,
    update: TaskExecutionStorageUpdateInternal,
    maxRetryAttempts?: number,
  ): Promise<void> {
    return await this.retry(
      'updateByIdsAndStatuses',
      () =>
        this.storage.updateByIdsAndStatuses(
          executionIds,
          statuses,
          getTaskExecutionStorageUpdate(now, update),
        ),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    now: Date,
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusAndStartAtLessThanAndReturn',
      () =>
        this.storage.updateByStatusAndStartAtLessThanAndReturn(
          status,
          startAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    now: Date,
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn',
      () =>
        this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
          status,
          onChildrenFinishedProcessingStatus,
          activeChildrenCountLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByStatusesAndCloseStatusAndReturn(
    now: Date,
    statuses: Array<TaskExecutionStatus>,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusesAndCloseStatusAndReturn',
      () =>
        this.storage.updateByStatusesAndCloseStatusAndReturn(
          statuses,
          closeStatus,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByExpiresAtLessThanAndReturn(
    now: Date,
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByExpiresAtLessThanAndReturn',
      () =>
        this.storage.updateByExpiresAtLessThanAndReturn(
          expiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
    now: Date,
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn',
      () =>
        this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
          onChildrenFinishedProcessingExpiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByCloseExpiresAtLessThanAndReturn(
    now: Date,
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByCloseExpiresAtLessThanAndReturn',
      () =>
        this.storage.updateByCloseExpiresAtLessThanAndReturn(
          closeExpiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async getByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'getByNeedsPromiseCancellationAndIds',
      () =>
        this.storage.getByNeedsPromiseCancellationAndIds(
          needsPromiseCancellation,
          executionIds,
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByNeedsPromiseCancellationAndIds(
    now: Date,
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    update: TaskExecutionStorageUpdateInternal,
    maxRetryAttempts?: number,
  ): Promise<void> {
    return await this.retry(
      'updateByNeedsPromiseCancellationAndIds',
      () =>
        this.storage.updateByNeedsPromiseCancellationAndIds(
          needsPromiseCancellation,
          executionIds,
          getTaskExecutionStorageUpdate(now, update),
        ),
      maxRetryAttempts,
    )
  }
}

/**
 * Complete storage representation of a task execution including all metadata, relationships and
 * state tracking fields.
 *
 * This type represents the full database record for a task execution. All fields are used
 * internally by the executor for state management, recovery and coordination.
 *
 * ## Field Categories
 *
 * - **Identity**: `taskId`, `executionId`, `root`, `parent`
 * - **Configuration**: `retryOptions`, `timeoutMs`, `sleepMsBeforeRun`
 * - **State**: `status`, `input`, `output`, `error`
 * - **Timing**: `startAt`, `startedAt`, `expiresAt`, `finishedAt`, etc.
 * - **Relationships**: `children`, `finalize`, `activeChildrenCount`
 * - **Recovery**: `closeStatus`, `onChildrenFinishedProcessingStatus`, `needsPromiseCancellation`
 *
 * @category Storage
 */
export type TaskExecutionStorageValue = {
  /**
   * The root task execution.
   */
  root?: {
    taskId: string
    executionId: string
  }

  /**
   * The parent task execution.
   */
  parent?: {
    taskId: string
    executionId: string
    indexInParentChildTaskExecutions: number
    isFinalizeTaskOfParentTask: boolean
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
   * The input of the task execution.
   */
  input: string

  /**
   * The status of the execution.
   */
  status: TaskExecutionStatus
  /**
   * The run output of the task execution. Deleted after the task execution is finished.
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
   * The children task executions of the execution. It is only present for `waiting_for_children`
   * status.
   */
  children?: Array<TaskExecutionSummary>
  /**
   * The number of active children task executions. It is only present for `waiting_for_children`
   * status.
   */
  activeChildrenCount: number
  /**
   * The status of the on children finished processing.
   */
  onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
  /**
   * The time the on children finished processing expires. It is used to recover from process
   * failures. Set on on children finished processing start.
   */
  onChildrenFinishedProcessingExpiresAt?: Date
  /**
   * The time the on children task executions finished processing finished. Set after on children
   * finished processing finishes.
   */
  onChildrenFinishedProcessingFinishedAt?: Date

  /**
   * The finalize task execution of the execution.
   */
  finalize?: TaskExecutionSummary

  /**
   * Whether the execution is closed. Once the execution is finished, the closing process will
   * update this field in the background.
   */
  closeStatus: TaskExecutionCloseStatus
  /**
   * The time the task execution close expires. It is used to recover from process failures. Set on
   * closing process start.
   */
  closeExpiresAt?: Date
  /**
   * The time the task execution was closed. Set on closing process finish.
   */
  closedAt?: Date

  /**
   * Whether the execution needs a promise cancellation. Set on cancellation.
   */
  needsPromiseCancellation: boolean

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
 * The processing status for handling child task completion.
 *
 * - `idle`: Not processing child completions
 * - `processing`: Currently processing child task completions
 * - `processed`: Child task completions have been processed
 *
 * @category Storage
 */
export type TaskExecutionOnChildrenFinishedProcessingStatus = 'idle' | 'processing' | 'processed'

/**
 * The status of the task execution cleanup/closure process.
 *
 * - `idle`: No closure process started
 * - `closing`: Currently performing cleanup (cancelling children, updating parent, etc.)
 * - `closed`: All cleanup completed
 *
 * @category Storage
 */
export type TaskExecutionCloseStatus = 'idle' | 'closing' | 'closed'

export function createTaskExecutionStorageValue({
  now,
  root,
  parent,
  taskId,
  executionId,
  retryOptions,
  sleepMsBeforeRun,
  timeoutMs,
  input,
}: {
  now: Date
  root?: {
    taskId: string
    executionId: string
  }
  parent?: {
    taskId: string
    executionId: string
    indexInParentChildTaskExecutions: number
    isFinalizeTaskOfParentTask: boolean
  }
  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  input: string
}): TaskExecutionStorageValue {
  return {
    root,
    parent,
    taskId,
    executionId,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    input,
    status: 'ready',
    retryAttempts: 0,
    startAt: new Date(now.getTime() + sleepMsBeforeRun),
    activeChildrenCount: 0,
    onChildrenFinishedProcessingStatus: 'idle',
    closeStatus: 'idle',
    needsPromiseCancellation: false,
    createdAt: now,
    updatedAt: now,
  }
}

/**
 * The filters for task execution storage get by ids. Storage values are filtered by the filters
 * before being returned.
 *
 * @category Storage
 */
export type TaskExecutionStorageGetByIdsFilters = {
  statuses?: Array<TaskExecutionStatus>
}

/**
 * The update for a task execution. See {@link TaskExecutionStorageValue} for more details about
 * the fields.
 *
 * @category Storage
 */
export type TaskExecutionStorageUpdate = {
  status?: TaskExecutionStatus
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  expiresAt?: Date

  children?: Array<TaskExecutionSummary>
  activeChildrenCount?: number
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: Date
  onChildrenFinishedProcessingFinishedAt?: Date

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: Date
  closedAt?: Date

  needsPromiseCancellation?: boolean

  unsetRunOutput?: boolean
  unsetError?: boolean
  unsetExpiresAt?: boolean
  finishedAt?: Date
  decrementParentActiveChildrenCount?: boolean
  unsetOnChildrenFinishedProcessingExpiresAt?: boolean
  unsetCloseExpiresAt?: boolean
  updatedAt: Date
}

export type TaskExecutionStorageUpdateInternal = Omit<
  TaskExecutionStorageUpdate,
  | 'unsetRunOutput'
  | 'unsetError'
  | 'unsetExpiresAt'
  | 'finishedAt'
  | 'decrementParentActiveChildrenCount'
  | 'unsetOnChildrenFinishedProcessingExpiresAt'
  | 'unsetCloseExpiresAt'
  | 'updatedAt'
> & {
  unsetRunOutput?: never
  unsetError?: never
  unsetExpiresAt?: never
  finishedAt?: never
  decrementParentActiveChildrenCount?: never
  unsetOnChildrenFinishedProcessingExpiresAt?: never
  unsetCloseExpiresAt?: never
  updatedAt?: never
}

export function getTaskExecutionStorageUpdate(
  now: Date,
  internalUpdate: TaskExecutionStorageUpdateInternal,
): TaskExecutionStorageUpdate {
  const update: TaskExecutionStorageUpdate = {
    ...internalUpdate,
    unsetRunOutput: undefined,
    unsetError: undefined,
    unsetExpiresAt: undefined,
    finishedAt: undefined,
    decrementParentActiveChildrenCount: undefined,
    unsetOnChildrenFinishedProcessingExpiresAt: undefined,
    unsetCloseExpiresAt: undefined,
    updatedAt: now,
  }
  if (internalUpdate.status) {
    if (FINISHED_TASK_EXECUTION_STATUSES.includes(internalUpdate.status)) {
      update.unsetRunOutput = true
      update.finishedAt = now
      update.decrementParentActiveChildrenCount = true
    }
    if (internalUpdate.status === 'ready') {
      update.unsetRunOutput = true
    }
    if (
      !ERRORED_TASK_EXECUTION_STATUSES.includes(internalUpdate.status) &&
      internalUpdate.status !== 'ready' &&
      internalUpdate.status !== 'running'
    ) {
      update.unsetError = true
    }
    if (internalUpdate.status !== 'running') {
      update.unsetExpiresAt = true
    }
  }
  if (
    internalUpdate.onChildrenFinishedProcessingStatus &&
    internalUpdate.onChildrenFinishedProcessingStatus !== 'processing'
  ) {
    update.unsetOnChildrenFinishedProcessingExpiresAt = true
  }
  if (internalUpdate.closeStatus && internalUpdate.closeStatus !== 'closing') {
    update.unsetCloseExpiresAt = true
  }
  return omitUndefinedValues(update)
}

/**
 * Convert a task execution storage value to a task execution.
 *
 * @category Storage
 */
export function convertTaskExecutionStorageValueToTaskExecution<TOutput>(
  execution: TaskExecutionStorageValue,
  serializer: SerializerInternal,
): TaskExecution<TOutput> {
  const input = serializer.deserialize(execution.input)
  const output = execution.output ? serializer.deserialize<TOutput>(execution.output) : undefined

  switch (execution.status) {
    case 'ready': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'ready',
        error: execution.error,
        retryAttempts: execution.retryAttempts,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'running': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'running',
        error: execution.error,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'failed': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'failed',
        error: execution.error!,
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
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'timed_out',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_children': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'waiting_for_children',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'waiting_for_finalize': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'waiting_for_finalize',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'finalize_failed': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'finalize_failed',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize!,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'completed': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'completed',
        output: output!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'cancelled': {
      return {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'cancelled',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt!,
        finishedAt: execution.finishedAt!,
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    default: {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw DurableExecutionError.nonRetryable(`Invalid task execution status: ${execution.status}`)
    }
  }
}

/**
 * Wraps a {@link TaskExecutionsStorage} and makes all operations atomic by protecting them with a
 * mutex. Only use this if the underlying storage is not atomic.
 *
 * @category Storage
 */
export class TaskExecutionsStorageWithMutex implements TaskExecutionsStorage {
  private readonly storage: TaskExecutionsStorage
  private readonly mutex: Mutex

  constructor(storage: TaskExecutionsStorage) {
    this.storage = storage
    this.mutex = createMutex()
  }

  async withMutex<T>(fn: () => T | Promise<T>): Promise<T> {
    await this.mutex.acquire()
    try {
      return await fn()
    } finally {
      this.mutex.release()
    }
  }

  async insert(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    await this.withMutex(() => this.storage.insert(executions))
  }

  async getByIds(
    executionIds: Array<string>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.withMutex(() => this.storage.getByIds(executionIds))
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => this.storage.getById(executionId, filters))
  }

  async updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.withMutex(() => this.storage.updateById(executionId, filters, update))
  }

  async updateByIdAndInsertIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    await this.withMutex(() =>
      this.storage.updateByIdAndInsertIfUpdated(
        executionId,
        filters,
        update,
        executionsToInsertIfAnyUpdated,
      ),
    )
  }

  async updateByIdsAndStatuses(
    executionIds: Array<string>,
    statuses: Array<TaskExecutionStatus>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.withMutex(() => this.storage.updateByIdsAndStatuses(executionIds, statuses, update))
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndStartAtLessThanAndReturn(
        status,
        startAtLessThan,
        update,
        limit,
      ),
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
        status,
        onChildrenFinishedProcessingStatus,
        activeChildrenCountLessThan,
        update,
        limit,
      ),
    )
  }

  async updateByStatusesAndCloseStatusAndReturn(
    statuses: Array<TaskExecutionStatus>,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusesAndCloseStatusAndReturn(statuses, closeStatus, update, limit),
    )
  }

  async updateByExpiresAtLessThanAndReturn(
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByExpiresAtLessThanAndReturn(expiresAtLessThan, update, limit),
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
        onChildrenFinishedProcessingExpiresAtLessThan,
        update,
        limit,
      ),
    )
  }

  async updateByCloseExpiresAtLessThanAndReturn(
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByCloseExpiresAtLessThanAndReturn(closeExpiresAtLessThan, update, limit),
    )
  }

  async getByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.getByNeedsPromiseCancellationAndIds(
        needsPromiseCancellation,
        executionIds,
        limit,
      ),
    )
  }

  async updateByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.withMutex(() =>
      this.storage.updateByNeedsPromiseCancellationAndIds(
        needsPromiseCancellation,
        executionIds,
        update,
      ),
    )
  }
}
