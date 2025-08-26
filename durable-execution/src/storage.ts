import z from 'zod'

import { getErrorMessage } from '@gpahal/std/errors'
import { omitUndefinedValues } from '@gpahal/std/objects'
import { createMutex, sleepWithJitter, type Mutex } from '@gpahal/std/promises'

import { DurableExecutionError, type DurableExecutionErrorStorageValue } from './errors'
import type { LoggerInternal } from './logger'
import type { SerializerInternal } from './serializer'
import {
  ERRORED_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  type ParentTaskExecutionSummary,
  type TaskExecution,
  type TaskExecutionStatus,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from './task'

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
 * - uniqueIndex(executionId)
 * - uniqueIndex(sleepingTaskUniqueId)
 * - index(status, startAt)
 * - index(status, onChildrenFinishedProcessingStatus, activeChildrenCount, updatedAt)
 * - index(closeStatus, updatedAt)
 * - index(isSleepingTask, expiresAt)
 * - index(onChildrenFinishedProcessingExpiresAt)
 * - index(closeExpiresAt)
 * - index(executorId, needsPromiseCancellation, updatedAt)
 * - index(parent.executionId, isFinished)
 * - index(isFinished, closeStatus, updatedAt)
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
 *   async getById(executionId: string) {
 *     const doc = await this.collection.findOne({
 *       execution_id: executionId
 *     })
 *     return taskExecutionStorageValueToDBValue(doc)
 *   }
 *
 *   // ... implement other methods
 * }
 * ```
 *
 * @category Storage
 */
export type TaskExecutionsStorage = {
  /**
   * Insert many task executions.
   *
   * @param executions - The task executions to insert.
   */
  insertMany: (executions: Array<TaskExecutionStorageValue>) => void | Promise<void>

  /**
   * Get task execution by id.
   *
   * @param executionId - The id of the task execution to get.
   * @param filters - The filters to filter the task execution.
   * @returns The task execution.
   */
  getById: (
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
  ) => TaskExecutionStorageValue | undefined | Promise<TaskExecutionStorageValue | undefined>

  /**
   * Get task execution by sleeping task unique id.
   *
   * @param sleepingTaskUniqueId - The unique id of the sleeping task to get.
   * @returns The task execution.
   */
  getBySleepingTaskUniqueId: (
    sleepingTaskUniqueId: string,
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
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
  ) => void | Promise<void>

  /**
   * Update task execution by id and insert children task executions if updated.
   *
   * @param executionId - The id of the task execution to update.
   * @param filters - The filters to filter the task execution.
   * @param update - The update object.
   * @param childrenTaskExecutionsToInsertIfAnyUpdated - The children task executions to insert if
   *   the task execution was updated.
   */
  updateByIdAndInsertChildrenIfUpdated: (
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ) => void | Promise<void>

  /**
   * Update task executions by status and start at less than and return the task executions that
   * were updated. The task executions are ordered by `startAt` ascending.
   *
   * Update `expiresAt = updateExpiresAtWithStartedAt + existingTaskExecution.timeoutMs`.
   *
   * @param status - The status of the task executions to update.
   * @param startAtLessThan - The start at less than of the task executions to update.
   * @param update - The update object.
   * @param updateExpiresAtWithStartedAt - The `startedAt` value to update the expires at with.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndStartAtLessThanAndReturn: (
    status: TaskExecutionStatus,
    startAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt: number,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by status and on children finished processing status and active
   * children task executions count zero and return the task executions that were updated. The task
   * executions are ordered by `updatedAt` ascending.
   *
   * @param status - The status of the task executions to update.
   * @param onChildrenFinishedProcessingStatus - The on children finished processing status of the
   *   task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn: (
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by close status and return the task executions that were updated. The
   * task executions are ordered by `updatedAt` ascending.
   *
   * @param closeStatus - The close status of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByCloseStatusAndReturn: (
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by is sleeping task and expires at less than and return the number of
   * task executions that were updated. The task executions are ordered by `expiresAt` ascending.
   *
   * @param isSleepingTask - The is sleeping task of the task executions to update.
   * @param expiresAtLessThan - The expires at less than of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateByIsSleepingTaskAndExpiresAtLessThan: (
    isSleepingTask: boolean,
    expiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => number | Promise<number>

  /**
   * Update task executions by on children finished processing expires at less than and return the
   * number of task executions that were updated. The task executions are ordered by
   * `onChildrenFinishedProcessingExpiresAt` ascending.
   *
   * @param onChildrenFinishedProcessingExpiresAtLessThan - The on children finished processing
   *   expires at less than of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateByOnChildrenFinishedProcessingExpiresAtLessThan: (
    onChildrenFinishedProcessingExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => number | Promise<number>

  /**
   * Update task executions by close expires at less than and return the number of task executions
   * that were updated. The task executions are ordered by `closeExpiresAt` ascending.
   *
   * @param closeExpiresAtLessThan - The close expires at less than of the task executions to
   *   update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByCloseExpiresAtLessThan: (
    closeExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => number | Promise<number>

  /**
   * Update task executions by executor id and needs promise cancellation. The task executions are
   * ordered by `updatedAt` ascending.
   *
   * @param executorId - The id of the executor.
   * @param needsPromiseCancellation - The needs promise cancellation of the task executions to
   *   update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByExecutorIdAndNeedsPromiseCancellationAndReturn: (
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Get task executions by parent execution id.
   *
   * @param parentExecutionId - The id of the parent task execution to get.
   * @returns The task executions.
   */
  getByParentExecutionId: (
    parentExecutionId: string,
  ) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by parent execution id and is finished.
   *
   * @param parentExecutionId - The id of the parent task execution to update.
   * @param isFinished - The is finished of the task executions to update.
   * @param update - The update object.
   */
  updateByParentExecutionIdAndIsFinished: (
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdate,
  ) => void | Promise<void>

  /**
   * Update task executions by is finished and close status. Also, decrement parent active children
   * count for all these task executions atomically along with the update. The task executions are
   * ordered by `updatedAt` ascending.
   *
   * @param isFinished - The is finished of the task executions to update.
   * @param closeStatus - The close status of the task executions to update.
   * @param update - The update object.
   * @param limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: (
    isFinished: boolean,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ) => number | Promise<number>

  /**
   * Delete task execution by id. This is used for testing. Ideally the storage implementation should
   * have a test and production mode and this method should be a no-op in production.
   */
  deleteById: (executionId: string) => void | Promise<void>

  /**
   * Delete all task executions. This is used for testing. Ideally the storage implementation should
   * have a test and production mode and this method should be a no-op in production.
   */
  deleteAll: () => void | Promise<void>
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

/**
 * Internal class that can be used to interact with the storage implementation.
 *
 * This class is used to interact with the storage implementation. It is used by the executor to
 * interact with the storage implementation.
 *
 * @category Storage
 * @internal
 */
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

    for (let attempt = 0; ; attempt++) {
      try {
        return await fn()
      } catch (error) {
        const durableExecutionError =
          error instanceof DurableExecutionError
            ? error
            : DurableExecutionError.retryable(getErrorMessage(error))

        if (!durableExecutionError.isRetryable) {
          throw error
        }
        if (attempt >= resolvedMaxRetryAttempts) {
          throw error
        }

        this.logger.error(`Error while retrying ${fnName}`, error)
        await sleepWithJitter(Math.min(25 * 2 ** (attempt - 1), 1000))
      }
    }
  }

  async insertMany(
    executions: Array<TaskExecutionStorageValue>,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (executions.length === 0) {
      return
    }

    return await this.retry(
      'insertMany',
      () => this.storage.insertMany(executions),
      maxRetryAttempts,
    )
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    maxRetryAttempts?: number,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.retry(
      'getById',
      () => this.storage.getById(executionId, filters),
      maxRetryAttempts,
    )
  }

  async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
    maxRetryAttempts?: number,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.retry(
      'getBySleepingTaskUniqueId',
      () => this.storage.getBySleepingTaskUniqueId(sleepingTaskUniqueId),
      maxRetryAttempts,
    )
  }

  async updateById(
    now: number,
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdateInternal,
    execution?: TaskExecutionStorageValue,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (
      update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      update.closeStatus = 'ready'
    }

    return await this.retry(
      'updateById',
      () =>
        this.storage.updateById(executionId, filters, getTaskExecutionStorageUpdate(now, update)),
      maxRetryAttempts,
    )
  }

  async updateByIdAndInsertChildrenIfUpdated(
    now: number,
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdateInternal,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
    execution?: TaskExecutionStorageValue,
    maxRetryAttempts?: number,
  ): Promise<void> {
    if (childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(now, executionId, filters, update, execution, maxRetryAttempts)
    }

    if (
      update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      update.closeStatus = 'ready'
    }

    return await this.retry(
      'updateByIdAndInsertManyIfUpdated',
      () =>
        this.storage.updateByIdAndInsertChildrenIfUpdated(
          executionId,
          filters,
          getTaskExecutionStorageUpdate(now, update),
          childrenTaskExecutionsToInsertIfAnyUpdated,
        ),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    now: number,
    status: TaskExecutionStatus,
    startAtLessThan: number,
    update: TaskExecutionStorageUpdateInternal,
    updateExpiresAtWithStartedAt: number,
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
          updateExpiresAtWithStartedAt,
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    now: number,
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn',
      () =>
        this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
          status,
          onChildrenFinishedProcessingStatus,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByCloseStatusAndReturn(
    now: number,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByCloseStatusAndReturn',
      () =>
        this.storage.updateByCloseStatusAndReturn(
          closeStatus,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByIsSleepingTaskAndExpiresAtLessThan(
    now: number,
    isSleepingTask: boolean,
    expiresAtLessThan: number,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByIsSleepingTaskAndExpiresAtLessThan',
      () =>
        this.storage.updateByIsSleepingTaskAndExpiresAtLessThan(
          isSleepingTask,
          expiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(
    now: number,
    onChildrenFinishedProcessingExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByOnChildrenFinishedProcessingExpiresAtLessThan',
      () =>
        this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(
          onChildrenFinishedProcessingExpiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByCloseExpiresAtLessThan(
    now: number,
    closeExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByCloseExpiresAtLessThan',
      () =>
        this.storage.updateByCloseExpiresAtLessThan(
          closeExpiresAtLessThan,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    now: number,
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByExecutorIdAndNeedsPromiseCancellationAndReturn',
      () =>
        this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
          executorId,
          needsPromiseCancellation,
          getTaskExecutionStorageUpdate(now, update),
          limit,
        ),
      maxRetryAttempts,
    )
  }

  async getByParentExecutionId(
    parentExecutionId: string,
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'getByParentExecutionId',
      () => this.storage.getByParentExecutionId(parentExecutionId),
      maxRetryAttempts,
    )
  }

  async updateByParentExecutionIdAndIsFinished(
    now: number,
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdateInternal,
    maxRetryAttempts?: number,
  ): Promise<void> {
    return await this.retry(
      'updateByParentExecutionIdAndIsFinished',
      () =>
        this.storage.updateByParentExecutionIdAndIsFinished(
          parentExecutionId,
          isFinished,
          getTaskExecutionStorageUpdate(now, update),
        ),
      maxRetryAttempts,
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
    now: number,
    isFinished: boolean,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdateInternal,
    limit: number,
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus',
      () =>
        this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
          isFinished,
          closeStatus,
          getTaskExecutionStorageUpdate(now, update),
          limit,
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
 * - **State**: `status`, `isFinished`, `input`, `output`, `error`
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
  root?: TaskExecutionSummary

  /**
   * The parent task execution.
   */
  parent?: ParentTaskExecutionSummary

  /**
   * The id of the task.
   */
  taskId: string
  /**
   * The id of the execution.
   */
  executionId: string
  /**
   * Whether the task execution is a sleeping task execution.
   */
  isSleepingTask: boolean
  /**
   * The unique id of the sleeping task execution. It is only present for sleeping task executions.
   */
  sleepingTaskUniqueId?: string
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
   * Whether the children task executions are sequential. It is only present for
   * `waiting_for_children` status.
   */
  areChildrenSequential: boolean
  /**
   * The input of the task execution.
   */
  input: string

  /**
   * The id of the executor.
   */
  executorId?: string
  /**
   * The status of the execution.
   */
  status: TaskExecutionStatus
  /**
   * Whether the execution is finished. Set on finish.
   */
  isFinished: boolean
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
  startAt: number
  /**
   * The time the task execution started. Set on start.
   */
  startedAt?: number
  /**
   * The time the task execution expires. It is used to recover from process failures. Set on
   * start.
   */
  expiresAt?: number
  /**
   * The time the task execution finished. Set on finish.
   */
  finishedAt?: number

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
  onChildrenFinishedProcessingExpiresAt?: number
  /**
   * The time the on children task executions finished processing finished. Set after on children
   * finished processing finishes.
   */
  onChildrenFinishedProcessingFinishedAt?: number

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
  closeExpiresAt?: number
  /**
   * The time the task execution was closed. Set on closing process finish.
   */
  closedAt?: number

  /**
   * Whether the execution needs a promise cancellation. Set on cancellation.
   */
  needsPromiseCancellation: boolean

  /**
   * The time the task execution was created.
   */
  createdAt: number
  /**
   * The time the task execution was updated.
   */
  updatedAt: number
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
 * - `ready`: The task execution is ready to be closed
 * - `closing`: Currently performing cleanup (cancelling children, updating parent, etc.)
 * - `closed`: All cleanup completed
 *
 * @category Storage
 */
export type TaskExecutionCloseStatus = 'idle' | 'ready' | 'closing' | 'closed'

export function createTaskExecutionStorageValue({
  now,
  root,
  parent,
  taskId,
  executionId,
  isSleepingTask,
  sleepingTaskUniqueId,
  retryOptions,
  sleepMsBeforeRun,
  timeoutMs,
  areChildrenSequential,
  input,
}: {
  now: number
  root?: TaskExecutionSummary
  parent?: ParentTaskExecutionSummary
  taskId: string
  executionId: string
  isSleepingTask: boolean
  sleepingTaskUniqueId?: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  areChildrenSequential?: boolean
  input: string
}): TaskExecutionStorageValue {
  const value: TaskExecutionStorageValue = {
    root,
    parent,
    taskId,
    executionId,
    isSleepingTask,
    sleepingTaskUniqueId: isSleepingTask ? (sleepingTaskUniqueId ?? '') : undefined,
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
    areChildrenSequential: areChildrenSequential ?? false,
    input,
    status: isSleepingTask ? 'running' : 'ready',
    isFinished: false,
    retryAttempts: 0,
    startAt: now + sleepMsBeforeRun,
    activeChildrenCount: 0,
    onChildrenFinishedProcessingStatus: 'idle',
    closeStatus: 'idle',
    needsPromiseCancellation: false,
    createdAt: now,
    updatedAt: now,
  }
  if (isSleepingTask) {
    value.expiresAt = now + sleepMsBeforeRun + timeoutMs
  }
  return value
}

/**
 * The filters for task execution storage get by id. Storage values are filtered by the filters
 * before being returned.
 *
 * @category Storage
 */
export type TaskExecutionStorageGetByIdFilters = {
  isSleepingTask?: boolean
  status?: TaskExecutionStatus
  isFinished?: boolean
}

/**
 * The update for a task execution. See {@link TaskExecutionStorageValue} for more details about
 * the fields.
 *
 * @category Storage
 */
export type TaskExecutionStorageUpdate = {
  executorId?: string
  status?: TaskExecutionStatus
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue
  retryAttempts?: number
  startAt?: number
  startedAt?: number
  expiresAt?: number

  children?: Array<TaskExecutionSummary>
  activeChildrenCount?: number
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: number
  onChildrenFinishedProcessingFinishedAt?: number

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: number
  closedAt?: number

  needsPromiseCancellation?: boolean

  unsetExecutorId?: boolean
  isFinished?: boolean
  unsetRunOutput?: boolean
  unsetError?: boolean
  unsetExpiresAt?: boolean
  finishedAt?: number
  unsetOnChildrenFinishedProcessingExpiresAt?: boolean
  unsetCloseExpiresAt?: boolean
  updatedAt: number
}

export type TaskExecutionStorageUpdateInternal = Omit<
  TaskExecutionStorageUpdate,
  | 'unsetExecutorId'
  | 'isFinished'
  | 'unsetRunOutput'
  | 'unsetError'
  | 'unsetExpiresAt'
  | 'finishedAt'
  | 'unsetOnChildrenFinishedProcessingExpiresAt'
  | 'unsetCloseExpiresAt'
  | 'updatedAt'
> & {
  unsetExecutorId?: never
  isFinished?: never
  unsetRunOutput?: never
  unsetError?: never
  unsetExpiresAt?: never
  finishedAt?: never
  unsetOnChildrenFinishedProcessingExpiresAt?: never
  unsetCloseExpiresAt?: never
  updatedAt?: never
}

export function getTaskExecutionStorageUpdate(
  now: number,
  internalUpdate: TaskExecutionStorageUpdateInternal,
): TaskExecutionStorageUpdate {
  const update: TaskExecutionStorageUpdate = {
    ...internalUpdate,
    unsetExecutorId: undefined,
    isFinished: undefined,
    unsetRunOutput: undefined,
    unsetError: undefined,
    unsetExpiresAt: undefined,
    finishedAt: undefined,
    unsetOnChildrenFinishedProcessingExpiresAt: undefined,
    unsetCloseExpiresAt: undefined,
    updatedAt: now,
  }
  if (internalUpdate.status) {
    if (FINISHED_TASK_EXECUTION_STATUSES.includes(internalUpdate.status)) {
      update.isFinished = true
      update.unsetRunOutput = true
      update.finishedAt = now
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
      update.unsetExecutorId = true
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
 * Applies a task execution storage update to a task execution storage value. This is used to
 * update the task execution storage value in the storage implementation before returning it to the
 * executor.
 *
 * @param execution - The task execution storage value to update.
 * @param update - The update to apply.
 * @returns The updated task execution storage value.
 *
 * @category Storage
 */
export function applyTaskExecutionStorageUpdate(
  execution: TaskExecutionStorageValue,
  update: TaskExecutionStorageUpdate,
): TaskExecutionStorageValue {
  for (const key in update) {
    switch (key) {
      case 'unsetExecutorId': {
        if (update.unsetExecutorId) {
          execution.executorId = undefined
        }

        break
      }
      case 'unsetRunOutput': {
        if (update.unsetRunOutput) {
          execution.runOutput = undefined
        }

        break
      }
      case 'unsetError': {
        if (update.unsetError) {
          execution.error = undefined
        }

        break
      }
      case 'unsetExpiresAt': {
        if (update.unsetExpiresAt) {
          execution.expiresAt = undefined
        }

        break
      }
      case 'unsetOnChildrenFinishedProcessingExpiresAt': {
        if (update.unsetOnChildrenFinishedProcessingExpiresAt) {
          execution.onChildrenFinishedProcessingExpiresAt = undefined
        }

        break
      }
      case 'unsetCloseExpiresAt': {
        if (update.unsetCloseExpiresAt) {
          execution.closeExpiresAt = undefined
        }

        break
      }
      default: {
        // @ts-expect-error - This is safe because we know the key is valid
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        execution[key] = update[key]
      }
    }
  }
  return execution
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
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        finishedAt: new Date(execution.finishedAt!),
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        finishedAt: new Date(execution.finishedAt!),
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize!,
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        finishedAt: new Date(execution.finishedAt!),
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize!,
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        finishedAt: new Date(execution.finishedAt!),
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize,
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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
        startedAt: execution.startedAt ? new Date(execution.startedAt) : undefined,
        expiresAt: execution.expiresAt ? new Date(execution.expiresAt) : undefined,
        finishedAt: new Date(execution.finishedAt!),
        children: execution.children ?? [],
        activeChildrenCount: execution.activeChildrenCount,
        finalize: execution.finalize,
        createdAt: new Date(execution.createdAt),
        updatedAt: new Date(execution.updatedAt),
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

  async insertMany(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    return await this.withMutex(() => this.storage.insertMany(executions))
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => this.storage.getById(executionId, filters))
  }

  async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => this.storage.getBySleepingTaskUniqueId(sleepingTaskUniqueId))
  }

  async updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.withMutex(() => this.storage.updateById(executionId, filters, update))
  }

  async updateByIdAndInsertChildrenIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    return await this.withMutex(() =>
      this.storage.updateByIdAndInsertChildrenIfUpdated(
        executionId,
        filters,
        update,
        childrenTaskExecutionsToInsertIfAnyUpdated,
      ),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt: number,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndStartAtLessThanAndReturn(
        status,
        startAtLessThan,
        update,
        updateExpiresAtWithStartedAt,
        limit,
      ),
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
        status,
        onChildrenFinishedProcessingStatus,
        update,
        limit,
      ),
    )
  }

  async updateByCloseStatusAndReturn(
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByCloseStatusAndReturn(closeStatus, update, limit),
    )
  }

  async updateByIsSleepingTaskAndExpiresAtLessThan(
    isSleepingTask: boolean,
    expiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateByIsSleepingTaskAndExpiresAtLessThan(
        isSleepingTask,
        expiresAtLessThan,
        update,
        limit,
      ),
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(
    onChildrenFinishedProcessingExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(
        onChildrenFinishedProcessingExpiresAtLessThan,
        update,
        limit,
      ),
    )
  }

  async updateByCloseExpiresAtLessThan(
    closeExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateByCloseExpiresAtLessThan(closeExpiresAtLessThan, update, limit),
    )
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
        executorId,
        needsPromiseCancellation,
        update,
        limit,
      ),
    )
  }

  async getByParentExecutionId(
    parentExecutionId: string,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => this.storage.getByParentExecutionId(parentExecutionId))
  }

  async updateByParentExecutionIdAndIsFinished(
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    return await this.withMutex(() =>
      this.storage.updateByParentExecutionIdAndIsFinished(parentExecutionId, isFinished, update),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
    isFinished: boolean,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
        isFinished,
        closeStatus,
        update,
        limit,
      ),
    )
  }

  async deleteById(executionId: string): Promise<void> {
    return await this.withMutex(() => this.storage.deleteById(executionId))
  }

  async deleteAll(): Promise<void> {
    return await this.withMutex(() => this.storage.deleteAll())
  }
}
