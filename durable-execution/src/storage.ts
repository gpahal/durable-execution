import { createMutex, type Mutex } from '@gpahal/std/promises'

import { DurableExecutionError, type DurableExecutionErrorStorageValue } from './errors'
import type { SerializerInternal } from './serializer'
import {
  type ParentTaskExecutionSummary,
  type TaskExecution,
  type TaskExecutionStatus,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from './task'

/**
 * The storage interface for persisting task execution state. Implementations must ensure
 * all operations are atomic to maintain consistency in distributed environments.
 *
 * If the implementation is not atomic, use {@link TaskExecutionsStorageWithMutex} to wrap the
 * storage implementation and make all operations atomic.
 *
 * If the implementation doesn't support batching methods, use
 * {@link TaskExecutionsStorageWithBatching} to wrap the storage implementation and implement the
 * batching methods.
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
 * - index(status, isSleepingTask, expiresAt)
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
 * - [Convex Storage](https://github.com/gpahal/durable-execution/tree/main/durable-execution-storage-convex) -
 *   Production-ready Convex support
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
  insertMany: (executions: ReadonlyArray<TaskExecutionStorageValue>) => void | Promise<void>

  /**
   * Get many task executions by id and optionally filter them.
   *
   * @param requests - The requests to get the task executions.
   * @returns The task executions.
   */
  getManyById: (
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
    }>,
  ) =>
    | Array<TaskExecutionStorageValue | undefined>
    | Promise<Array<TaskExecutionStorageValue | undefined>>

  /**
   * Get many task executions by sleeping task unique id.
   *
   * @param requests - The requests to get the task executions.
   * @returns The task executions.
   */
  getManyBySleepingTaskUniqueId: (
    requests: ReadonlyArray<{
      sleepingTaskUniqueId: string
    }>,
  ) =>
    | Array<TaskExecutionStorageValue | undefined>
    | Promise<Array<TaskExecutionStorageValue | undefined>>

  /**
   * Update many task executions by id and optionally filter them. Each request should be atomic.
   *
   * @param requests - The requests to update the task executions.
   */
  updateManyById: (
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ) => void | Promise<void>

  /**
   * Update many task executions by id and insert children task executions if updated. Each request
   * should be atomic.
   *
   * @param requests - The requests to update the task executions.
   */
  updateManyByIdAndInsertChildrenIfUpdated: (
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ) => void | Promise<void>

  /**
   * Update task executions by status and start at less than and return the task executions that
   * were updated. The task executions are ordered by `startAt` ascending.
   *
   * Update `expiresAt = updateExpiresAtWithStartedAt + existingTaskExecution.timeoutMs`.
   *
   * @param request - The request to update the task executions.
   * @param request.status - The status of the task executions to update.
   * @param request.startAtLessThan - The start at less than of the task executions to update.
   * @param request.update - The update object.
   * @param request.updateExpiresAtWithStartedAt - The `startedAt` value to update the expires at
   *   with.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndStartAtLessThanAndReturn: (request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by status and on children finished processing status and active
   * children task executions count zero and return the task executions that were updated. The task
   * executions are ordered by `updatedAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.status - The status of the task executions to update.
   * @param request.onChildrenFinishedProcessingStatus - The on children finished processing status
   *   of the task executions to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn: (request: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by close status and return the task executions that were updated. The
   * task executions are ordered by `updatedAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.closeStatus - The close status of the task executions to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByCloseStatusAndReturn: (request: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by is sleeping task and expires at less than and return the number of
   * task executions that were updated. The task executions are ordered by `expiresAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.status - The status of the task executions to update.
   * @param request.isSleepingTask - The is sleeping task of the task executions to update.
   * @param request.expiresAtLessThan - The expires at less than of the task executions to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: (request: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }) => number | Promise<number>

  /**
   * Update task executions by on children finished processing expires at less than and return the
   * number of task executions that were updated. The task executions are ordered by
   * `onChildrenFinishedProcessingExpiresAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.onChildrenFinishedProcessingExpiresAtLessThan - The on children finished
   *   processing expires at less than of the task executions to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateByOnChildrenFinishedProcessingExpiresAtLessThan: (request: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }) => number | Promise<number>

  /**
   * Update task executions by close expires at less than and return the number of task executions
   * that were updated. The task executions are ordered by `closeExpiresAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.closeExpiresAtLessThan - The close expires at less than of the task executions
   *   to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByCloseExpiresAtLessThan: (request: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }) => number | Promise<number>

  /**
   * Update task executions by executor id and needs promise cancellation. The task executions are
   * ordered by `updatedAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.executorId - The id of the executor.
   * @param request.needsPromiseCancellation - The needs promise cancellation of the task executions
   *   to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The task executions that were updated.
   */
  updateByExecutorIdAndNeedsPromiseCancellationAndReturn: (request: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Get many task executions by parent execution id.
   *
   * @param requests - The requests to get the task executions.
   * @returns The task executions.
   */
  getManyByParentExecutionId: (
    requests: ReadonlyArray<{
      parentExecutionId: string
    }>,
  ) => Array<Array<TaskExecutionStorageValue>> | Promise<Array<Array<TaskExecutionStorageValue>>>

  /**
   * Update many task executions by parent execution id and is finished. Each request should be
   * atomic.
   *
   * @param requests - The requests to update the task executions.
   */
  updateManyByParentExecutionIdAndIsFinished: (
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ) => void | Promise<void>

  /**
   * Update task executions by is finished and close status. Also, decrement parent active children
   * count for all these task executions atomically along with the update. The task executions are
   * ordered by `updatedAt` ascending.
   *
   * @param request - The request to update the task executions.
   * @param request.isFinished - The is finished of the task executions to update.
   * @param request.closeStatus - The close status of the task executions to update.
   * @param request.update - The update object.
   * @param request.limit - The maximum number of task executions to update.
   * @returns The number of task executions that were updated.
   */
  updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus: (request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }) => number | Promise<number>

  /**
   * Delete task execution by id. This is used for testing. Ideally the storage implementation should
   * have a test and production mode and this method should be a no-op in production.
   *
   * @param request - The request to delete the task execution.
   * @param request.executionId - The id of the task execution to delete.
   */
  deleteById: (request: { executionId: string }) => void | Promise<void>

  /**
   * Delete all task executions. This is used for testing. Ideally the storage implementation should
   * have a test and production mode and this method should be a no-op in production.
   */
  deleteAll: () => void | Promise<void>
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
   * The time the task execution waiting for children starts.
   */
  waitingForChildrenStartedAt?: number
  /**
   * The time the task execution waiting for finalize starts.
   */
  waitingForFinalizeStartedAt?: number
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
  sleepingTaskUniqueId?: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  areChildrenSequential?: boolean
  input: string
}): TaskExecutionStorageValue {
  const isSleepingTask = sleepingTaskUniqueId != null
  const value: TaskExecutionStorageValue = {
    root,
    parent,
    taskId,
    executionId,
    isSleepingTask,
    sleepingTaskUniqueId,
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
  isFinished?: boolean
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue
  retryAttempts?: number
  startAt?: number
  startedAt?: number
  expiresAt?: number
  waitingForChildrenStartedAt?: number
  waitingForFinalizeStartedAt?: number
  finishedAt?: number

  children?: ReadonlyArray<TaskExecutionSummary>
  activeChildrenCount?: number
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: number
  onChildrenFinishedProcessingFinishedAt?: number

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: number
  closedAt?: number

  needsPromiseCancellation?: boolean

  updatedAt: number

  unset?: {
    executorId?: boolean
    runOutput?: boolean
    error?: boolean
    startedAt?: boolean
    expiresAt?: boolean
    onChildrenFinishedProcessingExpiresAt?: boolean
    closeExpiresAt?: boolean
  }
}

/**
 * Applies a task execution storage update to a task execution storage value. This is used to
 * update the task execution storage value in the storage implementation before returning it to the
 * executor.
 *
 * @param execution - The task execution storage value to update.
 * @param update - The update to apply.
 * @param updateExpiresAtWithStartedAt - The expiresAt field will be set to the sum of the
 *   startedAt field and the timeoutMs field.
 * @returns The updated task execution storage value.
 *
 * @category Storage
 */
export function applyTaskExecutionStorageUpdate(
  execution: TaskExecutionStorageValue,
  update: TaskExecutionStorageUpdate,
  updateExpiresAtWithStartedAt?: number,
): TaskExecutionStorageValue {
  for (const key in update) {
    if (key === 'unset') {
      for (const unsetKey in update.unset) {
        // @ts-expect-error - This is safe because we know the key is valid
        if (update.unset[unsetKey]) {
          // @ts-expect-error - This is safe because we know the key is valid
          execution[unsetKey] = undefined
        }
      }
    } else {
      // @ts-expect-error - This is safe because we know the key is valid
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      execution[key] = update[key]
    }
  }

  if (updateExpiresAtWithStartedAt) {
    execution.expiresAt = updateExpiresAtWithStartedAt + execution.timeoutMs
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'ready',
        error: execution.error,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        executorId: execution.executorId!,
        status: 'running',
        error: execution.error,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'failed',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'timed_out',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'waiting_for_children',
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        waitingForChildrenStartedAt: new Date(execution.waitingForChildrenStartedAt!),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'waiting_for_finalize',
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        waitingForChildrenStartedAt: new Date(execution.waitingForChildrenStartedAt!),
        waitingForFinalizeStartedAt: new Date(execution.waitingForFinalizeStartedAt!),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'finalize_failed',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        waitingForChildrenStartedAt: new Date(execution.waitingForChildrenStartedAt!),
        waitingForFinalizeStartedAt: new Date(execution.waitingForFinalizeStartedAt!),
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        status: 'completed',
        output: output!,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
        startedAt: new Date(execution.startedAt!),
        expiresAt: new Date(execution.expiresAt!),
        waitingForChildrenStartedAt: execution.waitingForChildrenStartedAt
          ? new Date(execution.waitingForChildrenStartedAt)
          : undefined,
        waitingForFinalizeStartedAt: execution.waitingForFinalizeStartedAt
          ? new Date(execution.waitingForFinalizeStartedAt)
          : undefined,
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
        sleepingTaskUniqueId: execution.sleepingTaskUniqueId,
        areChildrenSequential: execution.areChildrenSequential,
        retryOptions: execution.retryOptions,
        sleepMsBeforeRun: execution.sleepMsBeforeRun,
        timeoutMs: execution.timeoutMs,
        input,
        executorId: execution.executorId,
        status: 'cancelled',
        error: execution.error!,
        retryAttempts: execution.retryAttempts,
        startAt: new Date(execution.startAt),
        startedAt: execution.startedAt ? new Date(execution.startedAt) : undefined,
        expiresAt: execution.expiresAt ? new Date(execution.expiresAt) : undefined,
        waitingForChildrenStartedAt: execution.waitingForChildrenStartedAt
          ? new Date(execution.waitingForChildrenStartedAt)
          : undefined,
        waitingForFinalizeStartedAt: execution.waitingForFinalizeStartedAt
          ? new Date(execution.waitingForFinalizeStartedAt)
          : undefined,
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

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    return await this.withMutex(() => this.storage.insertMany(executions))
  }

  async getManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.withMutex(() => this.storage.getManyById(requests))
  }

  async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{
      sleepingTaskUniqueId: string
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.withMutex(() => this.storage.getManyBySleepingTaskUniqueId(requests))
  }

  async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.withMutex(() => this.storage.updateManyById(requests))
  }

  async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    return await this.withMutex(() =>
      this.storage.updateManyByIdAndInsertChildrenIfUpdated(requests),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndStartAtLessThanAndReturn(request),
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(request: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
        request,
      ),
    )
  }

  async updateByCloseStatusAndReturn(request: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => this.storage.updateByCloseStatusAndReturn(request))
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request),
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(request: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(request),
    )
  }

  async updateByCloseExpiresAtLessThan(request: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() => this.storage.updateByCloseExpiresAtLessThan(request))
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() =>
      this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request),
    )
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{
      parentExecutionId: string
    }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    return await this.withMutex(() => this.storage.getManyByParentExecutionId(requests))
  }

  async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    return await this.withMutex(() =>
      this.storage.updateManyByParentExecutionIdAndIsFinished(requests),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() =>
      this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request),
    )
  }

  async deleteById(request: { executionId: string }): Promise<void> {
    return await this.withMutex(() => this.storage.deleteById(request))
  }

  async deleteAll(): Promise<void> {
    return await this.withMutex(() => this.storage.deleteAll())
  }
}

/**
 * A storage interface for task executions without batching. It is similar to
 * {@link TaskExecutionsStorage} but without the batching methods.
 *
 * @category Storage
 */
export type TaskExecutionsStorageWithoutBatching = Omit<
  TaskExecutionsStorage,
  | 'getManyById'
  | 'getManyBySleepingTaskUniqueId'
  | 'updateManyById'
  | 'updateManyByIdAndInsertChildrenIfUpdated'
  | 'getManyByParentExecutionId'
  | 'updateManyByParentExecutionIdAndIsFinished'
> & {
  /**
   * Get task execution by id and optionally filter them.
   *
   * @param request - The request object.
   * @param request.executionId - The id of the task execution to get.
   * @param request.filters - The filters to filter the task execution.
   * @returns The task execution.
   */
  getById: (request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
  }) => TaskExecutionStorageValue | undefined | Promise<TaskExecutionStorageValue | undefined>

  /**
   * Get task execution by sleeping task unique id.
   *
   * @param request - The request object.
   * @param request.sleepingTaskUniqueId - The unique id of the sleeping task to get.
   * @returns The task execution.
   */
  getBySleepingTaskUniqueId: (request: {
    sleepingTaskUniqueId: string
  }) => TaskExecutionStorageValue | undefined | Promise<TaskExecutionStorageValue | undefined>

  /**
   * Update task execution by id and optionally filter them.
   *
   * @param request - The request object.
   * @param request.executionId - The id of the task execution to update.
   * @param request.filters - The filters to filter the task execution.
   * @param request.update - The update object.
   */
  updateById: (request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
  }) => void | Promise<void>

  /**
   * Update task execution by id and insert children task executions if updated.
   *
   * @param request - The request object.
   * @param request.executionId - The id of the task execution to update.
   * @param request.filters - The filters to filter the task execution.
   * @param request.update - The update object.
   * @param request.childrenTaskExecutionsToInsertIfAnyUpdated - The children task executions to
   *   insert if the task execution was updated.
   */
  updateByIdAndInsertChildrenIfUpdated: (request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
    childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
  }) => void | Promise<void>

  /**
   * Get task executions by parent execution id.
   *
   * @param request - The request object.
   * @param request.parentExecutionId - The id of the parent task execution to get.
   * @returns The task executions.
   */
  getByParentExecutionId: (request: {
    parentExecutionId: string
  }) => Array<TaskExecutionStorageValue> | Promise<Array<TaskExecutionStorageValue>>

  /**
   * Update task executions by parent execution id and is finished.
   *
   * @param request - The request object.
   * @param request.parentExecutionId - The id of the parent task execution to update.
   * @param request.isFinished - The is finished of the task executions to update.
   * @param request.update - The update object.
   */
  updateByParentExecutionIdAndIsFinished: (request: {
    parentExecutionId: string
    isFinished: boolean
    update: TaskExecutionStorageUpdate
  }) => void | Promise<void>
}

/**
 * Wraps a {@link TaskExecutionsStorageWithoutBatching} and implements batching methods.
 *
 * @category Storage
 */
export class TaskExecutionsStorageWithBatching implements TaskExecutionsStorage {
  private readonly storage: TaskExecutionsStorageWithoutBatching
  private readonly disableParallelRequests: boolean

  constructor(storage: TaskExecutionsStorageWithoutBatching, disableParallelRequests = false) {
    this.storage = storage
    this.disableParallelRequests = disableParallelRequests
  }

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    return await this.storage.insertMany(executions)
  }

  async getManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    if (this.disableParallelRequests) {
      const results: Array<TaskExecutionStorageValue | undefined> = []
      for (const request of requests) {
        const result = await this.storage.getById(request)
        results.push(result)
      }
      return results
    }

    return await Promise.all(requests.map((request) => this.storage.getById(request)))
  }

  async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{
      sleepingTaskUniqueId: string
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    if (this.disableParallelRequests) {
      const results: Array<TaskExecutionStorageValue | undefined> = []
      for (const request of requests) {
        const result = await this.storage.getBySleepingTaskUniqueId(request)
        results.push(result)
      }
      return results
    }

    return await Promise.all(
      requests.map((request) => this.storage.getBySleepingTaskUniqueId(request)),
    )
  }

  async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    if (this.disableParallelRequests) {
      for (const request of requests) {
        await this.storage.updateById(request)
      }
      return
    }

    await Promise.all(requests.map((request) => this.storage.updateById(request)))
  }

  async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    if (this.disableParallelRequests) {
      for (const request of requests) {
        await this.storage.updateByIdAndInsertChildrenIfUpdated(request)
      }
      return
    }

    await Promise.all(
      requests.map((request) => this.storage.updateByIdAndInsertChildrenIfUpdated(request)),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.storage.updateByStatusAndStartAtLessThanAndReturn(request)
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(request: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
      request,
    )
  }

  async updateByCloseStatusAndReturn(request: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.storage.updateByCloseStatusAndReturn(request)
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request)
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(request: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(request)
  }

  async updateByCloseExpiresAtLessThan(request: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.storage.updateByCloseExpiresAtLessThan(request)
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request)
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{
      parentExecutionId: string
    }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    if (this.disableParallelRequests) {
      const results: Array<Array<TaskExecutionStorageValue>> = []
      for (const request of requests) {
        const result = await this.storage.getByParentExecutionId(request)
        results.push(result)
      }
      return results
    }

    return await Promise.all(
      requests.map((request) => this.storage.getByParentExecutionId(request)),
    )
  }

  async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    if (this.disableParallelRequests) {
      for (const request of requests) {
        await this.storage.updateByParentExecutionIdAndIsFinished(request)
      }
      return
    }

    await Promise.all(
      requests.map((request) => this.storage.updateByParentExecutionIdAndIsFinished(request)),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
      request,
    )
  }

  async deleteById(request: { executionId: string }): Promise<void> {
    return await this.storage.deleteById(request)
  }

  async deleteAll(): Promise<void> {
    return await this.storage.deleteAll()
  }
}
