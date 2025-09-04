import { Duration, Effect, Metric, Schedule } from 'effect'

import { omitUndefinedValues } from '@gpahal/std/objects'

import { makeBackgroundBatchProcessor } from './background-batch-processor'
import { DurableExecutionError } from './errors'
import {
  TaskExecutionsStorageService,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from './storage'
import {
  ERRORED_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  type TaskExecutionStatus,
} from './task'
import { convertMaybePromiseToEffect } from './utils'

/**
 * Internal storage update type that excludes timestamp fields and some other fields that are
 * managed automatically.
 *
 * @internal
 */
export type TaskExecutionStorageUpdateInternal = Omit<
  TaskExecutionStorageUpdate,
  | 'isFinished'
  | 'startedAt'
  | 'finishedAt'
  | 'onChildrenFinishedProcessingFinishedAt'
  | 'closedAt'
  | 'updatedAt'
  | 'unset'
> & {
  isFinished?: never
  startedAt?: never
  finishedAt?: never
  onChildrenFinishedProcessingFinishedAt?: never
  closedAt?: never
  updatedAt?: never

  unset?: Omit<
    TaskExecutionStorageUpdate['unset'],
    | 'executorId'
    | 'runOutput'
    | 'error'
    | 'startedAt'
    | 'expiresAt'
    | 'onChildrenFinishedProcessingExpiresAt'
    | 'closeExpiresAt'
  > & {
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
 * Converts an internal storage update to a complete storage update with timestamps.
 *
 * This function adds the fields that are automatically managed:
 * - Sets `updatedAt` to the current time
 * - Sets `isFinished` and `finishedAt` for terminal status transitions
 * - Clears temporary fields like `runOutput` for finished tasks
 * - Manages expiration and cleanup timestamps based on status
 *
 * The logic ensures consistent state transitions and proper cleanup of resources as tasks move
 * through their lifecycle.
 *
 * @param now - Current timestamp in milliseconds
 * @param internalUpdate - Update object without managed timestamp fields
 * @returns Complete storage update with all managed fields set
 *
 * @internal
 */
export function getTaskExecutionStorageUpdate(
  now: number,
  internalUpdate: TaskExecutionStorageUpdateInternal,
): TaskExecutionStorageUpdate {
  const update: TaskExecutionStorageUpdate = {
    ...internalUpdate,
    isFinished: undefined,
    startedAt: undefined,
    finishedAt: undefined,
    onChildrenFinishedProcessingFinishedAt: undefined,
    closedAt: undefined,
    updatedAt: now,
    unset: {
      ...internalUpdate.unset,
      executorId: undefined,
      runOutput: undefined,
      error: undefined,
      startedAt: undefined,
      expiresAt: undefined,
      onChildrenFinishedProcessingExpiresAt: undefined,
      closeExpiresAt: undefined,
    },
  }
  const updateUnset: TaskExecutionStorageUpdate['unset'] = {
    ...internalUpdate.unset,
    executorId: undefined,
    runOutput: undefined,
    error: undefined,
    startedAt: undefined,
    expiresAt: undefined,
    onChildrenFinishedProcessingExpiresAt: undefined,
    closeExpiresAt: undefined,
  }
  if (internalUpdate.status) {
    if (FINISHED_TASK_EXECUTION_STATUSES.includes(internalUpdate.status)) {
      update.isFinished = true
      updateUnset.runOutput = true
      update.finishedAt = now
    }
    if (internalUpdate.status === 'ready') {
      updateUnset.runOutput = true
      updateUnset.startedAt = true
      updateUnset.expiresAt = true
    }
    if (internalUpdate.status === 'running') {
      update.startedAt = now
    }
    if (internalUpdate.status === 'waiting_for_children') {
      update.waitingForChildrenStartedAt = now
    }
    if (internalUpdate.status === 'waiting_for_finalize') {
      update.waitingForFinalizeStartedAt = now
    }
    if (
      !ERRORED_TASK_EXECUTION_STATUSES.includes(internalUpdate.status) &&
      internalUpdate.status !== 'ready' &&
      internalUpdate.status !== 'running'
    ) {
      updateUnset.error = true
    }
    if (
      (internalUpdate.status !== 'running' && internalUpdate.status !== 'cancelled') ||
      (update.needsPromiseCancellation != null && !update.needsPromiseCancellation)
    ) {
      updateUnset.executorId = true
    }
    if (internalUpdate.status !== 'running') {
      updateUnset.expiresAt = true
    }
  }
  if (internalUpdate.onChildrenFinishedProcessingStatus) {
    if (internalUpdate.onChildrenFinishedProcessingStatus !== 'processing') {
      updateUnset.onChildrenFinishedProcessingExpiresAt = true
    }
    if (internalUpdate.onChildrenFinishedProcessingStatus === 'processed') {
      update.onChildrenFinishedProcessingFinishedAt = now
    }
  }
  if (internalUpdate.closeStatus) {
    if (internalUpdate.closeStatus !== 'closing') {
      updateUnset.closeExpiresAt = true
    }
    if (internalUpdate.closeStatus === 'closed') {
      update.closedAt = now
    }
  }

  update.unset = omitUndefinedValues(updateUnset)
  return omitUndefinedValues(update)
}

export type TaskExecutionsStorageInternalOptions = {
  executorId: string
  enableBatching: boolean
  enableStats: boolean
  backgroundBatchingIntraBatchSleepMs: number
  maxRetryAttempts?: number
}

export const makeTaskExecutionsStorageInternal = Effect.fn(function* (
  options: TaskExecutionsStorageInternalOptions,
) {
  const {
    executorId,
    enableBatching,
    enableStats,
    backgroundBatchingIntraBatchSleepMs,
    maxRetryAttempts: maxRetryAttemptsOption,
  } = options
  const maxRetryAttempts =
    maxRetryAttemptsOption == null ? 1 : Math.max(0, Math.min(10, maxRetryAttemptsOption))

  const storage = yield* TaskExecutionsStorageService

  const metricsMap = new Map<string, Metric.Metric.Summary<number>>()

  const withStats = Effect.fnUntraced(function* <T, E>(
    processName: string,
    effect: Effect.Effect<T, E, never>,
  ) {
    const [duration, result] = yield* effect.pipe(Effect.timed)

    if (!metricsMap.has(processName)) {
      metricsMap.set(
        processName,
        Metric.summary({
          name: processName,
          maxAge: Duration.minutes(30),
          maxSize: 100_000,
          error: 0.025,
          quantiles: [0.5, 0.9, 0.95],
        }),
      )
    }

    const metric = metricsMap.get(processName)!
    yield* metric(Effect.succeed(Duration.toMillis(duration)))
    return result
  })

  const wrapStorageEffect = <T, E>(processName: string, effect: Effect.Effect<T, E, never>) => {
    return maxRetryAttempts <= 0
      ? enableStats
        ? withStats(processName, effect)
        : effect
      : (enableStats ? withStats(processName, effect) : effect).pipe(
          Effect.retry({
            times: maxRetryAttempts,
            schedule: Schedule.exponential(Duration.millis(25), 2).pipe(
              Schedule.modifyDelay(Duration.min(Duration.millis(1000))),
            ),
            until: (error) => error instanceof DurableExecutionError && !error.isRetryable,
          }),
        )
  }

  const insertManyUnbatched = Effect.fn(function* (
    executions: ReadonlyArray<TaskExecutionStorageValue>,
  ) {
    if (executions.length === 0) {
      return
    }
    yield* wrapStorageEffect(
      'insertMany',
      convertMaybePromiseToEffect(() => storage.insertMany(executions)),
    )
  })

  const insertManyBackgroundBatchProcessor = yield* makeBackgroundBatchProcessor<
    ReadonlyArray<TaskExecutionStorageValue>,
    void
  >({
    executorId,
    processName: 'insertMany',
    enableBatching,
    intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
    batchWeight: 100,
    processBatchRequests: (requests) => insertManyUnbatched(requests.flat()),
  })

  const insertMany = Effect.fn(function* (requests: ReadonlyArray<TaskExecutionStorageValue>) {
    return yield* insertManyBackgroundBatchProcessor.processRequest({
      input: requests,
      weight: requests.length,
      enableBatching: requests.length <= 5,
    })
  })

  const getManyByIdUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{ executionId: string; filters?: TaskExecutionStorageGetByIdFilters }>,
  ) {
    if (requests.length === 0) {
      return []
    }
    return yield* wrapStorageEffect(
      'getManyById',
      convertMaybePromiseToEffect(() => storage.getManyById(requests)),
    )
  })

  const getManyByIdBackgroundBatchProcessor = yield* makeBackgroundBatchProcessor<
    { executionId: string; filters?: TaskExecutionStorageGetByIdFilters },
    TaskExecutionStorageValue | undefined
  >({
    executorId,
    processName: 'getManyById',
    enableBatching,
    intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
    batchWeight: 100,
    processBatchRequests: getManyByIdUnbatched,
  })

  const getById = Effect.fn(function* (request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
  }) {
    return yield* getManyByIdBackgroundBatchProcessor.processRequest({
      input: request,
    })
  })

  const getManyBySleepingTaskUniqueIdUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{ sleepingTaskUniqueId: string }>,
  ) {
    if (requests.length === 0) {
      return []
    }
    return yield* wrapStorageEffect(
      'getManyBySleepingTaskUniqueId',
      convertMaybePromiseToEffect(() => storage.getManyBySleepingTaskUniqueId(requests)),
    )
  })

  const getManyBySleepingTaskUniqueIdBackgroundBatchProcessor = yield* makeBackgroundBatchProcessor<
    { sleepingTaskUniqueId: string },
    TaskExecutionStorageValue | undefined
  >({
    executorId,
    processName: 'getManyBySleepingTaskUniqueId',
    enableBatching,
    intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
    batchWeight: 50,
    processBatchRequests: getManyBySleepingTaskUniqueIdUnbatched,
  })

  const getBySleepingTaskUniqueId = Effect.fn(function* (request: {
    sleepingTaskUniqueId: string
  }) {
    return yield* getManyBySleepingTaskUniqueIdBackgroundBatchProcessor.processRequest({
      input: request,
    })
  })

  const updateManyByIdUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ) {
    if (requests.length === 0) {
      return
    }
    yield* wrapStorageEffect(
      'updateManyById',
      convertMaybePromiseToEffect(() => storage.updateManyById(requests)),
    )
  })

  const updateManyByIdBackgroundBatchProcessor = yield* makeBackgroundBatchProcessor<
    {
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    },
    void
  >({
    executorId,
    processName: 'updateManyById',
    enableBatching,
    intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
    batchWeight: 100,
    processBatchRequests: updateManyByIdUnbatched,
  })

  const updateById = Effect.fn(function* (
    now: number,
    request: {
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdateInternal
    },
    execution?: TaskExecutionStorageValue,
  ) {
    let finalUpdate = request.update
    if (
      request.update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(request.update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      finalUpdate = { ...request.update, closeStatus: 'ready' }
    }

    return yield* updateManyByIdBackgroundBatchProcessor.processRequest({
      input: {
        ...request,
        update: getTaskExecutionStorageUpdate(now, finalUpdate),
      },
    })
  })

  const updateManyByIdAndInsertChildrenIfUpdatedUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ) {
    if (requests.length === 0) {
      return
    }
    yield* wrapStorageEffect(
      'updateManyByIdAndInsertChildrenIfUpdated',
      convertMaybePromiseToEffect(() => storage.updateManyByIdAndInsertChildrenIfUpdated(requests)),
    )
  })

  const updateManyByIdAndInsertChildrenIfUpdatedBackgroundBatchProcessor =
    yield* makeBackgroundBatchProcessor<
      {
        executionId: string
        filters?: TaskExecutionStorageGetByIdFilters
        update: TaskExecutionStorageUpdate
        childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
      },
      void
    >({
      executorId,
      processName: 'updateManyByIdAndInsertChildrenIfUpdated',
      enableBatching,
      intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
      batchWeight: 100,
      processBatchRequests: (requests) =>
        updateManyByIdAndInsertChildrenIfUpdatedUnbatched(requests),
    })

  const updateByIdAndInsertChildrenIfUpdated = Effect.fn(function* (
    now: number,
    request: {
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdateInternal
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    },
    execution?: TaskExecutionStorageValue,
  ) {
    if (request.childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return yield* updateById(now, request, execution)
    }

    let finalUpdate = request.update
    if (
      request.update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(request.update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      finalUpdate = { ...request.update, closeStatus: 'ready' }
    }

    return yield* updateManyByIdAndInsertChildrenIfUpdatedBackgroundBatchProcessor.processRequest({
      input: {
        ...request,
        update: getTaskExecutionStorageUpdate(now, finalUpdate),
      },
      weight: request.childrenTaskExecutionsToInsertIfAnyUpdated.length,
      enableBatching: request.childrenTaskExecutionsToInsertIfAnyUpdated.length <= 5,
    })
  })

  const updateByStatusAndStartAtLessThanAndReturn = Effect.fn(function* (
    now: number,
    request: {
      status: TaskExecutionStatus
      startAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      updateExpiresAtWithStartedAt: number
      limit: number
    },
  ) {
    if (request.limit <= 0) {
      return []
    }
    return yield* wrapStorageEffect(
      'updateByStatusAndStartAtLessThanAndReturn',
      convertMaybePromiseToEffect(() =>
        storage.updateByStatusAndStartAtLessThanAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn =
    Effect.fn(function* (
      now: number,
      request: {
        status: TaskExecutionStatus
        onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
        update: TaskExecutionStorageUpdateInternal
        limit: number
      },
    ) {
      if (request.limit <= 0) {
        return []
      }
      return yield* wrapStorageEffect(
        'updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn',
        convertMaybePromiseToEffect(() =>
          storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
            {
              ...request,
              update: getTaskExecutionStorageUpdate(now, request.update),
            },
          ),
        ),
      )
    })

  const updateByCloseStatusAndReturn = Effect.fn(function* (
    now: number,
    request: {
      closeStatus: TaskExecutionCloseStatus
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
  ) {
    if (request.limit <= 0) {
      return []
    }
    return yield* wrapStorageEffect(
      'updateByCloseStatusAndReturn',
      convertMaybePromiseToEffect(() =>
        storage.updateByCloseStatusAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const updateByStatusAndIsSleepingTaskAndExpiresAtLessThan = Effect.fn(function* (
    now: number,
    request: {
      status: TaskExecutionStatus
      isSleepingTask: boolean
      expiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
  ) {
    if (request.limit <= 0) {
      return 0
    }
    return yield* wrapStorageEffect(
      'updateByStatusAndIsSleepingTaskAndExpiresAtLessThan',
      convertMaybePromiseToEffect(() =>
        storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const updateByOnChildrenFinishedProcessingExpiresAtLessThan = Effect.fn(function* (
    now: number,
    request: {
      onChildrenFinishedProcessingExpiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
  ) {
    if (request.limit <= 0) {
      return 0
    }
    return yield* wrapStorageEffect(
      'updateByOnChildrenFinishedProcessingExpiresAtLessThan',
      convertMaybePromiseToEffect(() =>
        storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const updateByCloseExpiresAtLessThan = Effect.fn(function* (
    now: number,
    request: {
      closeExpiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
  ) {
    if (request.limit <= 0) {
      return 0
    }
    return yield* wrapStorageEffect(
      'updateByCloseExpiresAtLessThan',
      convertMaybePromiseToEffect(() =>
        storage.updateByCloseExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const updateByExecutorIdAndNeedsPromiseCancellationAndReturn = Effect.fn(function* (
    now: number,
    request: {
      executorId: string
      needsPromiseCancellation: boolean
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
  ) {
    return yield* wrapStorageEffect(
      'updateByExecutorIdAndNeedsPromiseCancellationAndReturn',
      convertMaybePromiseToEffect(() =>
        storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      ),
    )
  })

  const getManyByParentExecutionIdUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{ parentExecutionId: string }>,
  ) {
    if (requests.length === 0) {
      return []
    }
    return yield* wrapStorageEffect(
      'getManyByParentExecutionId',
      convertMaybePromiseToEffect(() => storage.getManyByParentExecutionId(requests)),
    )
  })

  const getManyByParentExecutionIdBackgroundBatchProcessor = yield* makeBackgroundBatchProcessor<
    { parentExecutionId: string },
    Array<TaskExecutionStorageValue>
  >({
    executorId,
    processName: 'getManyByParentExecutionId',
    enableBatching,
    intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
    batchWeight: 3,
    processBatchRequests: getManyByParentExecutionIdUnbatched,
  })

  const getByParentExecutionId = Effect.fn(function* (request: { parentExecutionId: string }) {
    return yield* getManyByParentExecutionIdBackgroundBatchProcessor.processRequest({
      input: request,
    })
  })

  const updateManyByParentExecutionIdAndIsFinishedUnbatched = Effect.fn(function* (
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ) {
    if (requests.length === 0) {
      return
    }
    yield* wrapStorageEffect(
      'updateManyByParentExecutionIdAndIsFinished',
      convertMaybePromiseToEffect(() =>
        storage.updateManyByParentExecutionIdAndIsFinished(requests),
      ),
    )
  })

  const updateManyByParentExecutionIdAndIsFinishedBackgroundBatchProcessor =
    yield* makeBackgroundBatchProcessor<
      { parentExecutionId: string; isFinished: boolean; update: TaskExecutionStorageUpdate },
      void
    >({
      executorId,
      processName: 'updateManyByParentExecutionIdAndIsFinished',
      enableBatching,
      intraBatchSleepMs: backgroundBatchingIntraBatchSleepMs,
      batchWeight: 3,
      processBatchRequests: updateManyByParentExecutionIdAndIsFinishedUnbatched,
    })

  const updateByParentExecutionIdAndIsFinished = Effect.fn(function* (
    now: number,
    request: {
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdateInternal
    },
  ) {
    return yield* updateManyByParentExecutionIdAndIsFinishedBackgroundBatchProcessor.processRequest(
      {
        input: {
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        },
      },
    )
  })

  const updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus = Effect.fn(
    function* (
      now: number,
      request: {
        isFinished: boolean
        closeStatus: TaskExecutionCloseStatus
        update: TaskExecutionStorageUpdateInternal
        limit: number
      },
    ) {
      if (request.limit <= 0) {
        return 0
      }
      return yield* wrapStorageEffect(
        'updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus',
        convertMaybePromiseToEffect(() =>
          storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
            ...request,
            update: getTaskExecutionStorageUpdate(now, request.update),
          }),
        ),
      )
    },
  )

  const backgroundBatchProcessors = [
    insertManyBackgroundBatchProcessor,
    getManyByIdBackgroundBatchProcessor,
    updateManyByIdBackgroundBatchProcessor,
    updateManyByIdAndInsertChildrenIfUpdatedBackgroundBatchProcessor,
    getManyByParentExecutionIdBackgroundBatchProcessor,
    updateManyByParentExecutionIdAndIsFinishedBackgroundBatchProcessor,
  ]

  const start = Effect.all(
    backgroundBatchProcessors.map((processor) => processor.start),
    { concurrency: 'unbounded', discard: true },
  )

  const shutdown = Effect.all(
    backgroundBatchProcessors.map((processor) => processor.shutdown),
    { concurrency: 'unbounded', discard: true },
  )

  const getMetric = Effect.fnUntraced(function* ([processName, metric]: [
    string,
    Metric.Metric.Summary<number>,
  ]) {
    const value = yield* Metric.value(metric)
    return {
      processName,
      count: value.count,
      min: value.min,
      max: value.max,
      quantiles: value.quantiles,
    }
  })

  const getMetrics = Effect.gen(function* () {
    const metrics = yield* Effect.forEach(metricsMap.entries(), getMetric, {
      concurrency: 'unbounded',
    })
    metrics.sort((a, b) => b.count - a.count)
    return metrics
  })

  return {
    insertMany,
    getById,
    getBySleepingTaskUniqueId,
    updateById,
    updateByIdAndInsertChildrenIfUpdated,
    updateByStatusAndStartAtLessThanAndReturn,
    updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn,
    updateByCloseStatusAndReturn,
    updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
    updateByOnChildrenFinishedProcessingExpiresAtLessThan,
    updateByCloseExpiresAtLessThan,
    updateByExecutorIdAndNeedsPromiseCancellationAndReturn,
    getByParentExecutionId,
    updateByParentExecutionIdAndIsFinished,
    updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus,
    start,
    shutdown,
    getMetrics,
  }
})

export type TaskExecutionsStorageInternal = Effect.Effect.Success<
  ReturnType<typeof makeTaskExecutionsStorageInternal>
>
