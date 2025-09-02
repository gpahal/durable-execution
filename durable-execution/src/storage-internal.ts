import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
import { getErrorMessage } from '@gpahal/std/errors'
import { omitUndefinedValues } from '@gpahal/std/objects'
import { sleep, sleepWithWakeup } from '@gpahal/std/promises'

import { DurableExecutionCancelledError, DurableExecutionError } from './errors'
import type { LoggerInternal } from './logger'
import { PromisePool } from './promise-pool'
import {
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from './storage'
import {
  ERRORED_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  type TaskExecutionStatus,
} from './task'

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

const BATCH_REQUESTER_MAX_CONSECUTIVE_ERRORS = 5

/**
 * A type that can be used to batch requests to a storage implementation.
 */
type BatchRequest<T, R> = {
  data: T
  resolve: (value: R) => void
  reject: (reason?: unknown) => void
}

/**
 * A class that can be used to batch requests to a storage implementation.
 */
class BatchRequester<T, R> {
  private readonly logger: LoggerInternal
  private readonly processName: string
  private readonly batchSize: number
  private readonly backgroundProcessIntraBatchSleepMs: number
  private readonly singleRequestsBatchProcessFn: (
    requests: ReadonlyArray<T>,
  ) =>
    | ReadonlyArray<R>
    | null
    | undefined
    | void
    | Promise<ReadonlyArray<R> | null | undefined | void>
  private readonly shutdownSignal: CancelSignal
  private readonly backgroundPromisePool: PromisePool

  private wakeupBackgroundProcess: (() => void) | undefined
  private pendingRequests: Array<BatchRequest<T, R>>

  constructor(
    logger: LoggerInternal,
    processName: string,
    batchSize: number,
    backgroundProcessIntraBatchSleepMs: number,
    singleRequestsBatchProcessFn: (
      requests: ReadonlyArray<T>,
    ) =>
      | ReadonlyArray<R>
      | null
      | undefined
      | void
      | Promise<ReadonlyArray<R> | null | undefined | void>,
    shutdownSignal: CancelSignal,
    backgroundPromisePool: PromisePool,
  ) {
    this.logger = logger
    this.processName = processName
    this.batchSize = batchSize
    this.backgroundProcessIntraBatchSleepMs = backgroundProcessIntraBatchSleepMs
    this.singleRequestsBatchProcessFn = singleRequestsBatchProcessFn
    this.shutdownSignal = shutdownSignal
    this.backgroundPromisePool = backgroundPromisePool
    this.pendingRequests = []
  }

  private async sleepWithWakeup(ms: number, jitterRatio = 0): Promise<void> {
    const [promise, resolve] = sleepWithWakeup(ms, jitterRatio)
    this.wakeupBackgroundProcess = resolve
    await promise
    this.wakeupBackgroundProcess = undefined
  }

  shutdown(): void {
    for (const request of this.pendingRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }
    this.pendingRequests = []
  }

  async runBackgroundProcess(): Promise<void> {
    let consecutiveErrors = 0

    const originalBackgroundProcessIntraBatchSleepMs = this.backgroundProcessIntraBatchSleepMs
    let backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs

    const runBackgroundProcessSingleBatch = async (
      requests: ReadonlyArray<BatchRequest<T, R>>,
    ): Promise<void> => {
      try {
        const results = await this.singleRequestsBatchProcessFn(
          requests.map((request) => request.data),
        )
        if (results == null) {
          for (const request of requests) {
            request.resolve(undefined as R)
          }
        } else if (results.length !== requests.length) {
          const error = DurableExecutionError.nonRetryable(
            `Batch processing returned ${results.length} results but expected ${requests.length}`,
          )
          this.logger.error(
            `Error in batch requester ${this.processName}: result count mismatch`,
            error,
          )
          for (const request of requests) {
            request.reject(error)
          }
        } else {
          for (const [i, request] of requests.entries()) {
            request.resolve(results[i]!)
          }
        }

        consecutiveErrors = 0
        backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs
      } catch (error) {
        for (const request of requests) {
          request.reject(error)
        }

        if (error instanceof DurableExecutionCancelledError && this.shutdownSignal.isCancelled()) {
          return
        }

        consecutiveErrors++
        this.logger.error(
          `Error in batch requester ${this.processName}: consecutive_errors=${consecutiveErrors}`,
          error,
        )

        if (consecutiveErrors >= BATCH_REQUESTER_MAX_CONSECUTIVE_ERRORS) {
          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.25,
            originalBackgroundProcessIntraBatchSleepMs * 5,
          )
        }
      }
    }

    await this.sleepWithWakeup(backgroundProcessIntraBatchSleepMs, 0.25)
    let skippedBatchCount = 0
    while (true) {
      try {
        const requests = this.pendingRequests
        this.pendingRequests = []
        if (requests.length === 0) {
          if (this.shutdownSignal.isCancelled()) {
            break
          }

          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.125,
            originalBackgroundProcessIntraBatchSleepMs * 2,
          )
          await this.sleepWithWakeup(backgroundProcessIntraBatchSleepMs)
          skippedBatchCount++
          continue
        } else {
          backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs
          if (skippedBatchCount < 5 && requests.length < this.batchSize / 3) {
            await this.sleepWithWakeup(backgroundProcessIntraBatchSleepMs)
            skippedBatchCount++
          } else {
            skippedBatchCount = 0
          }
        }

        for (let i = 0; i < requests.length; i += this.batchSize) {
          this.backgroundPromisePool.addPromise(
            runBackgroundProcessSingleBatch(requests.slice(i, i + this.batchSize)),
          )
        }
      } catch (error) {
        this.logger.error(`Error in batch requester ${this.processName}`, error)
      }
    }
  }

  addRequest(request: T): Promise<R> {
    const promise = new Promise<R>((resolve, reject) => {
      this.pendingRequests.push({ data: request, resolve, reject })
      if (
        this.wakeupBackgroundProcess != null &&
        this.pendingRequests.length >= 0.75 * this.batchSize
      ) {
        this.wakeupBackgroundProcess()
      }
    })
    return promise
  }
}

export const zStorageMaxRetryAttempts = z
  .number()
  .min(0)
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
  private readonly enableBatching: boolean
  private readonly enableStats: boolean
  private readonly baseBackgroundProcessIntraBatchSleepMs: number
  private readonly maxRetryAttempts: number
  private isStarted: boolean
  private readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private readonly backgroundProcessesPromisePool: PromisePool
  private readonly backgroundPromisePool: PromisePool

  private readonly callsDurationsStats: Map<string, { callsCount: number; meanDurationMs: number }>
  private readonly perSecondCallsCountsStats: {
    totalSeconds: number
    totalCallsCount: number
    meanCallsCount: number
    maxCallsCount: number
    recentPerSecondCallsCounts: Map<number, number>
  }

  private readonly insertManyBatchRequester:
    | BatchRequester<ReadonlyArray<TaskExecutionStorageValue>, void>
    | undefined
  private readonly getByIdBatchRequester:
    | BatchRequester<
        { executionId: string; filters?: TaskExecutionStorageGetByIdFilters },
        TaskExecutionStorageValue | undefined
      >
    | undefined
  private readonly getBySleepingTaskUniqueIdBatchRequester:
    | BatchRequester<{ sleepingTaskUniqueId: string }, TaskExecutionStorageValue | undefined>
    | undefined
  private readonly updateByIdBatchRequester:
    | BatchRequester<
        {
          executionId: string
          filters?: TaskExecutionStorageGetByIdFilters
          update: TaskExecutionStorageUpdate
        },
        void
      >
    | undefined
  private readonly updateByIdAndInsertChildrenIfUpdatedBatchRequester:
    | BatchRequester<
        {
          executionId: string
          filters?: TaskExecutionStorageGetByIdFilters
          update: TaskExecutionStorageUpdate
          childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
        },
        void
      >
    | undefined
  private readonly getByParentExecutionIdBatchRequester:
    | BatchRequester<{ parentExecutionId: string }, Array<TaskExecutionStorageValue>>
    | undefined
  private readonly updateByParentExecutionIdAndIsFinishedBatchRequester:
    | BatchRequester<
        {
          parentExecutionId: string
          isFinished: boolean
          update: TaskExecutionStorageUpdate
        },
        void
      >
    | undefined
  private readonly batchRequesters: ReadonlyArray<BatchRequester<unknown, unknown>>

  constructor(
    logger: LoggerInternal,
    storage: TaskExecutionsStorage,
    enableBatching: boolean,
    enableStats: boolean,
    baseBackgroundProcessIntraBatchSleepMs: number,
    maxRetryAttempts?: number,
  ) {
    this.logger = logger
    this.storage = storage
    this.enableBatching = enableBatching
    this.enableStats = enableStats
    this.baseBackgroundProcessIntraBatchSleepMs = baseBackgroundProcessIntraBatchSleepMs
    this.maxRetryAttempts = validateStorageMaxRetryAttempts(maxRetryAttempts)
    this.isStarted = false

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.backgroundProcessesPromisePool = new PromisePool()
    this.backgroundPromisePool = new PromisePool()
    this.callsDurationsStats = new Map()
    this.perSecondCallsCountsStats = {
      totalSeconds: 0,
      totalCallsCount: 0,
      meanCallsCount: 0,
      maxCallsCount: 0,
      recentPerSecondCallsCounts: new Map(),
    }

    if (this.enableBatching) {
      this.insertManyBatchRequester = new BatchRequester(
        logger,
        'insertMany',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.insertManyBatched(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.getByIdBatchRequester = new BatchRequester(
        logger,
        'getById',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.getManyById(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.getBySleepingTaskUniqueIdBatchRequester = new BatchRequester(
        logger,
        'getBySleepingTaskUniqueId',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.getManyBySleepingTaskUniqueId(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.updateByIdBatchRequester = new BatchRequester(
        logger,
        'updateById',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.updateManyById(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.updateByIdAndInsertChildrenIfUpdatedBatchRequester = new BatchRequester(
        logger,
        'updateByIdAndInsertChildrenIfUpdated',
        50,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.updateManyByIdAndInsertChildrenIfUpdated(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.getByParentExecutionIdBatchRequester = new BatchRequester(
        logger,
        'getByParentExecutionId',
        3,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.getManyByParentExecutionId(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.updateByParentExecutionIdAndIsFinishedBatchRequester = new BatchRequester(
        logger,
        'updateByParentExecutionIdAndIsFinished',
        5,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.updateManyByParentExecutionIdAndIsFinished(requests),
        this.shutdownSignal,
        this.backgroundPromisePool,
      )
      this.batchRequesters = [
        this.insertManyBatchRequester as BatchRequester<unknown, unknown>,
        this.getByIdBatchRequester as BatchRequester<unknown, unknown>,
        this.getBySleepingTaskUniqueIdBatchRequester as BatchRequester<unknown, unknown>,
        this.updateByIdBatchRequester as BatchRequester<unknown, unknown>,
        this.updateByIdAndInsertChildrenIfUpdatedBatchRequester as BatchRequester<unknown, unknown>,
        this.getByParentExecutionIdBatchRequester as BatchRequester<unknown, unknown>,
        this.updateByParentExecutionIdAndIsFinishedBatchRequester as BatchRequester<
          unknown,
          unknown
        >,
      ]
    } else {
      this.batchRequesters = []
    }
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw new DurableExecutionCancelledError('Storage shutdown')
    }
  }

  private checkIfBackgroundProcessesRunning(): boolean {
    return this.isStarted && !this.shutdownSignal.isCancelled()
  }

  private getMergedRecentPerSecondCallsCountsStats(now: number) {
    const currSecond = Math.floor(now / 1000)
    const filteredEntries = [
      ...this.perSecondCallsCountsStats.recentPerSecondCallsCounts.entries(),
    ].filter(([key]) => {
      return key < currSecond
    })
    if (filteredEntries.length === 0) {
      return {
        totalSeconds: this.perSecondCallsCountsStats.totalSeconds,
        totalCallsCount: this.perSecondCallsCountsStats.totalCallsCount,
        meanCallsCount: this.perSecondCallsCountsStats.meanCallsCount,
        maxCallsCount: this.perSecondCallsCountsStats.maxCallsCount,
        recentPerSecondCallsCounts: new Map(
          this.perSecondCallsCountsStats.recentPerSecondCallsCounts,
        ),
      }
    }

    const totalValue = filteredEntries.reduce((acc, [, value]) => acc + value, 0)
    const maxValue = Math.max(...filteredEntries.map(([, value]) => value))
    const prevTotalSeconds = this.perSecondCallsCountsStats.totalSeconds
    const meanCallsCount =
      (this.perSecondCallsCountsStats.meanCallsCount * prevTotalSeconds + totalValue) /
      (prevTotalSeconds + filteredEntries.length)
    return {
      totalSeconds: prevTotalSeconds + filteredEntries.length,
      totalCallsCount: this.perSecondCallsCountsStats.totalCallsCount + totalValue,
      meanCallsCount,
      maxCallsCount: Math.max(this.perSecondCallsCountsStats.maxCallsCount, maxValue),
      recentPerSecondCallsCounts: new Map(
        [...this.perSecondCallsCountsStats.recentPerSecondCallsCounts.entries()].filter(
          ([key]) => key >= currSecond,
        ),
      ),
    }
  }

  private mergeRecentPerSecondCallsCountsStats(now: number) {
    const mergedRecentPerSecondCallsCountsStats = this.getMergedRecentPerSecondCallsCountsStats(now)
    this.perSecondCallsCountsStats.totalSeconds = mergedRecentPerSecondCallsCountsStats.totalSeconds
    this.perSecondCallsCountsStats.totalCallsCount =
      mergedRecentPerSecondCallsCountsStats.totalCallsCount
    this.perSecondCallsCountsStats.meanCallsCount =
      mergedRecentPerSecondCallsCountsStats.meanCallsCount
    this.perSecondCallsCountsStats.maxCallsCount =
      mergedRecentPerSecondCallsCountsStats.maxCallsCount
    this.perSecondCallsCountsStats.recentPerSecondCallsCounts =
      mergedRecentPerSecondCallsCountsStats.recentPerSecondCallsCounts
  }

  /**
   * Run a function with stats.
   *
   * @param key - The key to use for the stats.
   * @param fn - The function to run.
   * @returns The result of the function.
   */
  private async withStats<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
    const now = Date.now()
    const start = performance.now()
    const result = await fn()
    const durationMs = performance.now() - start

    if (!this.callsDurationsStats.has(key)) {
      this.callsDurationsStats.set(key, { callsCount: 0, meanDurationMs: 0 })
    }
    const callsDurationsStat = this.callsDurationsStats.get(key)!
    const prevCallsCount = callsDurationsStat.callsCount
    const prevMeanDurationMs = callsDurationsStat.meanDurationMs
    callsDurationsStat.callsCount++
    callsDurationsStat.meanDurationMs =
      (prevMeanDurationMs * prevCallsCount + durationMs) / (prevCallsCount + 1)

    const currSecond = Math.floor(now / 1000)
    this.perSecondCallsCountsStats.recentPerSecondCallsCounts.set(
      currSecond,
      (this.perSecondCallsCountsStats.recentPerSecondCallsCounts.get(currSecond) ?? 0) + 1,
    )
    this.mergeRecentPerSecondCallsCountsStats(currSecond)
    return result
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
      return await (this.enableStats ? this.withStats(fnName, fn) : fn())
    }

    for (let attempt = 0; ; attempt++) {
      try {
        return await (this.enableStats ? this.withStats(fnName, fn) : fn())
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
        await sleep(Math.min(25 * 2 ** attempt, 1000), { jitterRatio: 0.25 })
      }
    }
  }

  async insertMany(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }

    if (
      !this.insertManyBatchRequester ||
      !this.checkIfBackgroundProcessesRunning() ||
      executions.length >= 3
    ) {
      await this.insertManyBatched([executions])
      return
    }
    await this.insertManyBatchRequester.addRequest(executions)
  }

  private async insertManyBatched(
    requests: ReadonlyArray<ReadonlyArray<TaskExecutionStorageValue>>,
  ): Promise<void> {
    await this.retry('insertMany', () => this.storage.insertMany(requests.flat()))
  }

  async getById(request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
  }): Promise<TaskExecutionStorageValue | undefined> {
    if (!this.getByIdBatchRequester || !this.checkIfBackgroundProcessesRunning()) {
      const storageValues = await this.getManyById([request])
      return storageValues[0]
    }

    return await this.getByIdBatchRequester.addRequest(request)
  }

  private async getManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.retry('getManyById', () => this.storage.getManyById(requests))
  }

  async getBySleepingTaskUniqueId(request: {
    sleepingTaskUniqueId: string
  }): Promise<TaskExecutionStorageValue | undefined> {
    if (
      !this.getBySleepingTaskUniqueIdBatchRequester ||
      !this.checkIfBackgroundProcessesRunning()
    ) {
      const storageValues = await this.getManyBySleepingTaskUniqueId([request])
      return storageValues[0]
    }

    return await this.getBySleepingTaskUniqueIdBatchRequester.addRequest(request)
  }

  private async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{
      sleepingTaskUniqueId: string
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.retry('getManyBySleepingTaskUniqueId', () =>
      this.storage.getManyBySleepingTaskUniqueId(requests),
    )
  }

  async updateById(
    now: number,
    request: {
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdateInternal
    },
    execution?: TaskExecutionStorageValue,
  ): Promise<void> {
    let finalUpdate = request.update
    if (
      request.update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(request.update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      finalUpdate = { ...request.update, closeStatus: 'ready' }
    }

    if (!this.updateByIdBatchRequester || !this.checkIfBackgroundProcessesRunning()) {
      await this.updateManyById([
        {
          ...request,
          update: getTaskExecutionStorageUpdate(now, finalUpdate),
        },
      ])
      return
    }

    await this.updateByIdBatchRequester.addRequest({
      executionId: request.executionId,
      filters: request.filters,
      update: getTaskExecutionStorageUpdate(now, finalUpdate),
    })
  }

  private async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.retry('updateManyById', () => this.storage.updateManyById(requests))
  }

  async updateByIdAndInsertChildrenIfUpdated(
    now: number,
    request: {
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdateInternal
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    },
    execution?: TaskExecutionStorageValue,
  ): Promise<void> {
    if (request.childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(now, request, execution)
    }

    let finalUpdate = request.update
    if (
      request.update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(request.update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      finalUpdate = { ...request.update, closeStatus: 'ready' }
    }

    if (
      !this.updateByIdAndInsertChildrenIfUpdatedBatchRequester ||
      !this.checkIfBackgroundProcessesRunning() ||
      request.childrenTaskExecutionsToInsertIfAnyUpdated.length >= 3
    ) {
      await this.updateManyByIdAndInsertChildrenIfUpdated([
        {
          ...request,
          update: getTaskExecutionStorageUpdate(now, finalUpdate),
        },
      ])
      return
    }

    await this.updateByIdAndInsertChildrenIfUpdatedBatchRequester.addRequest({
      ...request,
      update: getTaskExecutionStorageUpdate(now, finalUpdate),
    })
  }

  private async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    await this.retry('updateManyByIdAndInsertChildrenIfUpdated', () =>
      this.storage.updateManyByIdAndInsertChildrenIfUpdated(requests),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    now: number,
    request: {
      status: TaskExecutionStatus
      startAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      updateExpiresAtWithStartedAt: number
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusAndStartAtLessThanAndReturn',
      () =>
        this.storage.updateByStatusAndStartAtLessThanAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    now: number,
    request: {
      status: TaskExecutionStatus
      onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn',
      () =>
        this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
          {
            ...request,
            update: getTaskExecutionStorageUpdate(now, request.update),
          },
        ),
      maxRetryAttempts,
    )
  }

  async updateByCloseStatusAndReturn(
    now: number,
    request: {
      closeStatus: TaskExecutionCloseStatus
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByCloseStatusAndReturn',
      () =>
        this.storage.updateByCloseStatusAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(
    now: number,
    request: {
      status: TaskExecutionStatus
      isSleepingTask: boolean
      expiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByStatusAndIsSleepingTaskAndExpiresAtLessThan',
      () =>
        this.storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(
    now: number,
    request: {
      onChildrenFinishedProcessingExpiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByOnChildrenFinishedProcessingExpiresAtLessThan',
      () =>
        this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async updateByCloseExpiresAtLessThan(
    now: number,
    request: {
      closeExpiresAtLessThan: number
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateByCloseExpiresAtLessThan',
      () =>
        this.storage.updateByCloseExpiresAtLessThan({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    now: number,
    request: {
      executorId: string
      needsPromiseCancellation: boolean
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.retry(
      'updateByExecutorIdAndNeedsPromiseCancellationAndReturn',
      () =>
        this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  async getByParentExecutionId(
    parentExecutionId: string,
  ): Promise<Array<TaskExecutionStorageValue>> {
    if (!this.getByParentExecutionIdBatchRequester || !this.checkIfBackgroundProcessesRunning()) {
      const storageValues = await this.getManyByParentExecutionId([{ parentExecutionId }])
      return storageValues && storageValues.length > 0 ? storageValues[0]! : []
    }

    return await this.getByParentExecutionIdBatchRequester.addRequest({ parentExecutionId })
  }

  private async getManyByParentExecutionId(
    requests: ReadonlyArray<{
      parentExecutionId: string
    }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    return await this.retry('getManyByParentExecutionId', () =>
      this.storage.getManyByParentExecutionId(requests),
    )
  }

  async updateByParentExecutionIdAndIsFinished(
    now: number,
    request: {
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdateInternal
    },
  ): Promise<void> {
    if (
      !this.updateByParentExecutionIdAndIsFinishedBatchRequester ||
      !this.checkIfBackgroundProcessesRunning()
    ) {
      await this.updateManyByParentExecutionIdAndIsFinished([
        {
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        },
      ])
      return
    }

    return await this.updateByParentExecutionIdAndIsFinishedBatchRequester.addRequest({
      ...request,
      update: getTaskExecutionStorageUpdate(now, request.update),
    })
  }

  private async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.retry('updateManyByParentExecutionIdAndIsFinished', () =>
      this.storage.updateManyByParentExecutionIdAndIsFinished(requests),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
    now: number,
    request: {
      isFinished: boolean
      closeStatus: TaskExecutionCloseStatus
      update: TaskExecutionStorageUpdateInternal
      limit: number
    },
    maxRetryAttempts?: number,
  ): Promise<number> {
    return await this.retry(
      'updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus',
      () =>
        this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
          ...request,
          update: getTaskExecutionStorageUpdate(now, request.update),
        }),
      maxRetryAttempts,
    )
  }

  getCallsDurationsStats(): Map<string, { callsCount: number; meanDurationMs: number }> {
    return new Map(
      [...this.callsDurationsStats.entries()].map(([key, value]) => [
        key,
        { callsCount: value.callsCount, meanDurationMs: value.meanDurationMs },
      ]),
    )
  }

  getPerSecondCallsCountsStats(): {
    totalSeconds: number
    totalCallsCount: number
    meanCallsCount: number
    maxCallsCount: number
  } {
    const perSecondCallsCountsStats = this.getMergedRecentPerSecondCallsCountsStats(
      Date.now() + 5000,
    )
    return {
      totalSeconds: perSecondCallsCountsStats.totalSeconds,
      totalCallsCount: perSecondCallsCountsStats.totalCallsCount,
      meanCallsCount: perSecondCallsCountsStats.meanCallsCount,
      maxCallsCount: perSecondCallsCountsStats.maxCallsCount,
    }
  }

  start(): void {
    this.throwIfShutdown()

    if (!this.enableBatching || this.isStarted) {
      return
    }

    this.isStarted = true
    this.logger.debug('Starting storage background processes')

    this.backgroundProcessesPromisePool.addPromises(
      this.batchRequesters.map((batchRequester) => batchRequester.runBackgroundProcess()),
    )

    this.logger.debug('Started storage background processes')
  }

  async shutdown(): Promise<void> {
    const startTime = Date.now()
    this.logger.debug('Shutting down storage')
    if (!this.shutdownSignal.isCancelled()) {
      this.cancelShutdownSignal()
    }
    this.logger.debug('Storage cancelled')

    if (this.backgroundProcessesPromisePool.size > 0 || this.backgroundPromisePool.size > 0) {
      this.logger.debug('Stopping storage background processes and promises')
      await this.backgroundProcessesPromisePool.allSettled()
      await this.backgroundPromisePool.allSettled()
    }
    this.logger.debug('Stopped storage background processes and promises')

    this.logger.debug('Stopping batch requesters')
    for (const batchRequester of this.batchRequesters) {
      batchRequester.shutdown()
    }
    this.logger.debug('Batch requesters stopped')
    const durationMs = Date.now() - startTime
    this.logger.debug(`Storage shut down in ${(durationMs / 1000).toFixed(2)}s`)
  }
}
