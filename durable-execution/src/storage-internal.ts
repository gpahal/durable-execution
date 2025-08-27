import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
import { getErrorMessage } from '@gpahal/std/errors'
import { omitUndefinedValues } from '@gpahal/std/objects'
import { sleep, sleepWithJitter } from '@gpahal/std/promises'

import { DurableExecutionCancelledError, DurableExecutionError } from './errors'
import type { LoggerInternal } from './logger'
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
    requests: Array<T>,
  ) => Array<R> | null | undefined | void | Promise<Array<R> | null | undefined | void>
  private readonly shutdownSignal: CancelSignal
  private readonly addBackgroundPromise: (promise: Promise<void>) => void

  private pendingRequests: Array<BatchRequest<T, R>>

  constructor(
    logger: LoggerInternal,
    processName: string,
    batchSize: number,
    backgroundProcessIntraBatchSleepMs: number,
    singleRequestsBatchProcessFn: (
      requests: Array<T>,
    ) => Array<R> | null | undefined | void | Promise<Array<R> | null | undefined | void>,
    shutdownSignal: CancelSignal,
    addBackgroundPromise: (promise: Promise<void>) => void,
  ) {
    this.logger = logger
    this.processName = processName
    this.batchSize = batchSize
    this.backgroundProcessIntraBatchSleepMs = backgroundProcessIntraBatchSleepMs
    this.singleRequestsBatchProcessFn = singleRequestsBatchProcessFn
    this.shutdownSignal = shutdownSignal
    this.addBackgroundPromise = addBackgroundPromise
    this.pendingRequests = []
  }

  shutdown(): void {
    for (const request of this.pendingRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }
    this.pendingRequests = []
  }

  async runBackgroundProcess(): Promise<void> {
    let consecutiveErrors = 0
    const maxConsecutiveErrors = 10

    const originalBackgroundProcessIntraBatchSleepMs = this.backgroundProcessIntraBatchSleepMs
    let backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs

    const runBackgroundProcessSingleBatch = async (
      requests: Array<BatchRequest<T, R>>,
    ): Promise<void> => {
      try {
        const results = await this.singleRequestsBatchProcessFn(
          requests.map((request) => request.data),
        )
        if (results == null) {
          for (const request of requests) {
            request.resolve(undefined as R)
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

        if (consecutiveErrors >= maxConsecutiveErrors) {
          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.125,
            backgroundProcessIntraBatchSleepMs * 2.5,
          )
        }
      }
    }

    await sleepWithJitter(backgroundProcessIntraBatchSleepMs)
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
          await sleep(backgroundProcessIntraBatchSleepMs)
          skippedBatchCount++
          continue
        } else {
          backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs
          if (skippedBatchCount < 5 && requests.length < this.batchSize / 3) {
            await sleep(originalBackgroundProcessIntraBatchSleepMs)
            skippedBatchCount++
          } else {
            skippedBatchCount = 0
          }
        }

        for (let i = 0; i < requests.length; i += this.batchSize) {
          this.addBackgroundPromise(
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
    })
    return promise
  }
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
  private readonly enableBatching: boolean
  private readonly baseBackgroundProcessIntraBatchSleepMs: number
  private readonly maxRetryAttempts: number
  private readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private backgroundProcessesPromises: Set<Promise<void>>
  private backgroundPromises: Set<Promise<void>>
  readonly timingStats: Map<string, { count: number; meanMs: number }>
  readonly perSecondStorageCallCounts: Map<number, number>
  private readonly insertManyBatchRequester:
    | BatchRequester<Array<TaskExecutionStorageValue>, void>
    | undefined
  private readonly getByIdBatchRequester:
    | BatchRequester<
        { executionId: string; filters: TaskExecutionStorageGetByIdFilters },
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
          filters: TaskExecutionStorageGetByIdFilters
          update: TaskExecutionStorageUpdate
        },
        void
      >
    | undefined
  private readonly updateByIdAndInsertChildrenIfUpdatedBatchRequester:
    | BatchRequester<
        {
          executionId: string
          filters: TaskExecutionStorageGetByIdFilters
          update: TaskExecutionStorageUpdate
          childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>
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
  private readonly batchRequesters: Array<BatchRequester<unknown, unknown>>

  constructor(
    logger: LoggerInternal,
    storage: TaskExecutionsStorage,
    enableBatching: boolean,
    baseBackgroundProcessIntraBatchSleepMs: number,
    maxRetryAttempts?: number,
  ) {
    this.logger = logger
    this.storage = storage
    this.enableBatching = enableBatching
    this.baseBackgroundProcessIntraBatchSleepMs = baseBackgroundProcessIntraBatchSleepMs
    this.maxRetryAttempts = validateStorageMaxRetryAttempts(maxRetryAttempts)

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.backgroundProcessesPromises = new Set()
    this.backgroundPromises = new Set()
    this.timingStats = new Map()
    this.perSecondStorageCallCounts = new Map()

    if (this.enableBatching) {
      const addBackgroundPromise = (promise: Promise<void>) => {
        this.addBackgroundPromise(promise)
      }

      this.insertManyBatchRequester = new BatchRequester(
        logger,
        'insertMany',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.insertManyBatched(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.getByIdBatchRequester = new BatchRequester(
        logger,
        'getById',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.getManyById(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.getBySleepingTaskUniqueIdBatchRequester = new BatchRequester(
        logger,
        'getBySleepingTaskUniqueId',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.getManyBySleepingTaskUniqueId(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.updateByIdBatchRequester = new BatchRequester(
        logger,
        'updateById',
        100,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.updateManyById(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.updateByIdAndInsertChildrenIfUpdatedBatchRequester = new BatchRequester(
        logger,
        'updateByIdAndInsertChildrenIfUpdated',
        50,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.updateManyByIdAndInsertChildrenIfUpdated(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.getByParentExecutionIdBatchRequester = new BatchRequester(
        logger,
        'getByParentExecutionId',
        3,
        this.baseBackgroundProcessIntraBatchSleepMs,
        (requests) => this.getManyByParentExecutionId(requests),
        this.shutdownSignal,
        addBackgroundPromise,
      )
      this.updateByParentExecutionIdAndIsFinishedBatchRequester = new BatchRequester(
        logger,
        'updateByParentExecutionIdAndIsFinished',
        5,
        this.baseBackgroundProcessIntraBatchSleepMs * 2,
        (requests) => this.updateManyByParentExecutionIdAndIsFinished(requests),
        this.shutdownSignal,
        addBackgroundPromise,
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
      throw DurableExecutionCancelledError
    }
  }

  private addBackgroundProcessesPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundProcessesPromises.add(
      wrappedPromise().finally(() => this.backgroundProcessesPromises.delete(promise)),
    )
  }

  private addBackgroundPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundPromises.add(
      wrappedPromise().finally(() => this.backgroundPromises.delete(promise)),
    )
  }

  /**
   * Run a function with timing stats.
   *
   * @param key - The key to use for the timing stats.
   * @param fn - The function to run.
   * @returns The result of the function.
   */
  private async withTimingStats<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
    const start = performance.now()
    const result = await fn()
    const durationMs = performance.now() - start

    if (!this.timingStats.has(key)) {
      this.timingStats.set(key, { count: 0, meanMs: 0 })
    }
    const timingStat = this.timingStats.get(key)!
    timingStat.count++
    timingStat.meanMs = (timingStat.meanMs * (timingStat.count - 1) + durationMs) / timingStat.count
    return result
  }

  private addStorageCalls(count = 1): void {
    const second = Math.floor(Date.now() / 1000)
    this.perSecondStorageCallCounts.set(
      second,
      (this.perSecondStorageCallCounts.get(second) ?? 0) + count,
    )
  }

  startBackgroundProcesses(): void {
    this.throwIfShutdown()

    if (!this.enableBatching) {
      return
    }

    this.logger.info('Starting storage background processes')
    this.addBackgroundProcessesPromise(
      this.startBackgroundProcessesInternal().catch((error) => {
        console.error('Storage background processes exited with error', error)
      }),
    )
    this.logger.info('Started storage background processes')
  }

  private async startBackgroundProcessesInternal(): Promise<void> {
    await Promise.all(
      this.batchRequesters.map((batchRequester) => batchRequester.runBackgroundProcess()),
    )
  }

  async shutdown(): Promise<void> {
    const startTime = Date.now()
    this.logger.debug('Shutting down storage')
    if (!this.shutdownSignal.isCancelled()) {
      this.cancelShutdownSignal()
    }
    this.logger.debug('Storage cancelled')

    if (this.backgroundProcessesPromises.size > 0 || this.backgroundPromises.size > 0) {
      this.logger.debug('Stopping storage background processes and promises')
      await Promise.all(this.backgroundProcessesPromises)
      this.backgroundProcessesPromises.clear()
      await Promise.all(this.backgroundPromises)
      this.backgroundPromises.clear()
    }
    this.logger.debug('Storage background processes and promises stopped')

    this.logger.debug('Stopping batch requesters')
    for (const batchRequester of this.batchRequesters) {
      batchRequester.shutdown()
    }
    this.logger.debug('Batch requesters stopped')
    const durationMs = Date.now() - startTime
    this.logger.debug(`Storage shut down in ${(durationMs / 1000).toFixed(2)}s`)
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
      this.addStorageCalls()
      return await this.withTimingStats(fnName, fn)
    }

    for (let attempt = 0; ; attempt++) {
      try {
        this.addStorageCalls()
        return await this.withTimingStats(fnName, fn)
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

  async insertMany(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }

    if (
      executions.length >= 3 ||
      !this.insertManyBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      await this.insertManyBatched([executions])
      return
    }
    await this.insertManyBatchRequester.addRequest(executions)
  }

  private async insertManyBatched(
    requests: Array<Array<TaskExecutionStorageValue>>,
  ): Promise<void> {
    await this.retry('insertMany', () => this.storage.insertMany(requests.flat()))
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    if (
      !this.getByIdBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      const storageValues = await this.getManyById([{ executionId, filters }])
      return storageValues[0]
    }

    return await this.getByIdBatchRequester.addRequest({ executionId, filters })
  }

  private async getManyById(
    requests: Array<{
      executionId: string
      filters: TaskExecutionStorageGetByIdFilters
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.retry('getManyById', () => this.storage.getManyById(requests))
  }

  async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    if (
      !this.getBySleepingTaskUniqueIdBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      const storageValues = await this.getManyBySleepingTaskUniqueId([{ sleepingTaskUniqueId }])
      return storageValues[0]
    }

    return await this.getBySleepingTaskUniqueIdBatchRequester.addRequest({ sleepingTaskUniqueId })
  }

  private async getManyBySleepingTaskUniqueId(
    requests: Array<{
      sleepingTaskUniqueId: string
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.retry('getManyBySleepingTaskUniqueId', () =>
      this.storage.getManyBySleepingTaskUniqueId(requests),
    )
  }

  async updateById(
    now: number,
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdateInternal,
    execution?: TaskExecutionStorageValue,
  ): Promise<void> {
    if (
      !this.updateByIdBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      await this.updateManyById([
        {
          executionId,
          filters,
          update: getTaskExecutionStorageUpdate(now, update),
        },
      ])
      return
    }

    if (
      update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      update.closeStatus = 'ready'
    }

    await this.updateByIdBatchRequester.addRequest({
      executionId,
      filters,
      update: getTaskExecutionStorageUpdate(now, update),
    })
  }

  private async updateManyById(
    requests: Array<{
      executionId: string
      filters: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.retry('updateManyById', () => this.storage.updateManyById(requests))
  }

  async updateByIdAndInsertChildrenIfUpdated(
    now: number,
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdateInternal,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
    execution?: TaskExecutionStorageValue,
  ): Promise<void> {
    if (childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(now, executionId, filters, update, execution)
    }

    if (
      !this.updateByIdAndInsertChildrenIfUpdatedBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      await this.updateManyByIdAndInsertChildrenIfUpdated([
        {
          executionId,
          filters,
          update: getTaskExecutionStorageUpdate(now, update),
          childrenTaskExecutionsToInsertIfAnyUpdated,
        },
      ])
      return
    }

    if (
      update.status &&
      FINISHED_TASK_EXECUTION_STATUSES.includes(update.status) &&
      execution?.parent?.isFinalizeOfParent
    ) {
      update.closeStatus = 'ready'
    }

    if (childrenTaskExecutionsToInsertIfAnyUpdated.length >= 3) {
      await this.updateManyByIdAndInsertChildrenIfUpdated([
        {
          executionId,
          filters,
          update: getTaskExecutionStorageUpdate(now, update),
          childrenTaskExecutionsToInsertIfAnyUpdated,
        },
      ])
      return
    }

    await this.updateByIdAndInsertChildrenIfUpdatedBatchRequester.addRequest({
      executionId,
      filters,
      update: getTaskExecutionStorageUpdate(now, update),
      childrenTaskExecutionsToInsertIfAnyUpdated,
    })
  }

  private async updateManyByIdAndInsertChildrenIfUpdated(
    requests: Array<{
      executionId: string
      filters: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    await this.retry('updateManyByIdAndInsertChildrenIfUpdated', () =>
      this.storage.updateManyByIdAndInsertChildrenIfUpdated(requests),
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
  ): Promise<Array<TaskExecutionStorageValue>> {
    if (
      !this.getByParentExecutionIdBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      const storageValues = await this.getManyByParentExecutionId([{ parentExecutionId }])
      return storageValues && storageValues.length > 0 ? storageValues[0]! : []
    }

    return await this.getByParentExecutionIdBatchRequester.addRequest({ parentExecutionId })
  }

  private async getManyByParentExecutionId(
    requests: Array<{
      parentExecutionId: string
    }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    return await this.retry('getManyByParentExecutionId', () =>
      this.storage.getManyByParentExecutionId(requests),
    )
  }

  async updateByParentExecutionIdAndIsFinished(
    now: number,
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdateInternal,
  ): Promise<void> {
    if (
      !this.updateByParentExecutionIdAndIsFinishedBatchRequester ||
      this.backgroundProcessesPromises.size === 0 ||
      this.shutdownSignal.isCancelled()
    ) {
      await this.updateManyByParentExecutionIdAndIsFinished([
        { parentExecutionId, isFinished, update: getTaskExecutionStorageUpdate(now, update) },
      ])
      return
    }

    return await this.updateByParentExecutionIdAndIsFinishedBatchRequester.addRequest({
      parentExecutionId,
      isFinished,
      update: getTaskExecutionStorageUpdate(now, update),
    })
  }

  private async updateManyByParentExecutionIdAndIsFinished(
    requests: Array<{
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
