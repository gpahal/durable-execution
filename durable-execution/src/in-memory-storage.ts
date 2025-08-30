import superjson from 'superjson'
import z from 'zod'

import { createMutex, type Mutex } from '@gpahal/std/promises'

import { DurableExecutionError } from './errors'
import { LoggerInternal, zLogger, zLogLevel, type Logger, type LogLevel } from './logger'
import {
  applyTaskExecutionStorageUpdate,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from './storage'
import type { TaskExecutionStatus } from './task'

const zInMemoryTaskExecutionsStorageOptions = z.object({
  logger: zLogger.nullish(),
  logLevel: zLogLevel.nullish(),
})

/**
 * In-memory implementation of TaskExecutionsStorage for development and testing.
 *
 * ⚠️ **WARNING**: This storage is NOT suitable for production use because:
 * - Data is lost when the process restarts
 * - No persistence across application restarts
 * - No sharing between multiple processes
 * - Memory usage grows with task history
 *
 * @category Storage
 */
export class InMemoryTaskExecutionsStorage implements TaskExecutionsStorage {
  private logger: LoggerInternal
  private taskExecutionsMap: Map<string, TaskExecutionStorageValue>
  private sleepingTaskExecutionsMap: Map<string, string>
  private mutex: Mutex

  /**
   * Create an in-memory task executions storage.
   *
   * @param options - The options for the in-memory task executions storage.
   * @param options.logger - The logger to use for the in-memory task executions storage. If not provided, a
   *   default logger will be used.
   * @param options.logLevel - The log level to use for the in-memory task executions storage. If not provided,
   *   the default log level will be used.
   */
  constructor(options: { logger?: Logger; logLevel?: LogLevel } = {}) {
    const parsedOptions = zInMemoryTaskExecutionsStorageOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    const { logger, logLevel } = parsedOptions.data

    this.logger = new LoggerInternal(logger, logLevel)
    this.taskExecutionsMap = new Map()
    this.sleepingTaskExecutionsMap = new Map()
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

  async save(saveFn: (s: string) => Promise<void>): Promise<void> {
    await saveFn(
      await this.withMutex(() => {
        return superjson.stringify(this.taskExecutionsMap)
      }),
    )
  }

  async load(loadFn: () => Promise<string>): Promise<void> {
    let taskExecutionsMap: Map<string, TaskExecutionStorageValue>
    try {
      const data = await loadFn()
      if (!data.trim()) {
        taskExecutionsMap = new Map()
        return
      } else {
        taskExecutionsMap = superjson.parse<Map<string, TaskExecutionStorageValue>>(data)
      }
    } catch {
      taskExecutionsMap = new Map()
    }

    await this.withMutex(() => {
      this.taskExecutionsMap = taskExecutionsMap
    })
  }

  async logAllTaskExecutions(): Promise<void> {
    this.logger.info('------\n\nAll task executions:')
    await this.withMutex(() => {
      for (const execution of this.taskExecutionsMap.values()) {
        this.logger.info(
          `Task execution: ${execution.executionId}\nJSON: ${JSON.stringify(execution, null, 2)}\n\n`,
        )
      }
    })
    this.logger.info('------')
  }

  private insertTaskExecutionsInternal(executions: ReadonlyArray<TaskExecutionStorageValue>): void {
    for (const execution of executions) {
      if (this.taskExecutionsMap.has(execution.executionId)) {
        throw new Error(`Execution ${execution.executionId} already exists`)
      }
      if (
        execution.sleepingTaskUniqueId != null &&
        this.sleepingTaskExecutionsMap.has(execution.sleepingTaskUniqueId)
      ) {
        throw new Error(`Execution ${execution.sleepingTaskUniqueId} already exists`)
      }
      this.taskExecutionsMap.set(execution.executionId, execution)
      if (execution.sleepingTaskUniqueId != null) {
        this.sleepingTaskExecutionsMap.set(execution.sleepingTaskUniqueId, execution.executionId)
      }
    }
  }

  private getByIdsWithFiltersAndLimitInternal(
    executionIds: ReadonlyArray<string>,
    filters?: TaskExecutionStorageGetByIdFilters,
    limit?: number,
  ): Array<TaskExecutionStorageValue> {
    if (limit != null && limit <= 0) {
      return []
    }

    const taskExecutions: Array<TaskExecutionStorageValue> = []
    for (const executionId of executionIds) {
      const execution = this.taskExecutionsMap.get(executionId)
      if (
        execution &&
        (filters?.isSleepingTask == null || filters.isSleepingTask === execution.isSleepingTask) &&
        (filters?.status == null || filters.status === execution.status) &&
        (filters?.isFinished == null || filters.isFinished === execution.isFinished)
      ) {
        taskExecutions.push(execution)
        if (limit != null && taskExecutions.length >= limit) {
          break
        }
      }
    }
    return taskExecutions
  }

  private getByFilterFnAndLimitInternal(
    filterFn: (execution: TaskExecutionStorageValue) => boolean,
    limit?: number,
    sortFn?: (a: TaskExecutionStorageValue, b: TaskExecutionStorageValue) => number,
  ): Array<TaskExecutionStorageValue> {
    if (limit != null && limit <= 0) {
      return []
    }

    const filteredTaskExecutions: Array<TaskExecutionStorageValue> = []
    for (const execution of this.taskExecutionsMap.values()) {
      if (filterFn(execution)) {
        filteredTaskExecutions.push(execution)
      }
    }

    if (limit != null) {
      if (sortFn != null) {
        filteredTaskExecutions.sort(sortFn)
      } else {
        filteredTaskExecutions.sort((a, b) => a.updatedAt - b.updatedAt)
      }
      return filteredTaskExecutions.slice(0, limit)
    }
    return filteredTaskExecutions
  }

  private updateTaskExecutionsInternal(
    taskExecutions: Array<TaskExecutionStorageValue>,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt?: number,
  ) {
    for (const taskExecution of taskExecutions) {
      applyTaskExecutionStorageUpdate(taskExecution, update, updateExpiresAtWithStartedAt)
    }
  }

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    await this.withMutex(() => {
      this.insertTaskExecutionsInternal(executions)
    })
  }

  private async getById(
    executionId: string,
    filters?: TaskExecutionStorageGetByIdFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal([executionId], filters, 1)
      return taskExecutions.length > 0 ? taskExecutions[0] : undefined
    })
  }

  async getManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await Promise.all(
      requests.map((request) => this.getById(request.executionId, request.filters)),
    )
  }

  private async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) => execution.sleepingTaskUniqueId === sleepingTaskUniqueId,
        1,
      )
      return taskExecutions.length > 0 ? taskExecutions[0] : undefined
    })
  }

  async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{
      sleepingTaskUniqueId: string
    }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await Promise.all(
      requests.map((request) => this.getBySleepingTaskUniqueId(request.sleepingTaskUniqueId)),
    )
  }

  private async updateById(request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
  }): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal(
        [request.executionId],
        request.filters,
        1,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
    })
  }

  async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await Promise.all(requests.map((request) => this.updateById(request)))
  }

  private async updateByIdAndInsertChildrenIfUpdated(request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
    childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
  }): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal(
        [request.executionId],
        request.filters,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      if (
        taskExecutions.length > 0 &&
        request.childrenTaskExecutionsToInsertIfAnyUpdated.length > 0
      ) {
        this.insertTaskExecutionsInternal(request.childrenTaskExecutionsToInsertIfAnyUpdated)
      }
    })
  }

  async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    await Promise.all(requests.map((request) => this.updateByIdAndInsertChildrenIfUpdated(request)))
  }

  async updateByStatusAndStartAtLessThanAndReturn(request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.status === request.status && execution.startAt < request.startAtLessThan,
        request.limit,
        (a, b) => a.startAt - b.startAt,
      )
      this.updateTaskExecutionsInternal(
        taskExecutions,
        request.update,
        request.updateExpiresAtWithStartedAt,
      )
      return taskExecutions
    })
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(request: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.status === request.status &&
          execution.onChildrenFinishedProcessingStatus ===
            request.onChildrenFinishedProcessingStatus &&
          execution.activeChildrenCount === 0,
        request.limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions
    })
  }

  async updateByCloseStatusAndReturn(request: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) => execution.closeStatus === request.closeStatus,
        request.limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions
    })
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(request: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.status === request.status &&
          execution.isSleepingTask === request.isSleepingTask &&
          execution.expiresAt != null &&
          execution.expiresAt < request.expiresAtLessThan,
        request.limit,
        (a, b) => a.expiresAt! - b.expiresAt!,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions.length
    })
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(request: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.onChildrenFinishedProcessingExpiresAt != null &&
          execution.onChildrenFinishedProcessingExpiresAt <
            request.onChildrenFinishedProcessingExpiresAtLessThan,
        request.limit,
        (a, b) =>
          a.onChildrenFinishedProcessingExpiresAt! - b.onChildrenFinishedProcessingExpiresAt!,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions.length
    })
  }

  async updateByCloseExpiresAtLessThan(request: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.closeExpiresAt != null &&
          execution.closeExpiresAt < request.closeExpiresAtLessThan,
        request.limit,
        (a, b) => a.closeExpiresAt! - b.closeExpiresAt!,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions.length
    })
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(request: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.executorId === request.executorId &&
          execution.needsPromiseCancellation === request.needsPromiseCancellation,
        request.limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      return taskExecutions
    })
  }

  private async getByParentExecutionId(request: {
    parentExecutionId: string
  }): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      return this.getByFilterFnAndLimitInternal(
        (execution) => execution.parent?.executionId === request.parentExecutionId,
      )
    })
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{ parentExecutionId: string }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    return await Promise.all(requests.map((request) => this.getByParentExecutionId(request)))
  }

  private async updateByParentExecutionIdAndIsFinished(request: {
    parentExecutionId: string
    isFinished: boolean
    update: TaskExecutionStorageUpdate
  }): Promise<void> {
    await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.parent?.executionId === request.parentExecutionId &&
          execution.isFinished === request.isFinished,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
    })
  }

  async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await Promise.all(
      requests.map((request) => this.updateByParentExecutionIdAndIsFinished(request)),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.isFinished === request.isFinished &&
          execution.closeStatus === request.closeStatus,
        request.limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      for (const execution of taskExecutions) {
        if (execution.parent != null) {
          const parentExecution = this.taskExecutionsMap.get(execution.parent.executionId)
          if (parentExecution != null) {
            parentExecution.activeChildrenCount -= 1
          }
        }
      }
      return taskExecutions.length
    })
  }

  async deleteById(request: { executionId: string }): Promise<void> {
    return await this.withMutex(() => {
      const taskExecution = this.taskExecutionsMap.get(request.executionId)
      if (taskExecution != null) {
        this.taskExecutionsMap.delete(request.executionId)
        if (taskExecution.sleepingTaskUniqueId != null) {
          this.sleepingTaskExecutionsMap.delete(taskExecution.sleepingTaskUniqueId)
        }
      }
    })
  }

  async deleteAll(): Promise<void> {
    return await this.withMutex(() => {
      this.taskExecutionsMap.clear()
      this.sleepingTaskExecutionsMap.clear()
    })
  }
}
