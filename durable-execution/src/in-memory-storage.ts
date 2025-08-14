import superjson from 'superjson'
import z from 'zod'

import { createMutex, type Mutex } from '@gpahal/std/promises'

import { DurableExecutionError } from './errors'
import { LoggerInternal, zLogger, zLogLevel, type Logger, type LogLevel } from './logger'
import {
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStorageGetByIdsFilters,
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

  private insertTaskExecutionsInternal(executions: Array<TaskExecutionStorageValue>): void {
    for (const execution of executions) {
      this.taskExecutionsMap.set(execution.executionId, execution)
    }
  }

  async insert(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    await this.withMutex(() => {
      this.insertTaskExecutionsInternal(executions)
    })
  }

  private getByIdsInternal(
    executionIds: Array<string>,
  ): Array<TaskExecutionStorageValue | undefined> {
    const taskExecutions: Array<TaskExecutionStorageValue | undefined> = []
    for (const executionId of executionIds) {
      const execution = this.taskExecutionsMap.get(executionId)
      taskExecutions.push(execution)
    }
    return taskExecutions
  }

  private getByIdsWithFiltersAndLimitInternal(
    executionIds: Array<string>,
    filters: TaskExecutionStorageGetByIdsFilters,
    limit?: number,
  ): Array<TaskExecutionStorageValue> {
    if (limit != null && limit <= 0) {
      return []
    }

    const taskExecutions: Array<TaskExecutionStorageValue> = []
    for (const executionId of executionIds) {
      const execution = this.taskExecutionsMap.get(executionId)
      if (execution && (!filters.statuses || filters.statuses.includes(execution.status))) {
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
        filteredTaskExecutions.sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime())
      }
      return filteredTaskExecutions.slice(0, limit)
    }
    return filteredTaskExecutions
  }

  private updateTaskExecutionInternal(
    taskExecution: TaskExecutionStorageValue,
    update: TaskExecutionStorageUpdate,
  ) {
    for (const key in update) {
      switch (key) {
        case 'unsetRunOutput': {
          if (update.unsetRunOutput) {
            taskExecution.runOutput = undefined
          }

          break
        }
        case 'unsetError': {
          if (update.unsetError) {
            taskExecution.error = undefined
          }

          break
        }
        case 'unsetExpiresAt': {
          if (update.unsetExpiresAt) {
            taskExecution.expiresAt = undefined
          }

          break
        }
        case 'decrementParentActiveChildrenCount': {
          if (update.decrementParentActiveChildrenCount && taskExecution.parent) {
            const parentTaskExecution = this.taskExecutionsMap.get(taskExecution.parent.executionId)
            if (parentTaskExecution) {
              parentTaskExecution.activeChildrenCount--
            }
          }

          break
        }
        case 'unsetOnChildrenFinishedProcessingExpiresAt': {
          if (update.unsetOnChildrenFinishedProcessingExpiresAt) {
            taskExecution.onChildrenFinishedProcessingExpiresAt = undefined
          }

          break
        }
        case 'unsetCloseExpiresAt': {
          if (update.unsetCloseExpiresAt) {
            taskExecution.closeExpiresAt = undefined
          }

          break
        }
        default: {
          // @ts-expect-error - This is safe because we know the key is valid
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          taskExecution[key] = update[key]
        }
      }
    }
  }

  private updateTaskExecutionsInternal(
    taskExecutions: Array<TaskExecutionStorageValue>,
    update: TaskExecutionStorageUpdate,
  ) {
    for (const taskExecution of taskExecutions) {
      this.updateTaskExecutionInternal(taskExecution, update)
    }
  }

  async getByIds(
    executionIds: Array<string>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    return await this.withMutex(() => {
      return this.getByIdsInternal(executionIds)
    })
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal([executionId], filters, 1)
      return taskExecutions.length > 0 ? taskExecutions[0] : undefined
    })
  }

  async updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal([executionId], filters, 1)
      this.updateTaskExecutionsInternal(taskExecutions, update)
    })
  }

  async updateByIdAndInsertIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal([executionId], filters)
      this.updateTaskExecutionsInternal(taskExecutions, update)
      if (executionsToInsertIfAnyUpdated.length > 0) {
        this.insertTaskExecutionsInternal(executionsToInsertIfAnyUpdated)
      }
    })
  }

  async updateByIdsAndStatuses(
    executionIds: Array<string>,
    statuses: Array<TaskExecutionStatus>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByIdsWithFiltersAndLimitInternal(executionIds, { statuses })
      this.updateTaskExecutionsInternal(taskExecutions, update)
    })
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) => execution.status === status && execution.startAt < startAtLessThan,
        limit,
        (a, b) => a.startAt.getTime() - b.startAt.getTime(),
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.status === status &&
          execution.onChildrenFinishedProcessingStatus === onChildrenFinishedProcessingStatus &&
          execution.activeChildrenCount < activeChildrenCountLessThan,
        limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async updateByStatusesAndCloseStatusAndReturn(
    statuses: Array<TaskExecutionStatus>,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) => statuses.includes(execution.status) && execution.closeStatus === closeStatus,
        limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async updateByExpiresAtLessThanAndReturn(
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) => execution.expiresAt != null && execution.expiresAt < expiresAtLessThan,
        limit,
        (a, b) => a.expiresAt!.getTime() - b.expiresAt!.getTime(),
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.onChildrenFinishedProcessingExpiresAt != null &&
          execution.onChildrenFinishedProcessingExpiresAt <
            onChildrenFinishedProcessingExpiresAtLessThan,
        limit,
        (a, b) =>
          a.onChildrenFinishedProcessingExpiresAt!.getTime() -
          b.onChildrenFinishedProcessingExpiresAt!.getTime(),
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async updateByCloseExpiresAtLessThanAndReturn(
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.closeExpiresAt != null && execution.closeExpiresAt < closeExpiresAtLessThan,
        limit,
        (a, b) => a.closeExpiresAt!.getTime() - b.closeExpiresAt!.getTime(),
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
      return taskExecutions
    })
  }

  async getByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withMutex(() => {
      return this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.needsPromiseCancellation === needsPromiseCancellation &&
          executionIds.includes(execution.executionId),
        limit,
      )
    })
  }

  async updateByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    return await this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.needsPromiseCancellation === needsPromiseCancellation &&
          executionIds.includes(execution.executionId),
      )
      this.updateTaskExecutionsInternal(taskExecutions, update)
    })
  }

  async deleteById(executionId: string): Promise<void> {
    return await this.withMutex(() => {
      this.taskExecutionsMap.delete(executionId)
    })
  }
}
