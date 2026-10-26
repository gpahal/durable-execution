import superjson from 'superjson'

import { createMutex } from '@gpahal/std/promises'
import type { Mutex } from '@gpahal/std/promises'

import { applyTaskExecutionStorageUpdate } from './storage'
import type {
  TaskExecutionCloseStatus,
  TaskExecutionOnChildrenFinishedProcessingStatus,
  TaskExecutionsStorage,
  TaskExecutionStorageGetByIdFilters,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
} from './storage'
import type { TaskExecutionStatus } from './task'

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
  private taskExecutionsMap: Map<string, TaskExecutionStorageValue>
  private readonly sleepingTaskExecutionsMap: Map<string, string>
  private readonly mutex: Mutex

  /**
   * Create an in-memory task executions storage.
   */
  constructor() {
    this.taskExecutionsMap = new Map()
    this.sleepingTaskExecutionsMap = new Map()
    this.mutex = createMutex()
  }

  async withMutex<T>(fn: () => T | Promise<T>): Promise<T> {
    return this.mutex.runExclusive(fn)
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
      }
      taskExecutionsMap = superjson.parse<Map<string, TaskExecutionStorageValue>>(data)
    } catch {
      taskExecutionsMap = new Map()
    }

    await this.withMutex(() => {
      this.taskExecutionsMap = taskExecutionsMap
    })
  }

  /* eslint-disable no-console -- this debugging method exists to print the task executions */
  async logAllTaskExecutions(): Promise<void> {
    console.log('------\n\nAll task executions:')
    await this.withMutex(() => {
      for (const execution of this.taskExecutionsMap.values()) {
        console.log(
          `Task execution: ${execution.executionId}\nJSON: ${JSON.stringify(execution, null, 2)}\n\n`,
        )
      }
    })
    console.log('------')
  }
  /* eslint-enable no-console */

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
      if (!(
        execution &&
        (filters?.isSleepingTask == null || filters.isSleepingTask === execution.isSleepingTask) &&
        (filters?.status == null || filters.status === execution.status) &&
        (filters?.isFinished == null || filters.isFinished === execution.isFinished)
      )) {
        continue
      }

      taskExecutions.push(execution)
      if (limit != null && taskExecutions.length >= limit) {
        break
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
      return filteredTaskExecutions
        .toSorted(sortFn ?? ((a, b) => a.updatedAt - b.updatedAt))
        .slice(0, limit)
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
    return this.withMutex(() => {
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
    return Promise.all(
      requests.map(async (request) => this.getById(request.executionId, request.filters)),
    )
  }

  private async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    return this.withMutex(() => {
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
    return Promise.all(
      requests.map(async (request) => this.getBySleepingTaskUniqueId(request.sleepingTaskUniqueId)),
    )
  }

  private async updateById(request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
  }): Promise<void> {
    await this.withMutex(() => {
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
    await Promise.all(requests.map(async (request) => this.updateById(request)))
  }

  private async updateByIdAndInsertChildrenIfUpdated(request: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
    childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
  }): Promise<void> {
    await this.withMutex(() => {
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
    await Promise.all(
      requests.map(async (request) => this.updateByIdAndInsertChildrenIfUpdated(request)),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(request: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    return this.withMutex(() => {
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
    return this.withMutex(() => {
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
    return this.withMutex(() => {
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
    return this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.status === request.status &&
          execution.isSleepingTask === request.isSleepingTask &&
          execution.expiresAt != null &&
          execution.expiresAt < request.expiresAtLessThan,
        request.limit,
        (a, b) => (a.expiresAt ?? 0) - (b.expiresAt ?? 0),
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
    return this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.onChildrenFinishedProcessingExpiresAt != null &&
          execution.onChildrenFinishedProcessingExpiresAt <
            request.onChildrenFinishedProcessingExpiresAtLessThan,
        request.limit,
        (a, b) =>
          (a.onChildrenFinishedProcessingExpiresAt ?? 0) -
          (b.onChildrenFinishedProcessingExpiresAt ?? 0),
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
    return this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.closeExpiresAt != null &&
          execution.closeExpiresAt < request.closeExpiresAtLessThan,
        request.limit,
        (a, b) => (a.closeExpiresAt ?? 0) - (b.closeExpiresAt ?? 0),
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
    return this.withMutex(() => {
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
    return this.withMutex(() => {
      return this.getByFilterFnAndLimitInternal(
        (execution) => execution.parent?.executionId === request.parentExecutionId,
      )
    })
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{ parentExecutionId: string }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    return Promise.all(requests.map(async (request) => this.getByParentExecutionId(request)))
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
      requests.map(async (request) => this.updateByParentExecutionIdAndIsFinished(request)),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(request: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return this.withMutex(() => {
      const taskExecutions = this.getByFilterFnAndLimitInternal(
        (execution) =>
          execution.isFinished === request.isFinished &&
          execution.closeStatus === request.closeStatus,
        request.limit,
      )
      this.updateTaskExecutionsInternal(taskExecutions, request.update)
      for (const execution of taskExecutions) {
        if (execution.parent == null) {
          continue
        }

        const parentExecution = this.taskExecutionsMap.get(execution.parent.executionId)
        if (parentExecution != null) {
          parentExecution.activeChildrenCount -= 1
        }
      }
      return taskExecutions.length
    })
  }

  async deleteById(request: { executionId: string }): Promise<void> {
    await this.withMutex(() => {
      const taskExecution = this.taskExecutionsMap.get(request.executionId)
      if (taskExecution == null) {
        return
      }

      this.taskExecutionsMap.delete(request.executionId)
      if (taskExecution.sleepingTaskUniqueId != null) {
        this.sleepingTaskExecutionsMap.delete(taskExecution.sleepingTaskUniqueId)
      }
    })
  }

  async deleteAll(): Promise<void> {
    await this.withMutex(() => {
      this.taskExecutionsMap.clear()
      this.sleepingTaskExecutionsMap.clear()
    })
  }
}
