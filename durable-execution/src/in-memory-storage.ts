import { createMutex, type Mutex } from '@gpahal/std/promises'

import { createConsoleLogger, createLoggerWithDebugDisabled, type Logger } from './logger'
import {
  type FinishedChildTaskExecutionStorageValue,
  type Storage,
  type StorageTx,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionStorageWhere,
} from './storage'

/**
 * A storage that stores the task executions in memory. This is useful for testing and for simple
 * use cases. Do not use this for production. It is not persistent.
 *
 * @category Storage
 */
export class InMemoryStorage implements Storage {
  private logger: Logger
  private taskExecutions: Map<string, TaskExecutionStorageValue>
  private finishedChildTaskExecutions: Map<string, FinishedChildTaskExecutionStorageValue>
  private transactionMutex: Mutex

  constructor({ enableDebug = false }: { enableDebug?: boolean } = {}) {
    this.logger = createConsoleLogger('InMemoryStorage')
    if (!enableDebug) {
      this.logger = createLoggerWithDebugDisabled(this.logger)
    }
    this.taskExecutions = new Map()
    this.finishedChildTaskExecutions = new Map()
    this.transactionMutex = createMutex()
  }

  async withTransaction<T>(fn: (tx: InMemoryStorageTx) => T | Promise<T>): Promise<T> {
    await this.transactionMutex.acquire()
    try {
      const tx = new InMemoryStorageTx(
        this.logger,
        this.taskExecutions,
        this.finishedChildTaskExecutions,
      )
      const output = await fn(tx)
      this.taskExecutions = tx.taskExecutions
      this.finishedChildTaskExecutions = tx.finishedChildTaskExecutions
      return output
    } finally {
      this.transactionMutex.release()
    }
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    await this.withTransaction((tx) => {
      tx.insertTaskExecutions(executions)
    })
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction((tx) => {
      return tx.getTaskExecutions(where, limit)
    })
  }

  async updateTaskExecutionsAndReturn(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction((tx) => {
      return tx.updateTaskExecutionsAndReturn(where, update, limit)
    })
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    return await this.withTransaction((tx) => {
      return tx.updateAllTaskExecutions(where, update)
    })
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    return await this.withTransaction((tx) => {
      return tx.insertFinishedChildTaskExecutionIfNotExists(finishedChildTaskExecution)
    })
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    return await this.withTransaction((tx) => {
      return tx.deleteFinishedChildTaskExecutionsAndReturn(limit)
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
  readonly finishedChildTaskExecutions: Map<string, FinishedChildTaskExecutionStorageValue>

  constructor(
    logger: Logger,
    executions: Map<string, TaskExecutionStorageValue>,
    finishedChildTaskExecutions: Map<string, FinishedChildTaskExecutionStorageValue>,
  ) {
    this.logger = logger
    this.taskExecutions = new Map<string, TaskExecutionStorageValue>()
    for (const [key, value] of executions) {
      this.taskExecutions.set(key, structuredClone(value))
    }
    this.finishedChildTaskExecutions = new Map<string, FinishedChildTaskExecutionStorageValue>()
    for (const [key, value] of finishedChildTaskExecutions) {
      this.finishedChildTaskExecutions.set(key, structuredClone(value))
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

  updateTaskExecutionsAndReturn(
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
    const executionsToUpdate = this.getTaskExecutions(where)
    for (const executionToUpdate of executionsToUpdate) {
      updateTaskExecution(executionToUpdate, update)
    }
    return executionsToUpdate.length
  }

  insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): void {
    if (!this.finishedChildTaskExecutions.has(finishedChildTaskExecution.parentExecutionId)) {
      this.finishedChildTaskExecutions.set(
        finishedChildTaskExecution.parentExecutionId,
        finishedChildTaskExecution,
      )
    }
  }

  deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Array<FinishedChildTaskExecutionStorageValue> {
    const finishedChildTaskExecutions = [...this.finishedChildTaskExecutions.values()]
      .sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime())
      .slice(0, limit)
    for (const finishedChildTaskExecution of finishedChildTaskExecutions) {
      this.finishedChildTaskExecutions.delete(finishedChildTaskExecution.parentExecutionId)
    }
    return finishedChildTaskExecutions
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
        execution.needsPromiseCancellation === where.needsPromiseCancellation)
    ) {
      return true
    }
    if (
      where.type === 'by_statuses' &&
      (!where.statuses || where.statuses.includes(execution.status))
    ) {
      return true
    }
    if (
      where.type === 'by_status_and_start_at_less_than' &&
      where.status === execution.status &&
      execution.startAt < where.startAtLessThan
    ) {
      return true
    }
    if (
      where.type === 'by_status_and_expires_at_less_than' &&
      where.status === execution.status &&
      execution.expiresAt &&
      execution.expiresAt < where.expiresAtLessThan
    ) {
      return true
    }
    if (
      where.type === 'by_is_closed' &&
      where.isClosed === execution.isClosed &&
      (!where.statuses || where.statuses.includes(execution.status))
    ) {
      return true
    }
    return false
  })
  filteredTaskExecutions.sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime())
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
    switch (key) {
      case 'unsetError': {
        taskExecution.error = undefined

        break
      }
      case 'unsetExpiresAt': {
        taskExecution.expiresAt = undefined

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
