import { readFile, writeFile } from 'node:fs/promises'

import {
  createConsoleLogger,
  type DurableStorage,
  type DurableStorageTx,
  type DurableTaskExecutionStorageObject,
  type DurableTaskExecutionStorageObjectUpdate,
  type DurableTaskExecutionStorageWhere,
  type Logger,
} from '../src'
import { createLoggerDebugDisabled } from '../src/logger'

export class InMemoryStorage implements DurableStorage {
  private logger: Logger
  private taskExecutions: Map<string, DurableTaskExecutionStorageObject>
  private transactionLock: Promise<void>

  constructor({ enableDebug = false }: { enableDebug?: boolean } = {}) {
    this.logger = createConsoleLogger('InMemoryStorage')
    if (!enableDebug) {
      this.logger = createLoggerDebugDisabled(this.logger)
    }
    this.taskExecutions = new Map()
    this.transactionLock = Promise.resolve()
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    await this.transactionLock

    let resolveLock: (() => void) | undefined
    this.transactionLock = new Promise<void>((resolve) => {
      resolveLock = resolve
    })

    try {
      const tx = new InMemoryStorageTx(this.logger, this.taskExecutions)
      const output = await fn(tx)
      this.taskExecutions = tx.taskExecutions
      return output
    } finally {
      resolveLock?.()
    }
  }

  async saveToFile(path: string): Promise<void> {
    await writeFile(path, JSON.stringify(this.taskExecutions, null, 2))
  }

  async loadFromFile(path: string): Promise<void> {
    try {
      const data = await readFile(path, 'utf8')
      if (!data.trim()) {
        this.taskExecutions = new Map()
        return
      }

      this.taskExecutions = new Map(
        JSON.parse(data) as Array<[string, DurableTaskExecutionStorageObject]>,
      )
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

export class InMemoryStorageTx implements DurableStorageTx {
  private logger: Logger
  readonly taskExecutions: Map<string, DurableTaskExecutionStorageObject>

  constructor(logger: Logger, executions: Map<string, DurableTaskExecutionStorageObject>) {
    this.logger = logger
    this.taskExecutions = new Map<string, DurableTaskExecutionStorageObject>()
    for (const [key, value] of executions) {
      this.taskExecutions.set(key, { ...value })
    }
  }

  insertTaskExecutions(executions: Array<DurableTaskExecutionStorageObject>): Promise<void> {
    this.logger.debug(
      `Inserting ${executions.length} task executions: executions=${executions.map((e) => e.executionId).join(', ')}`,
    )
    for (const execution of executions) {
      if (this.taskExecutions.has(execution.executionId)) {
        throw new Error(`Task execution ${execution.executionId} already exists`)
      }
      this.taskExecutions.set(execution.executionId, execution)
    }
    return Promise.resolve()
  }

  getTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<DurableTaskExecutionStorageObject>> {
    let executions = [...this.taskExecutions.values()].filter((execution) => {
      if (
        where.type === 'by_ids' &&
        where.ids.includes(execution.executionId) &&
        (!where.statuses || where.statuses.includes(execution.status)) &&
        (!where.needsPromiseCancellation ||
          execution.needsPromiseCancellation === where.needsPromiseCancellation)
      ) {
        return true
      }
      if (
        where.type === 'by_statuses' &&
        where.statuses.includes(execution.status) &&
        (where.isClosed == null || execution.isClosed === where.isClosed) &&
        (where.expiresAtLessThan == null ||
          (execution.expiresAt && execution.expiresAt < where.expiresAtLessThan))
      ) {
        return true
      }
      if (
        where.type === 'by_start_at_less_than' &&
        execution.startAt < where.startAtLessThan &&
        (!where.statuses || where.statuses.includes(execution.status))
      ) {
        return true
      }
      return false
    })
    if (limit != null && limit >= 0) {
      executions = executions.slice(0, limit)
    }
    this.logger.debug(
      `Got ${executions.length} task executions: where=${JSON.stringify(where)} limit=${limit} executions=${executions.map((e) => e.executionId).join(', ')}`,
    )
    return Promise.resolve(executions)
  }

  async updateTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    update: DurableTaskExecutionStorageObjectUpdate,
    limit?: number,
  ): Promise<Array<string>> {
    const executions = await this.getTaskExecutions(where, limit)
    for (const execution of executions) {
      for (const key in update) {
        if (key != null) {
          // @ts-expect-error - This is safe because we know the key is valid
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
          execution[key] = update[key]
        }
      }
    }
    return executions.map((execution) => execution.executionId)
  }
}
