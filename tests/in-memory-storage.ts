import {
  createConsoleLogger,
  type DurableFunctionExecutionStorageObject,
  type DurableFunctionExecutionStorageObjectUpdate,
  type DurableFunctionExecutionStorageWhere,
  type DurableStorage,
  type DurableStorageTx,
  type Logger,
} from '../src'
import { createLoggerDebugDisabled } from '../src/logger'

export class InMemoryStorage implements DurableStorage {
  private logger: Logger
  private executions: Map<string, DurableFunctionExecutionStorageObject>
  private transactionLock: Promise<void>

  constructor({ enableDebug = false }: { enableDebug?: boolean } = {}) {
    this.logger = createConsoleLogger('InMemoryStorage')
    if (!enableDebug) {
      this.logger = createLoggerDebugDisabled(this.logger)
    }
    this.executions = new Map()
    this.transactionLock = Promise.resolve()
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    await this.transactionLock

    let resolveLock: (() => void) | undefined
    this.transactionLock = new Promise<void>((resolve) => {
      resolveLock = resolve
    })

    try {
      const tx = new InMemoryStorageTx(this.logger, this.executions)
      const output = await fn(tx)
      this.executions = tx.executions
      return output
    } finally {
      resolveLock?.()
    }
  }
}

export class InMemoryStorageTx implements DurableStorageTx {
  private logger: Logger
  readonly executions: Map<string, DurableFunctionExecutionStorageObject>

  constructor(logger: Logger, executions: Map<string, DurableFunctionExecutionStorageObject>) {
    this.logger = logger
    this.executions = new Map<string, DurableFunctionExecutionStorageObject>()
    for (const [key, value] of executions) {
      this.executions.set(key, { ...value })
    }
  }

  insertFunctionExecutions(
    executions: Array<DurableFunctionExecutionStorageObject>,
  ): Promise<void> {
    this.logger.debug(
      `Inserting ${executions.length} executions: executions=${executions.map((e) => e.executionId).join(', ')}`,
    )
    for (const execution of executions) {
      if (this.executions.has(execution.executionId)) {
        throw new Error(`Execution ${execution.executionId} already exists`)
      }
      this.executions.set(execution.executionId, execution)
    }
    return Promise.resolve()
  }

  getFunctionExecutions(
    where: DurableFunctionExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<DurableFunctionExecutionStorageObject>> {
    let executions = [...this.executions.values()].filter((execution) => {
      if (
        where.type === 'by_ids' &&
        where.ids.includes(execution.executionId) &&
        (!where.statuses || where.statuses.includes(execution.status))
      ) {
        return true
      }
      if (
        where.type === 'by_statuses' &&
        where.statuses.includes(execution.status) &&
        (where.isFinalized == null || execution.isFinalized) &&
        (where.expiresAtLessThan == null ||
          (execution.expiresAt && execution.expiresAt < where.expiresAtLessThan))
      ) {
        return true
      }
      if (
        where.type === 'by_start_at' &&
        execution.status === 'ready' &&
        execution.startAt &&
        execution.startAt < where.startAtLessThan
      ) {
        return true
      }
      return false
    })
    if (limit != null && limit >= 0) {
      executions = executions.slice(0, limit)
    }
    this.logger.debug(
      `Got ${executions.length} executions: where=${JSON.stringify(where)} limit=${limit} executions=${executions.map((e) => e.executionId).join(', ')}`,
    )
    return Promise.resolve(executions)
  }

  async updateFunctionExecutions(
    where: DurableFunctionExecutionStorageWhere,
    update: DurableFunctionExecutionStorageObjectUpdate,
    limit?: number,
  ): Promise<Array<string>> {
    const executions = await this.getFunctionExecutions(where, limit)
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
