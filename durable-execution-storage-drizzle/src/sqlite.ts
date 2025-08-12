import { and, eq, inArray, lt, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  index,
  integer,
  sqliteTable,
  text,
  uniqueIndex,
  type BaseSQLiteDatabase,
  type SQLiteTransaction,
} from 'drizzle-orm/sqlite-core'
import {
  createMutex,
  type ChildTaskExecution,
  type ChildTaskExecutionErrorStorageValue,
  type DurableExecutionErrorStorageValue,
  type Mutex,
  type Storage,
  type StorageTx,
  type TaskExecutionStatusStorageValue,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionStorageWhere,
  type TaskRetryOptions,
} from 'durable-execution'

import {
  selectValueToStorageValue,
  storageValueToInsertValue,
  storageValueToUpdateValue,
} from './common'

/**
 * Create a sqlite table for task executions.
 *
 * @param tableName - The name of the table.
 * @returns The sqlite table.
 */
export function createTaskExecutionsSQLiteTable(tableName = 'task_executions') {
  return sqliteTable(
    tableName,
    {
      id: integer('id').primaryKey({ autoIncrement: true }),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      isFinalizeTask: integer('is_finalize_task', {
        mode: 'boolean',
      }),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: text('retry_options', { mode: 'json' }).$type<TaskRetryOptions>().notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenTaskExecutionsCompletedCount: integer(
        'children_task_executions_completed_count',
      ).notNull(),
      childrenTaskExecutions: text('children_task_executions', { mode: 'json' }).$type<
        Array<ChildTaskExecution>
      >(),
      childrenTaskExecutionsErrors: text('children_task_executions_errors', { mode: 'json' }).$type<
        Array<ChildTaskExecutionErrorStorageValue>
      >(),
      finalizeTaskExecution: text('finalize_task_execution', {
        mode: 'json',
      }).$type<ChildTaskExecution>(),
      finalizeTaskExecutionError: text('finalize_task_execution_error', {
        mode: 'json',
      }).$type<DurableExecutionErrorStorageValue>(),
      error: text('error', { mode: 'json' }).$type<DurableExecutionErrorStorageValue>(),
      status: text('status').$type<TaskExecutionStatusStorageValue>().notNull(),
      isClosed: integer('is_closed', { mode: 'boolean' }).notNull(),
      needsPromiseCancellation: integer('needs_promise_cancellation', {
        mode: 'boolean',
      }).notNull(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: integer('start_at', { mode: 'timestamp' }).notNull(),
      startedAt: integer('started_at', { mode: 'timestamp' }),
      finishedAt: integer('finished_at', { mode: 'timestamp' }),
      expiresAt: integer('expires_at', { mode: 'timestamp' }),
      version: integer('version').notNull(),
      createdAt: integer('created_at', { mode: 'timestamp' }).notNull(),
      updatedAt: integer('updated_at', { mode: 'timestamp' }).notNull(),
    },
    (table) => [
      uniqueIndex(`ix_${tableName}_execution_id`).on(table.executionId),
      index(`ix_${tableName}_status_is_closed_expires_at`).on(
        table.status,
        table.isClosed,
        table.expiresAt,
      ),
      index(`ix_${tableName}_status_start_at`).on(table.status, table.startAt),
    ],
  )
}

/**
 * The type of the sqlite table for task executions.
 */
export type TaskExecutionsSQLiteTable = ReturnType<typeof createTaskExecutionsSQLiteTable>

/**
 * Create a sqlite storage.
 *
 * @param db - The sqlite database.
 * @param table - The sqlite task executions table.
 * @returns The sqlite storage.
 */
export function createSQLiteStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
  table: TaskExecutionsSQLiteTable,
): Storage {
  return new SQLiteStorage(db, table)
}

class SQLiteStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements Storage
{
  private readonly db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>
  private readonly table: TaskExecutionsSQLiteTable
  private readonly transactionMutex: Mutex

  constructor(
    db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
    table: TaskExecutionsSQLiteTable,
  ) {
    this.db = db
    this.table = table
    this.transactionMutex = createMutex()
  }

  async withTransaction<T>(fn: (tx: StorageTx) => Promise<T>): Promise<T> {
    await this.transactionMutex.acquire()
    try {
      return await this.db.transaction(async (tx) => {
        const storageTx = new SQLiteStorageTx(tx, this.table)
        return await fn(storageTx)
      })
    } finally {
      this.transactionMutex.release()
    }
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    await this.withTransaction(async (tx) => {
      await tx.insertTaskExecutions(executions)
    })
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.getTaskExecutions(where, limit)
    })
  }

  async updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.updateTaskExecutionsReturningTaskExecutions(where, update, limit)
    })
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    return await this.withTransaction(async (tx) => {
      return await tx.updateAllTaskExecutions(where, update)
    })
  }

  async insertTaskExecutionsAndUpdateAllTaskExecutions(
    executions: Array<TaskExecutionStorageValue>,
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.withTransaction(async (tx) => {
      await tx.insertTaskExecutionsAndUpdateAllTaskExecutions(executions, where, update)
    })
  }
}

class SQLiteStorageTx<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements StorageTx
{
  private readonly tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>
  private readonly table: TaskExecutionsSQLiteTable

  constructor(
    tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>,
    table: TaskExecutionsSQLiteTable,
  ) {
    this.tx = tx
    this.table = table
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => storageValueToInsertValue(execution))
    await this.tx.insert(this.table).values(rows)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.tx.select().from(this.table).where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => selectValueToStorageValue(row))
  }

  async updateTaskExecutionsReturningTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.tx.select().from(this.table).where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)

    await this.tx
      .update(this.table)
      .set(storageValueToUpdateValue(update))
      .where(
        inArray(
          this.table.executionId,
          rows.map((row) => row.executionId),
        ),
      )
    return rows.map((row) => selectValueToStorageValue(row))
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    const rows = await this.tx
      .update(this.table)
      .set(storageValueToUpdateValue(update))
      .where(buildWhereCondition(this.table, where))
      .returning({ executionId: this.table.executionId })
    return rows.length
  }

  async insertTaskExecutionsAndUpdateAllTaskExecutions(
    executions: Array<TaskExecutionStorageValue>,
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    await this.insertTaskExecutions(executions)
    await this.updateAllTaskExecutions(where, update)
  }
}

function buildWhereCondition(
  table: TaskExecutionsSQLiteTable,
  where: TaskExecutionStorageWhere,
): SQL | undefined {
  const conditions: Array<SQL> = []
  switch (where.type) {
    case 'by_execution_ids': {
      conditions.push(inArray(table.executionId, where.executionIds))
      if (where.statuses) {
        conditions.push(inArray(table.status, where.statuses))
      }
      if (where.needsPromiseCancellation !== undefined) {
        conditions.push(eq(table.needsPromiseCancellation, where.needsPromiseCancellation))
      }
      break
    }
    case 'by_statuses': {
      conditions.push(inArray(table.status, where.statuses))
      if (where.isClosed !== undefined) {
        conditions.push(eq(table.isClosed, where.isClosed))
      }
      if (where.expiresAtLessThan) {
        conditions.push(lt(table.expiresAt, where.expiresAtLessThan))
      }
      break
    }
    case 'by_start_at_less_than': {
      conditions.push(lt(table.startAt, where.startAtLessThan))
      if (where.statuses) {
        conditions.push(inArray(table.status, where.statuses))
      }
      break
    }
  }
  return and(...conditions)
}
