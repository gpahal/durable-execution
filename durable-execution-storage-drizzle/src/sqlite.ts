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
  createTransactionMutex,
  type DurableChildTaskExecution,
  type DurableChildTaskExecutionErrorStorageObject,
  type DurableExecutionErrorStorageObject,
  type DurableStorage,
  type DurableStorageTx,
  type DurableTaskExecutionStatusStorageObject,
  type DurableTaskExecutionStorageObject,
  type DurableTaskExecutionStorageObjectUpdate,
  type DurableTaskExecutionStorageWhere,
  type DurableTaskRetryOptions,
  type TransactionMutex,
} from 'durable-execution'

import {
  selectValueToStorageObject,
  storageObjectToInsertValue,
  storageUpdateToUpdateValue,
} from './common'

/**
 * Create a sqlite table for durable task executions.
 *
 * @param tableName - The name of the table.
 * @returns The sqlite table.
 */
export function createDurableTaskExecutionsSQLiteTable(tableName = 'durable_task_executions') {
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
      retryOptions: text('retry_options', { mode: 'json' })
        .$type<DurableTaskRetryOptions>()
        .notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenTasksCompletedCount: integer('children_tasks_completed_count').notNull(),
      childrenTasks: text('children_tasks', { mode: 'json' }).$type<
        Array<DurableChildTaskExecution>
      >(),
      childrenTasksErrors: text('children_tasks_errors', { mode: 'json' }).$type<
        Array<DurableChildTaskExecutionErrorStorageObject>
      >(),
      finalizeTask: text('finalize_task', {
        mode: 'json',
      }).$type<DurableChildTaskExecution>(),
      finalizeTaskError: text('finalize_task_error', {
        mode: 'json',
      }).$type<DurableExecutionErrorStorageObject>(),
      error: text('error', { mode: 'json' }).$type<DurableExecutionErrorStorageObject>(),
      status: text('status').$type<DurableTaskExecutionStatusStorageObject>().notNull(),
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
 * The type of the sqlite table for durable task executions.
 */
export type DurableTaskExecutionsSQLiteTable = ReturnType<
  typeof createDurableTaskExecutionsSQLiteTable
>

/**
 * Create a sqlite durable storage.
 *
 * @param db - The sqlite database.
 * @param table - The sqlite task executions table.
 * @returns The sqlite durable storage.
 */
export function createSQLiteDurableStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
  table: DurableTaskExecutionsSQLiteTable,
): DurableStorage {
  return new SQLiteDurableStorage(db, table)
}

class SQLiteDurableStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements DurableStorage
{
  private readonly db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>
  private readonly table: DurableTaskExecutionsSQLiteTable
  private readonly transactionMutex: TransactionMutex

  constructor(
    db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
    table: DurableTaskExecutionsSQLiteTable,
  ) {
    this.db = db
    this.table = table
    this.transactionMutex = createTransactionMutex()
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    await this.transactionMutex.acquire()
    try {
      return await this.db.transaction(async (tx) => {
        const durableTx = new SQLiteDurableStorageTx(tx, this.table)
        return await fn(durableTx)
      })
    } finally {
      this.transactionMutex.release()
    }
  }
}

class SQLiteDurableStorageTx<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements DurableStorageTx
{
  private readonly tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>
  private readonly table: DurableTaskExecutionsSQLiteTable

  constructor(
    tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>,
    table: DurableTaskExecutionsSQLiteTable,
  ) {
    this.tx = tx
    this.table = table
  }

  async insertTaskExecutions(executions: Array<DurableTaskExecutionStorageObject>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => storageObjectToInsertValue(execution))
    await this.tx.insert(this.table).values(rows)
  }

  async getTaskExecutionIds(
    where: DurableTaskExecutionStorageWhere,
    limit?: number,
    // skipLockedForUpdate can be ignored as transactions run sequentially using the mutex
    _skipLockedForUpdate?: boolean,
  ): Promise<Array<string>> {
    const query = this.tx
      .select({ executionId: this.table.executionId })
      .from(this.table)
      .where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => row.executionId)
  }

  async getTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    limit?: number,
    // skipLockedForUpdate can be ignored as transactions run sequentially using the mutex
    _skipLockedForUpdate?: boolean,
  ): Promise<Array<DurableTaskExecutionStorageObject>> {
    const query = this.tx.select().from(this.table).where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => selectValueToStorageObject(row))
  }

  async updateTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    update: DurableTaskExecutionStorageObjectUpdate,
  ): Promise<number> {
    const rows = await this.tx
      .update(this.table)
      .set(storageUpdateToUpdateValue(update))
      .where(buildWhereCondition(this.table, where))
      .returning({ executionId: this.table.executionId })
    return rows.length
  }
}

function buildWhereCondition(
  table: DurableTaskExecutionsSQLiteTable,
  where: DurableTaskExecutionStorageWhere,
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
