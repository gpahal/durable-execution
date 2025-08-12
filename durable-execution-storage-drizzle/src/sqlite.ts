import { and, asc, eq, inArray, lt, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
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
  type FinishedChildTaskExecutionStorageValue,
  type Mutex,
  type Storage,
  type TaskExecutionStatusStorageValue,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionStorageWhere,
  type TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionSelectValueToStorageValue,
  taskExecutionStorageValueToInsertValue,
  taskExecutionStorageValueToUpdateValue,
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
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      status: text('status').$type<TaskExecutionStatusStorageValue>().notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: text('error', { mode: 'json' }).$type<DurableExecutionErrorStorageValue>(),
      needsPromiseCancellation: integer('needs_promise_cancellation', {
        mode: 'boolean',
      }).notNull(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: integer('start_at', { mode: 'timestamp' }).notNull(),
      startedAt: integer('started_at', { mode: 'timestamp' }),
      expiresAt: integer('expires_at', { mode: 'timestamp' }),
      finishedAt: integer('finished_at', { mode: 'timestamp' }),
      childrenTaskExecutions: text('children_task_executions', { mode: 'json' }).$type<
        Array<ChildTaskExecution>
      >(),
      completedChildrenTaskExecutions: text('completed_children_task_executions', {
        mode: 'json',
      }).$type<Array<ChildTaskExecution>>(),
      childrenTaskExecutionsErrors: text('children_task_executions_errors', { mode: 'json' }).$type<
        Array<ChildTaskExecutionErrorStorageValue>
      >(),
      finalizeTaskExecution: text('finalize_task_execution', {
        mode: 'json',
      }).$type<ChildTaskExecution>(),
      finalizeTaskExecutionError: text('finalize_task_execution_error', {
        mode: 'json',
      }).$type<DurableExecutionErrorStorageValue>(),
      isClosed: integer('is_closed', { mode: 'boolean' }).notNull(),
      closedAt: integer('closed_at', { mode: 'timestamp' }),
      createdAt: integer('created_at', { mode: 'timestamp' }).notNull(),
      updatedAt: integer('updated_at', { mode: 'timestamp' }).notNull(),
    },
    (table) => [
      uniqueIndex(`ix_${tableName}_execution_id`).on(table.executionId),
      index(`ix_${tableName}_execution_id_updated_at`).on(table.executionId, table.updatedAt),
      index(`ix_${tableName}_status_updated_at`).on(table.status, table.updatedAt),
      index(`ix_${tableName}_status_start_at_updated_at`).on(
        table.status,
        table.startAt,
        table.updatedAt,
      ),
      index(`ix_${tableName}_status_expires_at_updated_at`).on(
        table.status,
        table.expiresAt,
        table.updatedAt,
      ),
      index(`ix_${tableName}_is_closed_status_updated_at`).on(
        table.isClosed,
        table.status,
        table.updatedAt,
      ),
    ],
  )
}

/**
 * Create a sqlite table for finished child task executions.
 *
 * @param tableName - The name of the table.
 * @returns The sqlite table.
 */
export function createFinishedChildTaskExecutionsSQLiteTable(
  tableName = 'finished_child_task_executions',
) {
  return sqliteTable(
    tableName,
    {
      id: integer('id').primaryKey({ autoIncrement: true }),
      parentExecutionId: text('parent_execution_id').notNull(),
      createdAt: integer('created_at', { mode: 'timestamp' }).notNull(),
      updatedAt: integer('updated_at', { mode: 'timestamp' }).notNull(),
    },
    (table) => [
      uniqueIndex(`ix_${tableName}_parent_execution_id`).on(table.parentExecutionId),
      index(`ix_${tableName}_updated_at`).on(table.updatedAt),
    ],
  )
}

/**
 * The type of the sqlite table for task executions.
 */
export type TaskExecutionsSQLiteTable = ReturnType<typeof createTaskExecutionsSQLiteTable>

/**
 * The type of the sqlite table for finished child task executions.
 */
export type FinishedChildTaskExecutionsSQLiteTable = ReturnType<
  typeof createFinishedChildTaskExecutionsSQLiteTable
>

/**
 * Create a sqlite storage.
 *
 * @param db - The sqlite database.
 * @param taskExecutionsTable - The sqlite task executions table.
 * @param finishedChildTaskExecutionsTable - The sqlite finished child task executions table.
 * @returns The sqlite storage.
 */
export function createSQLiteStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsSQLiteTable,
  finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsSQLiteTable,
): Storage {
  return new SQLiteStorage(db, taskExecutionsTable, finishedChildTaskExecutionsTable)
}

class SQLiteStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements Storage
{
  private readonly db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsSQLiteTable
  private readonly finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsSQLiteTable
  private readonly transactionMutex: Mutex

  constructor(
    db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsSQLiteTable,
    finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsSQLiteTable,
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
    this.finishedChildTaskExecutionsTable = finishedChildTaskExecutionsTable
    this.transactionMutex = createMutex()
  }

  async withTransaction<T>(
    fn: (tx: SQLiteStorageTx<TRunResult, TFullSchema, TSchema>) => Promise<T>,
  ): Promise<T> {
    await this.transactionMutex.acquire()
    try {
      return await this.db.transaction(async (tx) => {
        const storageTx = new SQLiteStorageTx(
          tx,
          this.taskExecutionsTable,
          this.finishedChildTaskExecutionsTable,
        )
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

  async updateTaskExecutionsAndReturn(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.updateTaskExecutionsAndReturn(where, update, limit)
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

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    await this.withTransaction(async (tx) => {
      await tx.insertFinishedChildTaskExecutionIfNotExists(finishedChildTaskExecution)
    })
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.deleteFinishedChildTaskExecutionsAndReturn(limit)
    })
  }
}

class SQLiteStorageTx<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> {
  private readonly tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsSQLiteTable
  private readonly finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsSQLiteTable

  constructor(
    tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsSQLiteTable,
    finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsSQLiteTable,
  ) {
    this.tx = tx
    this.taskExecutionsTable = taskExecutionsTable
    this.finishedChildTaskExecutionsTable = finishedChildTaskExecutionsTable
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => taskExecutionStorageValueToInsertValue(execution))
    await this.tx.insert(this.taskExecutionsTable).values(rows)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.tx
      .select()
      .from(this.taskExecutionsTable)
      .where(buildTaskExecutionWhereCondition(this.taskExecutionsTable, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
  }

  async updateTaskExecutionsAndReturn(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.tx
      .select()
      .from(this.taskExecutionsTable)
      .where(buildTaskExecutionWhereCondition(this.taskExecutionsTable, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)

    await this.tx
      .update(this.taskExecutionsTable)
      .set(taskExecutionStorageValueToUpdateValue(update))
      .where(
        inArray(
          this.taskExecutionsTable.executionId,
          rows.map((row) => row.executionId),
        ),
      )
    return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    const rows = await this.tx
      .update(this.taskExecutionsTable)
      .set(taskExecutionStorageValueToUpdateValue(update))
      .where(buildTaskExecutionWhereCondition(this.taskExecutionsTable, where))
      .returning({ executionId: this.taskExecutionsTable.executionId })
    return rows.length
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    await this.tx
      .insert(this.finishedChildTaskExecutionsTable)
      .values(finishedChildTaskExecution)
      .onConflictDoNothing()
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    const rows = await this.tx
      .select()
      .from(this.finishedChildTaskExecutionsTable)
      .orderBy(asc(this.finishedChildTaskExecutionsTable.updatedAt))
      .limit(limit)
    await this.tx.delete(this.finishedChildTaskExecutionsTable).where(
      inArray(
        this.finishedChildTaskExecutionsTable.parentExecutionId,
        rows.map((row) => row.parentExecutionId),
      ),
    )
    return rows.map((row) => ({
      parentExecutionId: row.parentExecutionId,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    }))
  }
}

function buildTaskExecutionWhereCondition(
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
      break
    }
    case 'by_status_and_start_at_less_than': {
      conditions.push(eq(table.status, where.status), lt(table.startAt, where.startAtLessThan))
      break
    }
    case 'by_status_and_expires_at_less_than': {
      conditions.push(eq(table.status, where.status), lt(table.expiresAt, where.expiresAtLessThan))
      break
    }
    case 'by_is_closed': {
      conditions.push(eq(table.isClosed, where.isClosed))
      if (where.statuses) {
        conditions.push(inArray(table.status, where.statuses))
      }
      break
    }
  }
  return and(...conditions)
}
