import { and, eq, inArray, lt, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  bigint,
  boolean,
  index,
  integer,
  json,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  type PgDatabase,
  type PgQueryResultHKT,
  type PgTransaction,
} from 'drizzle-orm/pg-core'
import type {
  ChildTaskExecution,
  ChildTaskExecutionErrorStorageValue,
  DurableExecutionErrorStorageValue,
  Storage,
  StorageTx,
  TaskExecutionStatusStorageValue,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskExecutionStorageWhere,
  TaskRetryOptions,
} from 'durable-execution'

import {
  selectValueToStorageValue,
  storageValueToInsertValue,
  storageValueToUpdateValue,
} from './common'

/**
 * Create a pg table for task executions.
 *
 * @param tableName - The name of the table.
 * @returns The pg table.
 */
export function createTaskExecutionsPgTable(tableName = 'task_executions') {
  return pgTable(
    tableName,
    {
      id: bigint('id', { mode: 'number' }).primaryKey().generatedAlwaysAsIdentity(),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      isFinalizeTask: boolean('is_finalize_task'),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenTaskExecutionsCompletedCount: integer(
        'children_task_executions_completed_count',
      ).notNull(),
      childrenTaskExecutions: json('children_task_executions').$type<Array<ChildTaskExecution>>(),
      childrenTaskExecutionsErrors: json('children_task_executions_errors').$type<
        Array<ChildTaskExecutionErrorStorageValue>
      >(),
      finalizeTaskExecution: json('finalize_task_execution').$type<ChildTaskExecution>(),
      finalizeTaskExecutionError: json(
        'finalize_task_execution_error',
      ).$type<DurableExecutionErrorStorageValue>(),
      error: json('error').$type<DurableExecutionErrorStorageValue>(),
      status: text('status').$type<TaskExecutionStatusStorageValue>().notNull(),
      isClosed: boolean('is_closed').notNull(),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: timestamp('start_at', { withTimezone: true }).notNull(),
      startedAt: timestamp('started_at', { withTimezone: true }),
      finishedAt: timestamp('finished_at', { withTimezone: true }),
      expiresAt: timestamp('expires_at', { withTimezone: true }),
      version: integer('version').notNull(),
      createdAt: timestamp('created_at', { withTimezone: true }).notNull(),
      updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
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
 * The type of the pg table for task executions.
 */
export type TaskExecutionsPgTable = ReturnType<typeof createTaskExecutionsPgTable>

/**
 * Create a pg storage.
 *
 * @param db - The pg database.
 * @param table - The pg task executions table.
 * @returns The pg storage.
 */
export function createPgStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(db: PgDatabase<TQueryResult, TFullSchema, TSchema>, table: TaskExecutionsPgTable): Storage {
  return new PgStorage(db, table)
}

class PgStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements Storage
{
  private db: PgDatabase<TQueryResult, TFullSchema, TSchema>
  private table: TaskExecutionsPgTable

  constructor(db: PgDatabase<TQueryResult, TFullSchema, TSchema>, table: TaskExecutionsPgTable) {
    this.db = db
    this.table = table
  }

  async withTransaction<T>(fn: (tx: StorageTx) => Promise<T>): Promise<T> {
    return await this.db.transaction(async (tx) => {
      const storageTx = new PgStorageTx(tx, this.table)
      return await fn(storageTx)
    })
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => storageValueToInsertValue(execution))
    await this.db.insert(this.table).values(rows)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.db.select().from(this.table).where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => selectValueToStorageValue(row))
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
    const rows = await this.db
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
    await this.withTransaction(async (tx) => {
      await tx.insertTaskExecutions(executions)
      await tx.updateAllTaskExecutions(where, update)
    })
  }
}

class PgStorageTx<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements StorageTx
{
  private tx: PgTransaction<TQueryResult, TFullSchema, TSchema>
  private table: TaskExecutionsPgTable

  constructor(tx: PgTransaction<TQueryResult, TFullSchema, TSchema>, table: TaskExecutionsPgTable) {
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
    const queryWithLimit = limit != null && limit > 0 ? query.limit(limit) : query
    const rows = await queryWithLimit.for('update', { skipLocked: true })

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
  table: TaskExecutionsPgTable,
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
