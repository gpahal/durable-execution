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
  DurableChildTaskExecution,
  DurableChildTaskExecutionErrorStorageObject,
  DurableExecutionErrorStorageObject,
  DurableStorage,
  DurableStorageTx,
  DurableTaskExecutionStatusStorageObject,
  DurableTaskExecutionStorageObject,
  DurableTaskExecutionStorageObjectUpdate,
  DurableTaskExecutionStorageWhere,
  DurableTaskRetryOptions,
} from 'durable-execution'

import {
  selectValueToStorageObject,
  storageObjectToInsertValue,
  storageUpdateToUpdateValue,
} from './common'

/**
 * Create a pg table for durable task executions.
 *
 * @param tableName - The name of the table.
 * @returns The pg table.
 */
export function createDurableTaskExecutionsPgTable(tableName = 'durable_task_executions') {
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
      retryOptions: json('retry_options').$type<DurableTaskRetryOptions>().notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenTasksCompletedCount: integer('children_tasks_completed_count').notNull(),
      childrenTasks: json('children_tasks').$type<Array<DurableChildTaskExecution>>(),
      childrenTasksErrors:
        json('children_tasks_errors').$type<Array<DurableChildTaskExecutionErrorStorageObject>>(),
      finalizeTask: json('finalize_task').$type<DurableChildTaskExecution>(),
      finalizeTaskError: json('finalize_task_error').$type<DurableExecutionErrorStorageObject>(),
      error: json('error').$type<DurableExecutionErrorStorageObject>(),
      status: text('status').$type<DurableTaskExecutionStatusStorageObject>().notNull(),
      isClosed: boolean('is_closed').notNull(),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: timestamp('start_at', { withTimezone: true }).notNull(),
      startedAt: timestamp('started_at', { withTimezone: true }),
      finishedAt: timestamp('finished_at', { withTimezone: true }),
      expiresAt: timestamp('expires_at', { withTimezone: true }),
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
 * The type of the pg table for durable task executions.
 */
export type DurableTaskExecutionsPgTable = ReturnType<typeof createDurableTaskExecutionsPgTable>

/**
 * Create a pg durable storage.
 *
 * @param db - The pg database.
 * @param table - The pg task executions table.
 * @returns The pg durable storage.
 */
export function createPgDurableStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
  table: DurableTaskExecutionsPgTable,
): DurableStorage {
  return new PgDurableStorage(db, table)
}

class PgDurableStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements DurableStorage
{
  private db: PgDatabase<TQueryResult, TFullSchema, TSchema>
  private table: DurableTaskExecutionsPgTable

  constructor(
    db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
    table: DurableTaskExecutionsPgTable,
  ) {
    this.db = db
    this.table = table
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    return await this.db.transaction(async (tx) => {
      const durableTx = new PgDurableStorageTx(tx, this.table)
      return await fn(durableTx)
    })
  }
}

class PgDurableStorageTx<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements DurableStorageTx
{
  private tx: PgTransaction<TQueryResult, TFullSchema, TSchema>
  private table: DurableTaskExecutionsPgTable

  constructor(
    tx: PgTransaction<TQueryResult, TFullSchema, TSchema>,
    table: DurableTaskExecutionsPgTable,
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
  ): Promise<Array<string>> {
    let rows: Array<{ executionId: string }> = []
    const query = this.tx
      .select({ executionId: this.table.executionId })
      .from(this.table)
      .where(buildWhereCondition(this.table, where))
    rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => row.executionId)
  }

  async getTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<DurableTaskExecutionStorageObject>> {
    const query = this.tx.select().from(this.table).where(buildWhereCondition(this.table, where))
    const rows = await (limit != null && limit > 0 ? query.limit(limit) : query)
    return rows.map((row) => selectValueToStorageObject(row))
  }

  async updateTaskExecutions(
    where: DurableTaskExecutionStorageWhere,
    update: DurableTaskExecutionStorageObjectUpdate,
  ): Promise<Array<string>> {
    const rows = await this.tx
      .update(this.table)
      .set(storageUpdateToUpdateValue(update))
      .where(buildWhereCondition(this.table, where))
      .returning({ executionId: this.table.executionId })
    return rows.map((row) => row.executionId)
  }
}

function buildWhereCondition(
  table: DurableTaskExecutionsPgTable,
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
