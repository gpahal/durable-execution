import {
  and,
  eq,
  inArray,
  lt,
  type ExtractTablesWithRelations,
  type SQL,
  type TablesRelationalConfig,
} from 'drizzle-orm'
import {
  bigint,
  boolean,
  index,
  int,
  json,
  mysqlTable,
  text,
  timestamp,
  uniqueIndex,
  varchar,
  type MySqlDatabase,
  type MySqlQueryResultHKT,
  type MySqlTransaction,
  type PreparedQueryHKTBase,
} from 'drizzle-orm/mysql-core'
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
 * Create a mysql table for durable task executions.
 *
 * @param tableName - The name of the table.
 * @returns The mysql table.
 */
export function createDurableTaskExecutionsMySQLTable(tableName = 'durable_task_executions') {
  return mysqlTable(
    tableName,
    {
      id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      isFinalizeTask: boolean('is_finalize_task'),
      taskId: text('task_id').notNull(),
      executionId: varchar('execution_id', { length: 255 }).notNull(),
      retryOptions: json('retry_options').$type<DurableTaskRetryOptions>().notNull(),
      timeoutMs: int('timeout_ms').notNull(),
      sleepMsBeforeRun: int('sleep_ms_before_run').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenTasksCompletedCount: int('children_tasks_completed_count').notNull(),
      childrenTasks: json('children_tasks').$type<Array<DurableChildTaskExecution>>(),
      childrenTasksErrors:
        json('children_tasks_errors').$type<Array<DurableChildTaskExecutionErrorStorageObject>>(),
      finalizeTask: json('finalize_task').$type<DurableChildTaskExecution>(),
      finalizeTaskError: json('finalize_task_error').$type<DurableExecutionErrorStorageObject>(),
      error: json('error').$type<DurableExecutionErrorStorageObject>(),
      status: varchar('status', { length: 32 })
        .$type<DurableTaskExecutionStatusStorageObject>()
        .notNull(),
      isClosed: boolean('is_closed').notNull(),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      retryAttempts: int('retry_attempts').notNull(),
      startAt: timestamp('start_at').notNull(),
      startedAt: timestamp('started_at'),
      finishedAt: timestamp('finished_at'),
      expiresAt: timestamp('expires_at'),
      createdAt: timestamp('created_at').notNull(),
      updatedAt: timestamp('updated_at').notNull(),
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
 * The type of the mysql table for durable task executions.
 */
export type DurableTaskExecutionsMySQLTable = ReturnType<
  typeof createDurableTaskExecutionsMySQLTable
>

/**
 * Create a mysql durable storage.
 *
 * @param db - The mysql database.
 * @param table - The mysql task executions table.
 * @returns The mysql durable storage.
 */
export function createMySQLDurableStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
  table: DurableTaskExecutionsMySQLTable,
): DurableStorage {
  return new MySQLDurableStorage(db, table)
}

class MySQLDurableStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements DurableStorage
{
  private db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>
  private table: DurableTaskExecutionsMySQLTable

  constructor(
    db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    table: DurableTaskExecutionsMySQLTable,
  ) {
    this.db = db
    this.table = table
  }

  async withTransaction<T>(fn: (tx: DurableStorageTx) => Promise<T>): Promise<T> {
    return await this.db.transaction(async (tx) => {
      const durableTx = new MySQLDurableStorageTx(tx, this.table)
      return await fn(durableTx)
    })
  }
}

class MySQLDurableStorageTx<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig = ExtractTablesWithRelations<TFullSchema>,
> implements DurableStorageTx
{
  private tx: MySqlTransaction<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>
  private table: DurableTaskExecutionsMySQLTable

  constructor(
    tx: MySqlTransaction<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    table: DurableTaskExecutionsMySQLTable,
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
    const rowsToUpdate = await this.tx
      .select({ executionId: this.table.executionId })
      .from(this.table)
      .where(buildWhereCondition(this.table, where))
      .for('update')
    if (rowsToUpdate.length === 0) {
      return []
    }

    const executionIds = rowsToUpdate.map((row) => row.executionId)
    await this.tx
      .update(this.table)
      .set(storageUpdateToUpdateValue(update))
      .where(inArray(this.table.executionId, executionIds))
    return executionIds
  }
}

function buildWhereCondition(
  table: DurableTaskExecutionsMySQLTable,
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
