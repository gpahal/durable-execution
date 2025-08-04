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
  type MySqlDatabase,
  type MySqlQueryResultHKT,
  type MySqlTransaction,
  type PreparedQueryHKTBase,
} from 'drizzle-orm/mysql-core'
import type {
  DurableStorage,
  DurableStorageTx,
  DurableTaskChildErrorStorageObject,
  DurableTaskChildExecutionStorageObject,
  DurableTaskErrorStorageObject,
  DurableTaskExecutionStatus,
  DurableTaskExecutionStorageObject,
  DurableTaskExecutionStorageObjectUpdate,
  DurableTaskExecutionStorageWhere,
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
      isOnRunAndChildrenCompleteChild: boolean('is_on_run_and_children_complete_child'),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      runInput: text('run_input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      childrenCompletedCount: int('children_completed_count').notNull(),
      children: json('children').$type<Array<DurableTaskChildExecutionStorageObject>>(),
      childrenErrors: json('children_errors').$type<Array<DurableTaskChildErrorStorageObject>>(),
      onRunAndChildrenComplete: json(
        'on_run_and_children_complete',
      ).$type<DurableTaskChildExecutionStorageObject>(),
      onRunAndChildrenCompleteError: json(
        'on_run_and_children_complete_error',
      ).$type<DurableTaskErrorStorageObject>(),
      error: json('error').$type<DurableTaskErrorStorageObject>(),
      status: text('status').$type<DurableTaskExecutionStatus>().notNull(),
      isClosed: boolean('is_closed').notNull(),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      retryAttempts: int('retry_attempts').notNull(),
      startAt: timestamp('start_at').notNull(),
      startedAt: timestamp('started_at'),
      finishedAt: timestamp('finished_at'),
      expiresAt: timestamp('expires_at').notNull(),
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
    const whereCondition = buildWhereCondition(this.table, where)

    // First, get the IDs that will be updated
    const idsToUpdate = await this.tx
      .select({ executionId: this.table.executionId })
      .from(this.table)
      .where(whereCondition)

    // Then perform the update
    if (idsToUpdate.length > 0) {
      await this.tx.update(this.table).set(storageUpdateToUpdateValue(update)).where(whereCondition)
    }

    return idsToUpdate.map((row) => row.executionId)
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
