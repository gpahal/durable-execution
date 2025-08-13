import { and, asc, eq, inArray, lt, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  bigint,
  boolean,
  index,
  int,
  json,
  mysqlTable,
  serial,
  text,
  timestamp,
  uniqueIndex,
  varchar,
  type MySqlDatabase,
  type MySqlQueryResultHKT,
  type MySqlQueryResultKind,
  type MySqlTransaction,
  type PreparedQueryHKTBase,
} from 'drizzle-orm/mysql-core'
import type {
  ChildTaskExecution,
  ChildTaskExecutionErrorStorageValue,
  DurableExecutionErrorStorageValue,
  FinishedChildTaskExecutionStorageValue,
  Storage,
  TaskExecutionStatusStorageValue,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskExecutionStorageWhere,
  TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionSelectValueToStorageValue,
  taskExecutionStorageValueToInsertValue,
  taskExecutionStorageValueToUpdateValue,
} from './common'

/**
 * Create a mysql table for task executions.
 *
 * @param tableName - The name of the table.
 * @returns The mysql table.
 */
export function createTaskExecutionsMySqlTable(tableName = 'task_executions') {
  return mysqlTable(
    tableName,
    {
      id: serial('id').primaryKey(),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      isFinalizeTask: boolean('is_finalize_task'),
      taskId: text('task_id').notNull(),
      executionId: varchar('execution_id', { length: 64 }).notNull(),
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: bigint('sleep_ms_before_run', { mode: 'number' }).notNull(),
      timeoutMs: bigint('timeout_ms', { mode: 'number' }).notNull(),
      status: varchar('status', { length: 64 }).$type<TaskExecutionStatusStorageValue>().notNull(),
      input: text('input').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: json('error').$type<DurableExecutionErrorStorageValue>(),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      retryAttempts: int('retry_attempts').notNull(),
      startAt: timestamp('start_at').notNull(),
      startedAt: timestamp('started_at'),
      expiresAt: timestamp('expires_at'),
      finishedAt: timestamp('finished_at'),
      childrenTaskExecutions: json('children_task_executions').$type<Array<ChildTaskExecution>>(),
      completedChildrenTaskExecutions: json('completed_children_task_executions').$type<
        Array<ChildTaskExecution>
      >(),
      childrenTaskExecutionsErrors: json('children_task_executions_errors').$type<
        Array<ChildTaskExecutionErrorStorageValue>
      >(),
      finalizeTaskExecution: json('finalize_task_execution').$type<ChildTaskExecution>(),
      finalizeTaskExecutionError: json(
        'finalize_task_execution_error',
      ).$type<DurableExecutionErrorStorageValue>(),
      isClosed: boolean('is_closed').notNull(),
      closedAt: timestamp('closed_at'),
      createdAt: timestamp('created_at').notNull(),
      updatedAt: timestamp('updated_at').notNull(),
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
 * Create a mysql table for finished child task executions.
 *
 * @param tableName - The name of the table.
 * @returns The mysql table.
 */
export function createFinishedChildTaskExecutionsMySqlTable(
  tableName = 'finished_child_task_executions',
) {
  return mysqlTable(
    tableName,
    {
      id: serial('id').primaryKey(),
      parentExecutionId: varchar('parent_execution_id', { length: 255 }).notNull(),
      createdAt: timestamp('created_at').notNull(),
      updatedAt: timestamp('updated_at').notNull(),
    },
    (table) => [
      uniqueIndex(`ix_${tableName}_parent_execution_id`).on(table.parentExecutionId),
      index(`ix_${tableName}_updated_at`).on(table.updatedAt),
    ],
  )
}

/**
 * The type of the mysql table for task executions.
 */
export type TaskExecutionsMySqlTable = ReturnType<typeof createTaskExecutionsMySqlTable>

/**
 * The type of the mysql table for finished child task executions.
 */
export type FinishedChildTaskExecutionsMySqlTable = ReturnType<
  typeof createFinishedChildTaskExecutionsMySqlTable
>

/**
 * Create a mysql storage.
 *
 * @param db - The mysql database.
 * @param taskExecutionsTable - The mysql task executions table.
 * @param finishedChildTaskExecutionsTable - The mysql finished child task executions table.
 * @returns The mysql storage.
 */
export function createMySqlStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsMySqlTable,
  finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsMySqlTable,
  getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
): Storage {
  return new MySqlStorage(
    db,
    taskExecutionsTable,
    finishedChildTaskExecutionsTable,
    getAffectedRowsCount,
  )
}

class MySqlStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements Storage
{
  private readonly db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsMySqlTable
  private readonly finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsMySqlTable
  private readonly getAffectedRowsCount: (
    result: MySqlQueryResultKind<TQueryResult, never>,
  ) => number

  constructor(
    db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsMySqlTable,
    finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsMySqlTable,
    getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
    this.finishedChildTaskExecutionsTable = finishedChildTaskExecutionsTable
    this.getAffectedRowsCount = getAffectedRowsCount
  }

  async withTransaction<T>(
    fn: (tx: MySqlStorageTx<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>) => Promise<T>,
  ): Promise<T> {
    return await this.db.transaction(async (tx) => {
      const storageTx = new MySqlStorageTx(
        tx,
        this.taskExecutionsTable,
        this.finishedChildTaskExecutionsTable,
        this.getAffectedRowsCount,
      )
      return await fn(storageTx)
    })
  }

  async insertTaskExecutions(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => taskExecutionStorageValueToInsertValue(execution))
    await this.db.insert(this.taskExecutionsTable).values(rows)
  }

  async getTaskExecutions(
    where: TaskExecutionStorageWhere,
    limit?: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const query = this.db
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
    return await this.withTransaction(async (tx) => {
      return await tx.updateTaskExecutionsAndReturn(where, update, limit)
    })
  }

  async updateAllTaskExecutions(
    where: TaskExecutionStorageWhere,
    update: TaskExecutionStorageUpdate,
  ): Promise<number> {
    const result = await this.db
      .update(this.taskExecutionsTable)
      .set(taskExecutionStorageValueToUpdateValue(update))
      .where(buildTaskExecutionWhereCondition(this.taskExecutionsTable, where))
    return this.getAffectedRowsCount(result)
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    await this.db.insert(this.finishedChildTaskExecutionsTable).values(finishedChildTaskExecution)
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    return await this.withTransaction(async (tx) => {
      return await tx.deleteFinishedChildTaskExecutionsAndReturn(limit)
    })
  }
}

class MySqlStorageTx<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> {
  private readonly tx: MySqlTransaction<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsMySqlTable
  private readonly finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsMySqlTable
  private readonly getAffectedRowsCount: (
    result: MySqlQueryResultKind<TQueryResult, never>,
  ) => number

  constructor(
    tx: MySqlTransaction<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsMySqlTable,
    finishedChildTaskExecutionsTable: FinishedChildTaskExecutionsMySqlTable,
    getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
  ) {
    this.tx = tx
    this.taskExecutionsTable = taskExecutionsTable
    this.finishedChildTaskExecutionsTable = finishedChildTaskExecutionsTable
    this.getAffectedRowsCount = getAffectedRowsCount
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
    const queryWithLimit = limit != null && limit > 0 ? query.limit(limit) : query
    const rows = await queryWithLimit.for('update', { skipLocked: true })

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
    const result = await this.tx
      .update(this.taskExecutionsTable)
      .set(taskExecutionStorageValueToUpdateValue(update))
      .where(buildTaskExecutionWhereCondition(this.taskExecutionsTable, where))
    return this.getAffectedRowsCount(result)
  }

  async insertFinishedChildTaskExecutionIfNotExists(
    finishedChildTaskExecution: FinishedChildTaskExecutionStorageValue,
  ): Promise<void> {
    await this.tx
      .insert(this.finishedChildTaskExecutionsTable)
      .values(finishedChildTaskExecution)
      .onDuplicateKeyUpdate({
        set: {
          parentExecutionId: finishedChildTaskExecution.parentExecutionId,
        },
      })
  }

  async deleteFinishedChildTaskExecutionsAndReturn(
    limit: number,
  ): Promise<Array<FinishedChildTaskExecutionStorageValue>> {
    const rows = await this.tx
      .select()
      .from(this.finishedChildTaskExecutionsTable)
      .orderBy(asc(this.finishedChildTaskExecutionsTable.updatedAt))
      .limit(limit)
      .for('update', { skipLocked: true })
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
  table: TaskExecutionsMySqlTable,
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
