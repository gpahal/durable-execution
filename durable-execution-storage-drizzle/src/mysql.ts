import { and, asc, eq, inArray, lt, sql, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  bigint,
  boolean,
  index,
  int,
  json,
  longtext,
  mysqlTable,
  serial,
  timestamp,
  uniqueIndex,
  varchar,
  type MySqlDatabase,
  type MySqlQueryResultHKT,
  type MySqlQueryResultKind,
  type PreparedQueryHKTBase,
} from 'drizzle-orm/mysql-core'
import type {
  DurableExecutionErrorStorageValue,
  TaskExecutionCloseStatus,
  TaskExecutionOnChildrenFinishedProcessingStatus,
  TaskExecutionsStorage,
  TaskExecutionStatus,
  TaskExecutionStorageGetByIdFilters,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskExecutionSummary,
  TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionDBValueToStorageValue,
  taskExecutionStorageUpdateToDBUpdate,
  taskExecutionStorageValueToDBValue,
} from './common'

/**
 * Creates a MySQL table schema for storing durable task executions.
 *
 * This function generates a Drizzle ORM table definition with all necessary columns and indexes
 * for the durable-execution storage layer. The table includes:
 * - Task hierarchy tracking (root, parent, child relationships)
 * - Execution state management (status, retries, timeouts)
 * - Input/output data storage
 * - Timestamp tracking for scheduling and expiration
 *
 * @example
 * ```ts
 * // In your schema file (e.g., schema.ts)
 * import { createMySqlTaskExecutionsTable } from 'durable-execution-storage-drizzle'
 *
 * // Use default table name
 * export const taskExecutions = createMySqlTaskExecutionsTable()
 *
 * // Or specify a custom table name
 * export const myTasksTable = createMySqlTaskExecutionsTable('my_custom_task_executions')
 * ```
 *
 * @param tableName - The name of the table to create. Defaults to 'task_executions'.
 * @returns A Drizzle MySQL table definition ready for migrations.
 */
export function createMySqlTaskExecutionsTable(tableName = 'task_executions') {
  return mysqlTable(
    tableName,
    {
      id: serial('id').primaryKey(),
      rootTaskId: varchar('root_task_id', { length: 64 }),
      rootExecutionId: varchar('root_execution_id', { length: 64 }),
      parentTaskId: varchar('parent_task_id', { length: 64 }),
      parentExecutionId: varchar('parent_execution_id', { length: 64 }),
      indexInParentChildTaskExecutions: int('index_in_parent_child_task_executions'),
      isFinalizeTaskOfParentTask: boolean('is_finalize_task_of_parent_task'),
      taskId: varchar('task_id', { length: 64 }).notNull(),
      executionId: varchar('execution_id', { length: 64 }).notNull(),
      isSleepingTask: boolean('is_sleeping_task').notNull(),
      sleepingTaskUniqueId: varchar('sleeping_task_unique_id', { length: 255 }),
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: bigint('sleep_ms_before_run', { mode: 'number' }).notNull(),
      timeoutMs: bigint('timeout_ms', { mode: 'number' }).notNull(),
      input: longtext('input').notNull(),
      executorId: varchar('executor_id', { length: 64 }),
      status: varchar('status', { length: 64 }).$type<TaskExecutionStatus>().notNull(),
      isFinished: boolean('is_finished').notNull(),
      runOutput: longtext('run_output'),
      output: longtext('output'),
      error: json('error').$type<DurableExecutionErrorStorageValue>(),
      retryAttempts: int('retry_attempts').notNull(),
      startAt: timestamp('start_at').notNull(),
      startedAt: timestamp('started_at'),
      expiresAt: timestamp('expires_at'),
      finishedAt: timestamp('finished_at'),
      children: json('children').$type<Array<TaskExecutionSummary>>(),
      activeChildrenCount: int('active_children_count').notNull(),
      onChildrenFinishedProcessingStatus: varchar('on_children_finished_processing_status', {
        length: 64,
      })
        .$type<TaskExecutionOnChildrenFinishedProcessingStatus>()
        .notNull(),
      onChildrenFinishedProcessingExpiresAt: timestamp(
        'on_children_finished_processing_expires_at',
      ),
      onChildrenFinishedProcessingFinishedAt: timestamp(
        'on_children_finished_processing_finished_at',
      ),
      finalize: json('finalize').$type<TaskExecutionSummary>(),
      closeStatus: varchar('close_status', { length: 64 })
        .$type<TaskExecutionCloseStatus>()
        .notNull(),
      closeExpiresAt: timestamp('close_expires_at'),
      closedAt: timestamp('closed_at'),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      createdAt: timestamp('created_at').notNull(),
      updatedAt: timestamp('updated_at').notNull(),
    },
    (table) => [
      uniqueIndex(`ix_${tableName}_execution_id`).on(table.executionId),
      uniqueIndex(`ix_${tableName}_sleeping_task_unique_id`).on(table.sleepingTaskUniqueId),
      index(`ix_${tableName}_status_start_at`).on(table.status, table.startAt),
      index(`ix_${tableName}_status_ocfp_status_acc_updated_at`).on(
        table.status,
        table.onChildrenFinishedProcessingStatus,
        table.activeChildrenCount,
        table.updatedAt,
      ),
      index(`ix_${tableName}_close_status_updated_at`).on(table.closeStatus, table.updatedAt),
      index(`ix_${tableName}_is_sleeping_task_expires_at`).on(
        table.isSleepingTask,
        table.expiresAt,
      ),
      index(`ix_${tableName}_on_ocfp_expires_at`).on(table.onChildrenFinishedProcessingExpiresAt),
      index(`ix_${tableName}_close_expires_at`).on(table.closeExpiresAt),
      index(`ix_${tableName}_executor_id_npc_updated_at`).on(
        table.executorId,
        table.needsPromiseCancellation,
        table.updatedAt,
      ),
      index(`ix_${tableName}_parent_execution_id_is_finished`).on(
        table.parentExecutionId,
        table.isFinished,
      ),
      index(`ix_${tableName}_is_finished_close_status_updated_at`).on(
        table.isFinished,
        table.closeStatus,
        table.updatedAt,
      ),
    ],
  )
}

/**
 * Type representing the MySQL table schema for task executions. Use this type when you need to
 * reference the table structure in your code.
 */
export type TaskExecutionsMySqlTable = ReturnType<typeof createMySqlTaskExecutionsTable>

/**
 * Creates a MySQL storage adapter for durable-execution.
 *
 * This function creates a storage implementation that uses MySQL as the backend for persisting
 * task execution state. The storage adapter handles:
 * - Transactional operations for consistency
 * - Row-level locking with 'FOR UPDATE SKIP LOCKED' for concurrent access
 * - Efficient querying with proper indexes
 * - Parent-child task relationship management
 *
 * @example
 * ```ts
 * import { drizzle } from 'drizzle-orm/mysql2'
 * import mysql from 'mysql2/promise'
 * import { DurableExecutor } from 'durable-execution'
 * import { createMySqlTaskExecutionsTable, createMySqlTaskExecutionsStorage } from 'durable-execution-storage-drizzle'
 *
 * // Setup database connection
 * const connection = await mysql.createConnection({
 *   uri: process.env.DATABASE_URL
 * })
 * const db = drizzle(connection)
 *
 * // Create table schema and storage
 * const taskExecutionsTable = createMySqlTaskExecutionsTable()
 * const storage = createMySqlTaskExecutionsStorage(
 *   db,
 *   taskExecutionsTable,
 *   (result) => result[0].affectedRows // mysql2 driver
 * )
 *
 * // Use with DurableExecutor
 * const executor = new DurableExecutor(storage)
 * ```
 *
 * @example
 * ```ts
 * // For PlanetScale driver
 * import { drizzle } from 'drizzle-orm/planetscale-serverless'
 * import { Client } from '@planetscale/database'
 *
 * const client = new Client({
 *   url: process.env.DATABASE_URL
 * })
 * const db = drizzle(client)
 *
 * const storage = createMySqlTaskExecutionsStorage(
 *   db,
 *   taskExecutionsTable,
 *   (result) => result.rowsAffected // PlanetScale driver
 * )
 * ```
 *
 * @param db - A Drizzle MySQL database instance.
 * @param taskExecutionsTable - The table created by `createMySqlTaskExecutionsTable()`.
 * @param getAffectedRowsCount - A function to extract affected rows count from MySQL query
 *   results. This varies based on your MySQL driver (mysql2, planetscale, etc.).
 * @param options - The options for the storage.
 * @param options.enableTestMode - Whether to enable test mode. Defaults to false.
 * @returns A TaskExecutionsStorage implementation for use with DurableExecutor.
 */
export function createMySqlTaskExecutionsStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsMySqlTable,
  getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
  options: {
    enableTestMode?: boolean
  } = {},
): TaskExecutionsStorage {
  return new MySqlTaskExecutionsStorage(db, taskExecutionsTable, getAffectedRowsCount, options)
}

class MySqlTaskExecutionsStorage<
  TQueryResult extends MySqlQueryResultHKT,
  TPreparedQueryHKT extends PreparedQueryHKTBase,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements TaskExecutionsStorage
{
  private readonly db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsMySqlTable
  private readonly getAffectedRowsCount: (
    result: MySqlQueryResultKind<TQueryResult, never>,
  ) => number
  private readonly enableTestMode: boolean

  constructor(
    db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsMySqlTable,
    getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
    {
      enableTestMode = false,
    }: {
      enableTestMode?: boolean
    } = {},
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
    this.getAffectedRowsCount = getAffectedRowsCount
    this.enableTestMode = enableTestMode
  }

  async insertMany(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }

    const rows = executions.map((execution) => taskExecutionStorageValueToDBValue(execution))
    await this.db.insert(this.taskExecutionsTable).values(rows)
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
      .limit(1)
    return rows.length > 0 ? taskExecutionDBValueToStorageValue(rows[0]!) : undefined
  }

  async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(eq(this.taskExecutionsTable.sleepingTaskUniqueId, sleepingTaskUniqueId))
      .limit(1)
    return rows.length > 0 ? taskExecutionDBValueToStorageValue(rows[0]!) : undefined
  }

  async updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    await this.db
      .update(this.taskExecutionsTable)
      .set(dbUpdate)
      .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
  }

  async updateByIdAndInsertManyIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    if (executionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(executionId, filters, update)
    }

    const rowsToInsert = executionsToInsertIfAnyUpdated.map((execution) =>
      taskExecutionStorageValueToDBValue(execution),
    )
    await this.db.transaction(async (tx) => {
      const updateResult = await tx
        .update(this.taskExecutionsTable)
        .set(dbUpdate)
        .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))

      const affectedRowsCount = this.getAffectedRowsCount(updateResult)
      if (affectedRowsCount > 0) {
        await tx.insert(this.taskExecutionsTable).values(rowsToInsert)
      }
    })
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt: Date,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            and(
              eq(this.taskExecutionsTable.status, status),
              lt(this.taskExecutionsTable.startAt, startAtLessThan),
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.startAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set({
              ...dbUpdate,
              expiresAt: sql`FROM_UNIXTIME((${updateExpiresAtWithStartedAt.getTime()} + timeout_ms) / 1000.0)`,
            })
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) =>
      taskExecutionDBValueToStorageValue(row, update, updateExpiresAtWithStartedAt),
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            and(
              eq(this.taskExecutionsTable.status, status),
              eq(
                this.taskExecutionsTable.onChildrenFinishedProcessingStatus,
                onChildrenFinishedProcessingStatus,
              ),
              lt(this.taskExecutionsTable.activeChildrenCount, activeChildrenCountLessThan),
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.updatedAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByCloseStatusAndReturn(
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(eq(this.taskExecutionsTable.closeStatus, closeStatus))
          .orderBy(asc(this.taskExecutionsTable.updatedAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByIsSleepingTaskAndExpiresAtLessThanAndReturn(
    isSleepingTask: boolean,
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            and(
              eq(this.taskExecutionsTable.isSleepingTask, isSleepingTask),
              lt(this.taskExecutionsTable.expiresAt, expiresAtLessThan),
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.expiresAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            lt(
              this.taskExecutionsTable.onChildrenFinishedProcessingExpiresAt,
              onChildrenFinishedProcessingExpiresAtLessThan,
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.onChildrenFinishedProcessingExpiresAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByCloseExpiresAtLessThanAndReturn(
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(lt(this.taskExecutionsTable.closeExpiresAt, closeExpiresAtLessThan))
          .orderBy(asc(this.taskExecutionsTable.closeExpiresAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            and(
              eq(this.taskExecutionsTable.executorId, executorId),
              eq(this.taskExecutionsTable.needsPromiseCancellation, needsPromiseCancellation),
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.updatedAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )
        }
        return rows
      },
      {
        isolationLevel: 'read committed',
      },
    )
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async getByParentExecutionId(
    parentExecutionId: string,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(eq(this.taskExecutionsTable.parentExecutionId, parentExecutionId))
    return rows.map((row) => taskExecutionDBValueToStorageValue(row))
  }

  async updateByParentExecutionIdAndIsFinished(
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    await this.db
      .update(this.taskExecutionsTable)
      .set(dbUpdate)
      .where(
        and(
          eq(this.taskExecutionsTable.parentExecutionId, parentExecutionId),
          eq(this.taskExecutionsTable.isFinished, isFinished),
        ),
      )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
    isFinished: boolean,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(
      async (tx) => {
        const rows = await tx
          .select()
          .from(this.taskExecutionsTable)
          .where(
            and(
              eq(this.taskExecutionsTable.isFinished, isFinished),
              eq(this.taskExecutionsTable.closeStatus, closeStatus),
            ),
          )
          .orderBy(asc(this.taskExecutionsTable.updatedAt))
          .limit(limit)
          .for('update', { skipLocked: true })

        if (rows.length > 0) {
          await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(
              inArray(
                this.taskExecutionsTable.executionId,
                rows.map((row) => row.executionId),
              ),
            )

          const parentExecutionIdToDecrementValueMap = new Map<string, number>()
          for (const row of rows) {
            if (row.parentExecutionId != null) {
              parentExecutionIdToDecrementValueMap.set(
                row.parentExecutionId,
                (parentExecutionIdToDecrementValueMap.get(row.parentExecutionId) ?? 0) + 1,
              )
            }
          }

          if (parentExecutionIdToDecrementValueMap.size > 0) {
            const parentIds = [...parentExecutionIdToDecrementValueMap.keys()].sort()
            const caseStatements = parentIds.map(
              (parentId) =>
                sql`WHEN ${parentId} THEN ${parentExecutionIdToDecrementValueMap.get(parentId)}`,
            )

            await tx.execute(sql`
            UPDATE ${this.taskExecutionsTable}
            SET
              active_children_count = active_children_count - (
                CASE execution_id
                  ${sql.join(caseStatements, sql` `)}
                  ELSE 0
                END
              ),
              updated_at = ${dbUpdate.updatedAt}
            WHERE execution_id IN (${sql.join(parentIds, sql`, `)})
          `)
          }
        }
        return rows.length
      },
      {
        isolationLevel: 'read committed',
      },
    )
  }

  async deleteById(executionId: string): Promise<void> {
    if (!this.enableTestMode) {
      return
    }

    await this.db
      .delete(this.taskExecutionsTable)
      .where(eq(this.taskExecutionsTable.executionId, executionId))
  }

  async deleteAll(): Promise<void> {
    if (!this.enableTestMode) {
      return
    }

    await this.db.delete(this.taskExecutionsTable)
  }
}

function getByIdWhereCondition(
  table: TaskExecutionsMySqlTable,
  executionId: string,
  filters: TaskExecutionStorageGetByIdFilters,
): SQL | undefined {
  const conditions: Array<SQL> = [eq(table.executionId, executionId)]
  if (filters.isSleepingTask != null) {
    conditions.push(eq(table.isSleepingTask, filters.isSleepingTask))
  }
  if (filters.status != null) {
    conditions.push(eq(table.status, filters.status))
  }
  if (filters.isFinished != null) {
    conditions.push(eq(table.isFinished, filters.isFinished))
  }
  return and(...conditions)
}
