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
  type MySqlTransaction,
  type PreparedQueryHKTBase,
} from 'drizzle-orm/mysql-core'
import type {
  DurableExecutionErrorStorageValue,
  TaskExecutionCloseStatus,
  TaskExecutionOnChildrenFinishedProcessingStatus,
  TaskExecutionsStorage,
  TaskExecutionStatus,
  TaskExecutionStorageGetByIdsFilters,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskExecutionSummary,
  TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionSelectValueToStorageValue,
  taskExecutionStorageValueToInsertValue,
  taskExecutionStorageValueToUpdateValue,
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
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: bigint('sleep_ms_before_run', { mode: 'number' }).notNull(),
      timeoutMs: bigint('timeout_ms', { mode: 'number' }).notNull(),
      input: longtext('input').notNull(),
      status: varchar('status', { length: 64 }).$type<TaskExecutionStatus>().notNull(),
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
      index(`ix_${tableName}_execution_id_status`).on(table.executionId, table.status),
      index(`ix_${tableName}_status_start_at`).on(table.status, table.startAt),
      index(`ix_${tableName}_status_ocfp_status_acc_updated_at`).on(
        table.status,
        table.onChildrenFinishedProcessingStatus,
        table.activeChildrenCount,
        table.updatedAt,
      ),
      index(`ix_${tableName}_status_close_status_updated_at`).on(
        table.status,
        table.closeStatus,
        table.updatedAt,
      ),
      index(`ix_${tableName}_expires_at`).on(table.expiresAt),
      index(`ix_${tableName}_on_ocfp_expires_at`).on(table.onChildrenFinishedProcessingExpiresAt),
      index(`ix_${tableName}_close_expires_at`).on(table.closeExpiresAt),
      index(`ix_${tableName}_npc_execution_id_updated_at`).on(
        table.needsPromiseCancellation,
        table.executionId,
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
): TaskExecutionsStorage {
  return new MySqlTaskExecutionsStorage(db, taskExecutionsTable, getAffectedRowsCount)
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

  constructor(
    db: MySqlDatabase<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsMySqlTable,
    getAffectedRowsCount: (result: MySqlQueryResultKind<TQueryResult, never>) => number,
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
    this.getAffectedRowsCount = getAffectedRowsCount
  }

  private async decrementParentActiveChildrenCount(
    tx: MySqlTransaction<TQueryResult, TPreparedQueryHKT, TFullSchema, TSchema>,
    executionIds: Array<string>,
    updatedAt: Date,
  ): Promise<void> {
    if (executionIds.length === 0) {
      return
    }

    await tx
      .update(this.taskExecutionsTable)
      .set({
        activeChildrenCount: sql`${this.taskExecutionsTable.activeChildrenCount} - 1`,
        updatedAt,
      })
      .where(
        inArray(
          this.taskExecutionsTable.executionId,
          sql`(
            SELECT parent_execution_id FROM (
              SELECT parent_execution_id
              FROM ${this.taskExecutionsTable}
              WHERE ${inArray(this.taskExecutionsTable.executionId, executionIds)}
              AND parent_execution_id IS NOT NULL
            ) AS temp_table
          )`,
        ),
      )
  }

  async insert(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }
    const rows = executions.map((execution) => taskExecutionStorageValueToInsertValue(execution))
    await this.db.insert(this.taskExecutionsTable).values(rows)
  }

  async getByIds(
    executionIds: Array<string>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(inArray(this.taskExecutionsTable.executionId, executionIds))
    const rowsMap = new Map<string, TaskExecutionStorageValue>()
    for (const row of rows) {
      rowsMap.set(row.executionId, taskExecutionSelectValueToStorageValue(row))
    }
    return executionIds.map((executionId) => rowsMap.get(executionId))
  }

  async getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
      .limit(1)
    return rows.length > 0 ? taskExecutionSelectValueToStorageValue(rows[0]!) : undefined
  }

  async updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    const { dbUpdate, decrementParentActiveChildrenCount } =
      taskExecutionStorageValueToUpdateValue(update)

    await (!decrementParentActiveChildrenCount
      ? this.db
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
      : this.db.transaction(async (tx) => {
          const updateResult = await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))

          const affectedRowsCount = this.getAffectedRowsCount(updateResult)
          if (affectedRowsCount > 0) {
            await this.decrementParentActiveChildrenCount(tx, [executionId], dbUpdate.updatedAt)
          }
        }))
  }

  async updateByIdAndInsertIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdsFilters,
    update: TaskExecutionStorageUpdate,
    executionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    const { dbUpdate, decrementParentActiveChildrenCount } =
      taskExecutionStorageValueToUpdateValue(update)

    await this.db.transaction(async (tx) => {
      const updateResult = await tx
        .update(this.taskExecutionsTable)
        .set(dbUpdate)
        .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))

      const affectedRowsCount = this.getAffectedRowsCount(updateResult)
      if (affectedRowsCount > 0) {
        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(tx, [executionId], dbUpdate.updatedAt)
        }

        if (executionsToInsertIfAnyUpdated.length > 0) {
          const rows = executionsToInsertIfAnyUpdated.map((execution) =>
            taskExecutionStorageValueToInsertValue(execution),
          )
          await tx.insert(this.taskExecutionsTable).values(rows)
        }
      }
    })
  }

  async updateByIdsAndStatuses(
    executionIds: Array<string>,
    statuses: Array<TaskExecutionStatus>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    if (executionIds.length === 0) {
      return
    }

    const { dbUpdate, decrementParentActiveChildrenCount } =
      taskExecutionStorageValueToUpdateValue(update)
    const conditions = [inArray(this.taskExecutionsTable.executionId, executionIds)]
    if (statuses.length > 0) {
      conditions.push(inArray(this.taskExecutionsTable.status, statuses))
    }

    await (!decrementParentActiveChildrenCount
      ? this.db
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(and(...conditions))
      : this.db.transaction(async (tx) => {
          const rowsToUpdate = await tx
            .select({ executionId: this.taskExecutionsTable.executionId })
            .from(this.taskExecutionsTable)
            .where(and(...conditions))

          if (rowsToUpdate.length > 0) {
            await tx
              .update(this.taskExecutionsTable)
              .set(dbUpdate)
              .where(and(...conditions))

            await this.decrementParentActiveChildrenCount(
              tx,
              rowsToUpdate.map((row) => row.executionId),
              dbUpdate.updatedAt,
            )
          }
        }))
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const { dbUpdate, decrementParentActiveChildrenCount } =
      taskExecutionStorageValueToUpdateValue(update)

    return await this.db.transaction(async (tx) => {
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
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    activeChildrenCountLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.db.transaction(async (tx) => {
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
        const { dbUpdate, decrementParentActiveChildrenCount } =
          taskExecutionStorageValueToUpdateValue(update)

        await tx
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async updateByStatusesAndCloseStatusAndReturn(
    statuses: Array<TaskExecutionStatus>,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(this.taskExecutionsTable)
        .where(
          and(
            inArray(this.taskExecutionsTable.status, statuses),
            eq(this.taskExecutionsTable.closeStatus, closeStatus),
          ),
        )
        .orderBy(asc(this.taskExecutionsTable.updatedAt))
        .limit(limit)
        .for('update', { skipLocked: true })

      if (rows.length > 0) {
        const { dbUpdate, decrementParentActiveChildrenCount } =
          taskExecutionStorageValueToUpdateValue(update)

        await tx
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async updateByExpiresAtLessThanAndReturn(
    expiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(this.taskExecutionsTable)
        .where(lt(this.taskExecutionsTable.expiresAt, expiresAtLessThan))
        .orderBy(asc(this.taskExecutionsTable.expiresAt))
        .limit(limit)
        .for('update', { skipLocked: true })

      if (rows.length > 0) {
        const { dbUpdate, decrementParentActiveChildrenCount } =
          taskExecutionStorageValueToUpdateValue(update)

        await tx
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
    onChildrenFinishedProcessingExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.db.transaction(async (tx) => {
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
        const { dbUpdate, decrementParentActiveChildrenCount } =
          taskExecutionStorageValueToUpdateValue(update)

        await tx
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async updateByCloseExpiresAtLessThanAndReturn(
    closeExpiresAtLessThan: Date,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(this.taskExecutionsTable)
        .where(lt(this.taskExecutionsTable.closeExpiresAt, closeExpiresAtLessThan))
        .orderBy(asc(this.taskExecutionsTable.closeExpiresAt))
        .limit(limit)
        .for('update', { skipLocked: true })

      if (rows.length > 0) {
        const { dbUpdate, decrementParentActiveChildrenCount } =
          taskExecutionStorageValueToUpdateValue(update)

        await tx
          .update(this.taskExecutionsTable)
          .set(dbUpdate)
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )

        if (decrementParentActiveChildrenCount) {
          await this.decrementParentActiveChildrenCount(
            tx,
            rows.map((row) => row.executionId),
            dbUpdate.updatedAt,
          )
        }
      }

      return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
    })
  }

  async getByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    if (executionIds.length === 0) {
      return []
    }

    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(
        and(
          eq(this.taskExecutionsTable.needsPromiseCancellation, needsPromiseCancellation),
          inArray(this.taskExecutionsTable.executionId, executionIds),
        ),
      )
      .orderBy(asc(this.taskExecutionsTable.updatedAt))
      .limit(limit)

    return rows.map((row) => taskExecutionSelectValueToStorageValue(row))
  }

  async updateByNeedsPromiseCancellationAndIds(
    needsPromiseCancellation: boolean,
    executionIds: Array<string>,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    if (executionIds.length === 0) {
      return
    }

    const { dbUpdate, decrementParentActiveChildrenCount } =
      taskExecutionStorageValueToUpdateValue(update)
    const condition = and(
      eq(this.taskExecutionsTable.needsPromiseCancellation, needsPromiseCancellation),
      inArray(this.taskExecutionsTable.executionId, executionIds),
    )

    await (!decrementParentActiveChildrenCount
      ? this.db.update(this.taskExecutionsTable).set(dbUpdate).where(condition)
      : this.db.transaction(async (tx) => {
          const rowsToUpdate = await tx
            .select({ executionId: this.taskExecutionsTable.executionId })
            .from(this.taskExecutionsTable)
            .where(condition)

          if (rowsToUpdate.length > 0) {
            await tx.update(this.taskExecutionsTable).set(dbUpdate).where(condition)

            await this.decrementParentActiveChildrenCount(
              tx,
              rowsToUpdate.map((row) => row.executionId),
              dbUpdate.updatedAt,
            )
          }
        }))
  }
}

function getByIdWhereCondition(
  table: TaskExecutionsMySqlTable,
  executionId: string,
  filters: TaskExecutionStorageGetByIdsFilters,
): SQL | undefined {
  const conditions: Array<SQL> = [eq(table.executionId, executionId)]
  if (filters.statuses != null && filters.statuses.length > 0) {
    conditions.push(inArray(table.status, filters.statuses))
  }
  return and(...conditions)
}
