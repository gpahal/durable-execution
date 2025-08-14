import { and, asc, eq, inArray, lt, sql, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
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
  TaskExecutionsStorageWithMutex,
  type DurableExecutionErrorStorageValue,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdsFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionSelectValueToStorageValue,
  taskExecutionStorageValueToInsertValue,
  taskExecutionStorageValueToUpdateValue,
} from './common'

/**
 * Creates a SQLite table schema for storing durable task executions.
 *
 * This function generates a Drizzle ORM table definition with all necessary columns and indexes
 * for the durable-execution storage layer. The table includes:
 * - Task hierarchy tracking (root, parent, child relationships)
 * - Execution state management (status, retries, timeouts)
 * - Input/output data storage with JSON support
 * - Timestamp tracking for scheduling and expiration
 *
 * Note: SQLite storage uses a mutex for write operations to prevent database
 * locking issues with concurrent writes.
 *
 * @example
 * ```ts
 * // In your schema file (e.g., schema.ts)
 * import { createSQLiteTaskExecutionsTable } from 'durable-execution-storage-drizzle'
 *
 * // Use default table name
 * export const taskExecutions = createSQLiteTaskExecutionsTable()
 *
 * // Or specify a custom table name
 * export const myTasksTable = createSQLiteTaskExecutionsTable('my_custom_task_executions')
 * ```
 *
 * @param tableName - The name of the table to create. Defaults to 'task_executions'.
 * @returns A Drizzle SQLite table definition ready for migrations.
 */
export function createSQLiteTaskExecutionsTable(tableName = 'task_executions') {
  return sqliteTable(
    tableName,
    {
      id: integer('id').primaryKey({ autoIncrement: true }),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      indexInParentChildTaskExecutions: integer('index_in_parent_child_task_executions'),
      isFinalizeTaskOfParentTask: integer('is_finalize_task_of_parent_task', {
        mode: 'boolean',
      }),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: text('retry_options', { mode: 'json' }).$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      input: text('input').notNull(),
      status: text('status').$type<TaskExecutionStatus>().notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: text('error', { mode: 'json' }).$type<DurableExecutionErrorStorageValue>(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: integer('start_at', { mode: 'timestamp' }).notNull(),
      startedAt: integer('started_at', { mode: 'timestamp' }),
      expiresAt: integer('expires_at', { mode: 'timestamp' }),
      finishedAt: integer('finished_at', { mode: 'timestamp' }),
      children: text('children', { mode: 'json' }).$type<Array<TaskExecutionSummary>>(),
      activeChildrenCount: integer('active_children_count').notNull(),
      onChildrenFinishedProcessingStatus: text('on_children_finished_processing_status')
        .$type<TaskExecutionOnChildrenFinishedProcessingStatus>()
        .notNull(),
      onChildrenFinishedProcessingExpiresAt: integer('on_children_finished_processing_expires_at', {
        mode: 'timestamp',
      }),
      onChildrenFinishedProcessingFinishedAt: integer(
        'on_children_finished_processing_finished_at',
        {
          mode: 'timestamp',
        },
      ),
      finalize: text('finalize', { mode: 'json' }).$type<TaskExecutionSummary>(),
      closeStatus: text('close_status').$type<TaskExecutionCloseStatus>().notNull(),
      closeExpiresAt: integer('close_expires_at', { mode: 'timestamp' }),
      closedAt: integer('closed_at', { mode: 'timestamp' }),
      needsPromiseCancellation: integer('needs_promise_cancellation', {
        mode: 'boolean',
      }).notNull(),
      createdAt: integer('created_at', { mode: 'timestamp' }).notNull(),
      updatedAt: integer('updated_at', { mode: 'timestamp' }).notNull(),
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
 * Type representing the SQLite table schema for task executions. Use this type when you need to
 * reference the table structure in your code.
 */
export type TaskExecutionsSQLiteTable = ReturnType<typeof createSQLiteTaskExecutionsTable>

/**
 * Creates a SQLite storage adapter for durable-execution.
 *
 * This function creates a storage implementation that uses SQLite as the backend for
 * persisting task execution state. The storage adapter handles:
 * - Mutex-based write synchronization to prevent database locking
 * - Transactional operations for consistency
 * - Efficient querying with proper indexes
 * - Parent-child task relationship management
 *
 * Important: SQLite has limitations with concurrent writes. This implementation uses a mutex to
 * serialize write operations, making it suitable for:
 * - Development and testing environments
 * - Single-instance applications
 * - Low to moderate throughput scenarios
 *
 * For high-concurrency production workloads, consider using PostgreSQL or MySQL.
 *
 * @example
 * ```ts
 * import { drizzle } from 'drizzle-orm/libsql'
 * import { createClient } from '@libsql/client'
 * import { DurableExecutor } from 'durable-execution'
 * import { createSQLiteTaskExecutionsTable, createSQLiteTaskExecutionsStorage } from 'durable-execution-storage-drizzle'
 *
 * // Setup database connection (local file)
 * const client = createClient({
 *   url: 'file:./local.db'
 * })
 * const db = drizzle(client)
 *
 * // Create table schema and storage
 * const taskExecutionsTable = createSQLiteTaskExecutionsTable()
 * const storage = createSQLiteTaskExecutionsStorage(db, taskExecutionsTable)
 *
 * // Use with DurableExecutor
 * const executor = new DurableExecutor(storage)
 * ```
 *
 * @example
 * ```ts
 * // Using Turso (LibSQL cloud)
 * import { createClient } from '@libsql/client'
 *
 * const client = createClient({
 *   url: process.env.TURSO_DATABASE_URL!,
 *   authToken: process.env.TURSO_AUTH_TOKEN!
 * })
 * const db = drizzle(client)
 *
 * const storage = createSQLiteTaskExecutionsStorage(db, taskExecutionsTable)
 * ```
 *
 * @param db - A Drizzle SQLite database instance.
 * @param taskExecutionsTable - The table created by `createSQLiteTaskExecutionsTable()`.
 * @returns A TaskExecutionsStorage implementation for use with DurableExecutor.
 */
export function createSQLiteTaskExecutionsStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsSQLiteTable,
): TaskExecutionsStorage {
  return new TaskExecutionsStorageWithMutex(
    new SQLiteTaskExecutionsStorageNonAtomic(db, taskExecutionsTable),
  )
}

class SQLiteTaskExecutionsStorageNonAtomic<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements TaskExecutionsStorage
{
  private readonly db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsSQLiteTable

  constructor(
    db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsSQLiteTable,
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
  }

  private async decrementParentActiveChildrenCount(
    tx: SQLiteTransaction<'async', TRunResult, TFullSchema, TSchema>,
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
            SELECT parent_execution_id
            FROM ${this.taskExecutionsTable}
            WHERE ${inArray(this.taskExecutionsTable.executionId, executionIds)}
            AND parent_execution_id IS NOT NULL
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
            .returning({ executionId: this.taskExecutionsTable.executionId })

          if (updateResult.length > 0) {
            await this.decrementParentActiveChildrenCount(
              tx,
              updateResult.map((row) => row.executionId),
              dbUpdate.updatedAt,
            )
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
        .returning({ executionId: this.taskExecutionsTable.executionId })

      if (updateResult.length > 0) {
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
          const updateResult = await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(and(...conditions))
            .returning({ executionId: this.taskExecutionsTable.executionId })

          if (updateResult.length > 0) {
            await this.decrementParentActiveChildrenCount(
              tx,
              updateResult.map((row) => row.executionId),
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
          const updateResult = await tx
            .update(this.taskExecutionsTable)
            .set(dbUpdate)
            .where(condition)
            .returning({ executionId: this.taskExecutionsTable.executionId })

          if (updateResult.length > 0) {
            await this.decrementParentActiveChildrenCount(
              tx,
              updateResult.map((row) => row.executionId),
              dbUpdate.updatedAt,
            )
          }
        }))
  }
}

function getByIdWhereCondition(
  table: TaskExecutionsSQLiteTable,
  executionId: string,
  filters: TaskExecutionStorageGetByIdsFilters,
): SQL | undefined {
  const conditions: Array<SQL> = [eq(table.executionId, executionId)]
  if (filters.statuses != null && filters.statuses.length > 0) {
    conditions.push(inArray(table.status, filters.statuses))
  }
  return and(...conditions)
}
