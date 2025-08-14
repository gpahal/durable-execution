import { and, asc, eq, inArray, lt, sql, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
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
 * Creates a PostgreSQL table schema for storing durable task executions.
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
 * import { createPgTaskExecutionsTable } from 'durable-execution-storage-drizzle'
 *
 * // Use default table name
 * export const taskExecutions = createPgTaskExecutionsTable()
 *
 * // Or specify a custom table name
 * export const myTasksTable = createPgTaskExecutionsTable('my_custom_task_executions')
 * ```
 *
 * @param tableName - The name of the table to create. Defaults to 'task_executions'.
 * @returns A Drizzle PostgreSQL table definition ready for migrations.
 */
export function createPgTaskExecutionsTable(tableName = 'task_executions') {
  return pgTable(
    tableName,
    {
      id: bigint('id', { mode: 'number' }).primaryKey().generatedAlwaysAsIdentity(),
      rootTaskId: text('root_task_id'),
      rootExecutionId: text('root_execution_id'),
      parentTaskId: text('parent_task_id'),
      parentExecutionId: text('parent_execution_id'),
      indexInParentChildTaskExecutions: integer('index_in_parent_child_task_executions'),
      isFinalizeTaskOfParentTask: boolean('is_finalize_task_of_parent_task'),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: bigint('sleep_ms_before_run', { mode: 'number' }).notNull(),
      timeoutMs: bigint('timeout_ms', { mode: 'number' }).notNull(),
      input: text('input').notNull(),
      status: text('status').$type<TaskExecutionStatus>().notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: json('error').$type<DurableExecutionErrorStorageValue>(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: timestamp('start_at', { withTimezone: true }).notNull(),
      startedAt: timestamp('started_at', { withTimezone: true }),
      expiresAt: timestamp('expires_at', { withTimezone: true }),
      finishedAt: timestamp('finished_at', { withTimezone: true }),
      children: json('children').$type<Array<TaskExecutionSummary>>(),
      activeChildrenCount: integer('active_children_count').notNull(),
      onChildrenFinishedProcessingStatus: text('on_children_finished_processing_status')
        .$type<TaskExecutionOnChildrenFinishedProcessingStatus>()
        .notNull(),
      onChildrenFinishedProcessingExpiresAt: timestamp(
        'on_children_finished_processing_expires_at',
        { withTimezone: true },
      ),
      onChildrenFinishedProcessingFinishedAt: timestamp(
        'on_children_finished_processing_finished_at',
        { withTimezone: true },
      ),
      finalize: json('finalize').$type<TaskExecutionSummary>(),
      closeStatus: text('close_status').$type<TaskExecutionCloseStatus>().notNull(),
      closeExpiresAt: timestamp('close_expires_at', { withTimezone: true }),
      closedAt: timestamp('closed_at', { withTimezone: true }),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      createdAt: timestamp('created_at', { withTimezone: true }).notNull(),
      updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
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
 * Type representing the PostgreSQL table schema for task executions. Use this type when you need
 * to reference the table structure in your code.
 */
export type TaskExecutionsPgTable = ReturnType<typeof createPgTaskExecutionsTable>

/**
 * Creates a PostgreSQL storage adapter for durable-execution.
 *
 * This function creates a storage implementation that uses PostgreSQL as the backend for
 * persisting task execution state. The storage adapter handles:
 * - Transactional operations for consistency
 * - Optimistic locking with 'FOR UPDATE SKIP LOCKED' for concurrent access
 * - Efficient querying with proper indexes
 * - Parent-child task relationship management
 *
 * @example
 * ```ts
 * import { drizzle } from 'drizzle-orm/node-postgres'
 * import { Pool } from 'pg'
 * import { DurableExecutor } from 'durable-execution'
 * import { createPgTaskExecutionsTable, createPgTaskExecutionsStorage } from 'durable-execution-storage-drizzle'
 *
 * // Setup database connection
 * const pool = new Pool({ connectionString: process.env.DATABASE_URL })
 * const db = drizzle(pool)
 *
 * // Create table schema and storage
 * const taskExecutionsTable = createPgTaskExecutionsTable()
 * const storage = createPgTaskExecutionsStorage(db, taskExecutionsTable)
 *
 * // Use with DurableExecutor
 * const executor = new DurableExecutor(storage)
 * ```
 *
 * @param db - A Drizzle PostgreSQL database instance.
 * @param taskExecutionsTable - The table created by `createPgTaskExecutionsTable()`.
 * @returns A TaskExecutionsStorage implementation for use with DurableExecutor.
 */
export function createPgTaskExecutionsStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsPgTable,
): TaskExecutionsStorage {
  return new PgTaskExecutionsStorage(db, taskExecutionsTable)
}

class PgTaskExecutionsStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements TaskExecutionsStorage
{
  private readonly db: PgDatabase<TQueryResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsPgTable

  constructor(
    db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsPgTable,
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
  }

  private async decrementParentActiveChildrenCount(
    tx: PgTransaction<TQueryResult, TFullSchema, TSchema>,
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
  table: TaskExecutionsPgTable,
  executionId: string,
  filters: TaskExecutionStorageGetByIdsFilters,
): SQL | undefined {
  const conditions: Array<SQL> = [eq(table.executionId, executionId)]
  if (filters.statuses != null && filters.statuses.length > 0) {
    conditions.push(inArray(table.status, filters.statuses))
  }
  return and(...conditions)
}
