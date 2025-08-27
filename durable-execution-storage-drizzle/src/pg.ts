import { and, asc, eq, inArray, lt, sql, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  bigint,
  boolean,
  index,
  integer,
  json,
  pgTable,
  text,
  uniqueIndex,
  type PgDatabase,
  type PgQueryResultHKT,
} from 'drizzle-orm/pg-core'
import {
  TaskExecutionsStorageWithBatching,
  type DurableExecutionErrorStorageValue,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionsStorageWithoutBatching,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from 'durable-execution'

import {
  taskExecutionDBValueToStorageValue,
  taskExecutionStorageUpdateToDBUpdate,
  taskExecutionStorageValueToDBValue,
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
      indexInParentChildren: integer('index_in_parent_children'),
      isOnlyChildOfParent: boolean('is_only_child_of_parent'),
      isFinalizeOfParent: boolean('is_finalize_of_parent'),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: json('retry_options').$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: bigint('sleep_ms_before_run', { mode: 'number' }).notNull(),
      timeoutMs: bigint('timeout_ms', { mode: 'number' }).notNull(),
      areChildrenSequential: boolean('are_children_sequential').notNull(),
      input: text('input').notNull(),
      executorId: text('executor_id'),
      isSleepingTask: boolean('is_sleeping_task').notNull(),
      sleepingTaskUniqueId: text('sleeping_task_unique_id'),
      status: text('status').$type<TaskExecutionStatus>().notNull(),
      isFinished: boolean('is_finished').notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: json('error').$type<DurableExecutionErrorStorageValue>(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: bigint('start_at', { mode: 'number' }).notNull(),
      startedAt: bigint('started_at', { mode: 'number' }),
      expiresAt: bigint('expires_at', { mode: 'number' }),
      finishedAt: bigint('finished_at', { mode: 'number' }),
      children: json('children').$type<Array<TaskExecutionSummary>>(),
      activeChildrenCount: integer('active_children_count').notNull(),
      onChildrenFinishedProcessingStatus: text('on_children_finished_processing_status')
        .$type<TaskExecutionOnChildrenFinishedProcessingStatus>()
        .notNull(),
      onChildrenFinishedProcessingExpiresAt: bigint('on_children_finished_processing_expires_at', {
        mode: 'number',
      }),
      onChildrenFinishedProcessingFinishedAt: bigint(
        'on_children_finished_processing_finished_at',
        { mode: 'number' },
      ),
      finalize: json('finalize').$type<TaskExecutionSummary>(),
      closeStatus: text('close_status').$type<TaskExecutionCloseStatus>().notNull(),
      closeExpiresAt: bigint('close_expires_at', { mode: 'number' }),
      closedAt: bigint('closed_at', { mode: 'number' }),
      needsPromiseCancellation: boolean('needs_promise_cancellation').notNull(),
      createdAt: bigint('created_at', { mode: 'number' }).notNull(),
      updatedAt: bigint('updated_at', { mode: 'number' }).notNull(),
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
 * @param options - The options for the storage.
 * @param options.enableTestMode - Whether to enable test mode. Defaults to false.
 * @returns A TaskExecutionsStorage implementation for use with DurableExecutor.
 */
export function createPgTaskExecutionsStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsPgTable,
  options: {
    enableTestMode?: boolean
  } = {},
): TaskExecutionsStorage {
  return new TaskExecutionsStorageWithBatching(
    new PgTaskExecutionsStorage(db, taskExecutionsTable, options),
  )
}

class PgTaskExecutionsStorage<
  TQueryResult extends PgQueryResultHKT,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements TaskExecutionsStorageWithoutBatching
{
  private readonly db: PgDatabase<TQueryResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsPgTable
  private readonly enableTestMode: boolean

  constructor(
    db: PgDatabase<TQueryResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsPgTable,
    {
      enableTestMode = false,
    }: {
      enableTestMode?: boolean
    } = {},
  ) {
    this.db = db
    this.taskExecutionsTable = taskExecutionsTable
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

  async updateByIdAndInsertChildrenIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    if (childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById(executionId, filters, update)
    }

    const rowsToInsert = childrenTaskExecutionsToInsertIfAnyUpdated.map((execution) =>
      taskExecutionStorageValueToDBValue(execution),
    )
    await this.db.transaction(async (tx) => {
      const updateResult = await tx
        .update(this.taskExecutionsTable)
        .set(dbUpdate)
        .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
        .returning({ executionId: this.taskExecutionsTable.executionId })

      if (updateResult.length > 0) {
        await tx.insert(this.taskExecutionsTable).values(rowsToInsert)
      }
    })
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt: number,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(async (tx) => {
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
            expiresAt: sql`(${updateExpiresAtWithStartedAt} + timeout_ms)`,
          })
          .where(
            inArray(
              this.taskExecutionsTable.executionId,
              rows.map((row) => row.executionId),
            ),
          )
      }
      return rows
    })
    return updatedRows.map((row) =>
      taskExecutionDBValueToStorageValue(row, update, updateExpiresAtWithStartedAt),
    )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(async (tx) => {
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
            eq(this.taskExecutionsTable.activeChildrenCount, 0),
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
    })
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByCloseStatusAndReturn(
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(async (tx) => {
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
    })
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByIsSleepingTaskAndExpiresAtLessThan(
    isSleepingTask: boolean,
    expiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select({ executionId: this.taskExecutionsTable.executionId })
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
      return rows.length
    })
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(
    onChildrenFinishedProcessingExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select({ executionId: this.taskExecutionsTable.executionId })
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
      return rows.length
    })
  }

  async updateByCloseExpiresAtLessThan(
    closeExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select({ executionId: this.taskExecutionsTable.executionId })
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
      return rows.length
    })
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(async (tx) => {
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
    })
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

    return await this.db.transaction(async (tx) => {
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
          const values = parentIds.map(
            (parentId) =>
              sql`(${parentId}::text, ${parentExecutionIdToDecrementValueMap.get(parentId)}::integer)`,
          )

          await tx.execute(sql`
            UPDATE ${this.taskExecutionsTable}
            SET
              active_children_count = active_children_count - v.decrement_value,
              updated_at = ${dbUpdate.updatedAt}
            FROM (VALUES ${sql.join(values, sql`, `)}) AS v(parent_id, decrement_value)
            WHERE ${this.taskExecutionsTable}.execution_id = v.parent_id
          `)
        }
      }
      return rows.length
    })
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
  table: TaskExecutionsPgTable,
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
