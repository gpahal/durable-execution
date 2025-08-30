import { and, asc, eq, inArray, lt, sql, type SQL, type TablesRelationalConfig } from 'drizzle-orm'
import {
  index,
  integer,
  sqliteTable,
  text,
  uniqueIndex,
  type BaseSQLiteDatabase,
} from 'drizzle-orm/sqlite-core'
import {
  TaskExecutionsStorageWithBatching,
  TaskExecutionsStorageWithMutex,
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
      indexInParentChildren: integer('index_in_parent_children'),
      isOnlyChildOfParent: integer('is_only_child_of_parent', {
        mode: 'boolean',
      }),
      isFinalizeOfParent: integer('is_finalize_of_parent', {
        mode: 'boolean',
      }),
      taskId: text('task_id').notNull(),
      executionId: text('execution_id').notNull(),
      retryOptions: text('retry_options', { mode: 'json' }).$type<TaskRetryOptions>().notNull(),
      sleepMsBeforeRun: integer('sleep_ms_before_run').notNull(),
      timeoutMs: integer('timeout_ms').notNull(),
      areChildrenSequential: integer('are_children_sequential', {
        mode: 'boolean',
      }).notNull(),
      input: text('input').notNull(),
      executorId: text('executor_id'),
      isSleepingTask: integer('is_sleeping_task', { mode: 'boolean' }).notNull(),
      sleepingTaskUniqueId: text('sleeping_task_unique_id'),
      status: text('status').$type<TaskExecutionStatus>().notNull(),
      isFinished: integer('is_finished', { mode: 'boolean' }).notNull(),
      runOutput: text('run_output'),
      output: text('output'),
      error: text('error', { mode: 'json' }).$type<DurableExecutionErrorStorageValue>(),
      retryAttempts: integer('retry_attempts').notNull(),
      startAt: integer('start_at').notNull(),
      startedAt: integer('started_at'),
      expiresAt: integer('expires_at'),
      waitingForChildrenStartedAt: integer('waiting_for_children_started_at'),
      waitingForFinalizeStartedAt: integer('waiting_for_finalize_started_at'),
      finishedAt: integer('finished_at'),
      children: text('children', { mode: 'json' }).$type<Array<TaskExecutionSummary>>(),
      activeChildrenCount: integer('active_children_count').notNull(),
      onChildrenFinishedProcessingStatus: text('on_children_finished_processing_status')
        .$type<TaskExecutionOnChildrenFinishedProcessingStatus>()
        .notNull(),
      onChildrenFinishedProcessingExpiresAt: integer('on_children_finished_processing_expires_at'),
      onChildrenFinishedProcessingFinishedAt: integer(
        'on_children_finished_processing_finished_at',
      ),
      finalize: text('finalize', { mode: 'json' }).$type<TaskExecutionSummary>(),
      closeStatus: text('close_status').$type<TaskExecutionCloseStatus>().notNull(),
      closeExpiresAt: integer('close_expires_at'),
      closedAt: integer('closed_at'),
      needsPromiseCancellation: integer('needs_promise_cancellation', {
        mode: 'boolean',
      }).notNull(),
      createdAt: integer('created_at').notNull(),
      updatedAt: integer('updated_at').notNull(),
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
      index(`ix_${tableName}_status_is_sleeping_task_expires_at`).on(
        table.status,
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
 * @param options - The options for the storage.
 * @param options.enableTestMode - Whether to enable test mode. Defaults to false.
 * @returns A TaskExecutionsStorage implementation for use with DurableExecutor.
 */
export function createSQLiteTaskExecutionsStorage<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
>(
  db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
  taskExecutionsTable: TaskExecutionsSQLiteTable,
  options: {
    enableTestMode?: boolean
  } = {},
): TaskExecutionsStorage {
  return new TaskExecutionsStorageWithMutex(
    new TaskExecutionsStorageWithBatching(
      new SQLiteTaskExecutionsStorageNonAtomic(db, taskExecutionsTable, options),
      true,
    ),
  )
}

class SQLiteTaskExecutionsStorageNonAtomic<
  TRunResult,
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> implements TaskExecutionsStorageWithoutBatching
{
  private readonly db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>
  private readonly taskExecutionsTable: TaskExecutionsSQLiteTable
  private readonly enableTestMode: boolean

  constructor(
    db: BaseSQLiteDatabase<'async', TRunResult, TFullSchema, TSchema>,
    taskExecutionsTable: TaskExecutionsSQLiteTable,
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

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }

    const rows = executions.map((execution) => taskExecutionStorageValueToDBValue(execution))
    await this.db.insert(this.taskExecutionsTable).values(rows)
  }

  async getById({
    executionId,
    filters,
  }: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
  }): Promise<TaskExecutionStorageValue | undefined> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
      .limit(1)
    return rows.length > 0 ? taskExecutionDBValueToStorageValue(rows[0]!) : undefined
  }

  async getBySleepingTaskUniqueId({
    sleepingTaskUniqueId,
  }: {
    sleepingTaskUniqueId: string
  }): Promise<TaskExecutionStorageValue | undefined> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(eq(this.taskExecutionsTable.sleepingTaskUniqueId, sleepingTaskUniqueId))
      .limit(1)
    return rows.length > 0 ? taskExecutionDBValueToStorageValue(rows[0]!) : undefined
  }

  async updateById({
    executionId,
    filters,
    update,
  }: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
  }): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    await this.db
      .update(this.taskExecutionsTable)
      .set(dbUpdate)
      .where(getByIdWhereCondition(this.taskExecutionsTable, executionId, filters))
  }

  async updateByIdAndInsertChildrenIfUpdated({
    executionId,
    filters,
    update,
    childrenTaskExecutionsToInsertIfAnyUpdated,
  }: {
    executionId: string
    filters?: TaskExecutionStorageGetByIdFilters
    update: TaskExecutionStorageUpdate
    childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
  }): Promise<void> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    if (childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      return await this.updateById({ executionId, filters, update })
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

  async updateByStatusAndStartAtLessThanAndReturn({
    status,
    startAtLessThan,
    update,
    updateExpiresAtWithStartedAt,
    limit,
  }: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
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

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn({
    status,
    onChildrenFinishedProcessingStatus,
    update,
    limit,
  }: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
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

  async updateByCloseStatusAndReturn({
    closeStatus,
    update,
    limit,
  }: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    const updatedRows = await this.db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(this.taskExecutionsTable)
        .where(eq(this.taskExecutionsTable.closeStatus, closeStatus))
        .orderBy(asc(this.taskExecutionsTable.updatedAt))
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
      }
      return rows
    })
    return updatedRows.map((row) => taskExecutionDBValueToStorageValue(row, update))
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
    status,
    isSleepingTask,
    expiresAtLessThan,
    update,
    limit,
  }: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select({ executionId: this.taskExecutionsTable.executionId })
        .from(this.taskExecutionsTable)
        .where(
          and(
            eq(this.taskExecutionsTable.status, status),
            eq(this.taskExecutionsTable.isSleepingTask, isSleepingTask),
            lt(this.taskExecutionsTable.expiresAt, expiresAtLessThan),
          ),
        )
        .orderBy(asc(this.taskExecutionsTable.expiresAt))
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
      }
      return rows.length
    })
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan({
    onChildrenFinishedProcessingExpiresAtLessThan,
    update,
    limit,
  }: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
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

  async updateByCloseExpiresAtLessThan({
    closeExpiresAtLessThan,
    update,
    limit,
  }: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    const dbUpdate = taskExecutionStorageUpdateToDBUpdate(update)

    return await this.db.transaction(async (tx) => {
      const rows = await tx
        .select({ executionId: this.taskExecutionsTable.executionId })
        .from(this.taskExecutionsTable)
        .where(lt(this.taskExecutionsTable.closeExpiresAt, closeExpiresAtLessThan))
        .orderBy(asc(this.taskExecutionsTable.closeExpiresAt))
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
      }
      return rows.length
    })
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
    executorId,
    needsPromiseCancellation,
    update,
    limit,
  }: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
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

  async getByParentExecutionId({
    parentExecutionId,
  }: {
    parentExecutionId: string
  }): Promise<Array<TaskExecutionStorageValue>> {
    const rows = await this.db
      .select()
      .from(this.taskExecutionsTable)
      .where(eq(this.taskExecutionsTable.parentExecutionId, parentExecutionId))
    return rows.map((row) => taskExecutionDBValueToStorageValue(row))
  }

  async updateByParentExecutionIdAndIsFinished({
    parentExecutionId,
    isFinished,
    update,
  }: {
    parentExecutionId: string
    isFinished: boolean
    update: TaskExecutionStorageUpdate
  }): Promise<void> {
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

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
    isFinished,
    closeStatus,
    update,
    limit,
  }: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
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

          await tx.run(sql`
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
    })
  }

  async deleteById({ executionId }: { executionId: string }): Promise<void> {
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
  table: TaskExecutionsSQLiteTable,
  executionId: string,
  filters?: TaskExecutionStorageGetByIdFilters,
): SQL | undefined {
  const conditions: Array<SQL> = [eq(table.executionId, executionId)]
  if (filters?.isSleepingTask != null) {
    conditions.push(eq(table.isSleepingTask, filters.isSleepingTask))
  }
  if (filters?.status != null) {
    conditions.push(eq(table.status, filters.status))
  }
  if (filters?.isFinished != null) {
    conditions.push(eq(table.isFinished, filters.isFinished))
  }
  return and(...conditions)
}
