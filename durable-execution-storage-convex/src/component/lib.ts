import { v, type Infer } from 'convex/values'
import type {
  TaskExecutionCloseStatus,
  TaskExecutionOnChildrenFinishedProcessingStatus,
  TaskExecutionStatus,
} from 'durable-execution'

import {
  applyTaskExecutionIdFilters,
  taskExecutionStorageUpdateRequestToDBUpdate,
  vTaskExecutionDBUpdateRequest,
  vTaskExecutionStorageGetByIdFilters,
  type TaskExecutionDBUpdateRequest,
  type TaskExecutionDBValue,
} from '../common'
import { internal } from './_generated/api'
import type { Id } from './_generated/dataModel'
import { action, internalMutation, mutation, query, type MutationCtx } from './_generated/server'
import { vTaskExecutionDBInsertValue, type TaskExecutionDBInsertValue } from './schema'

const LOCK_EXPIRATION_MS = 60_000

export const acquireLock = mutation({
  args: {
    key: v.string(),
  },
  handler: async (ctx, { key }) => {
    const lock = await ctx.db
      .query('locks')
      .withIndex('by_key', (q) => q.eq('key', key))
      .first()
    if (!lock) {
      const lockId = await ctx.db.insert('locks', {
        key,
        expiresAt: Date.now() + LOCK_EXPIRATION_MS,
      })
      return lockId
    }

    if (lock.expiresAt <= Date.now()) {
      await ctx.db.patch(lock._id, { expiresAt: Date.now() + LOCK_EXPIRATION_MS })
      return lock._id
    }

    return null
  },
})

export const releaseLock = mutation({
  args: {
    id: v.id('locks'),
  },
  handler: async (ctx, { id }) => {
    await ctx.db.delete(id)
  },
})

async function insertOneHelper(ctx: MutationCtx, execution: TaskExecutionDBInsertValue) {
  let existingDBValue = await ctx.db
    .query('taskExecutions')
    .withIndex('by_executionId', (q) => q.eq('executionId', execution.executionId))
    .first()
  if (existingDBValue) {
    throw new Error('Execution id already exists')
  }

  if (execution.sleepingTaskUniqueId) {
    existingDBValue = await ctx.db
      .query('taskExecutions')
      .withIndex('by_sleepingTaskUniqueId', (q) =>
        q.eq('sleepingTaskUniqueId', execution.sleepingTaskUniqueId),
      )
      .first()
    if (existingDBValue) {
      throw new Error('Sleeping task unique id already exists')
    }
  }

  const now = Date.now() + 50
  execution.createdAt = now
  execution.updatedAt = now
  if (execution.startAt < now) {
    execution.startAt = now
  }
  if (execution.expiresAt && execution.expiresAt < now) {
    execution.expiresAt = now + 1000
  }
  if (execution.ocfpExpiresAt && execution.ocfpExpiresAt < now) {
    execution.ocfpExpiresAt = now + 1000
  }
  if (execution.closeExpiresAt && execution.closeExpiresAt < now) {
    execution.closeExpiresAt = now + 1000
  }
  await ctx.db.insert('taskExecutions', execution)
}

async function insertManyHelper(
  ctx: MutationCtx,
  executions: ReadonlyArray<TaskExecutionDBInsertValue>,
) {
  for (const execution of executions) {
    await insertOneHelper(ctx, execution)
  }
}

async function updateOneHelper(
  ctx: MutationCtx,
  dbValue: TaskExecutionDBValue,
  update: TaskExecutionDBUpdateRequest,
) {
  const now = Date.now() + 50
  if (update.updatedAt < now) {
    update.updatedAt = now
  }
  if (update.startAt && update.startAt < now) {
    update.startAt = now
  }
  if (update.startedAt && update.startedAt < now) {
    update.startedAt = now
  }
  if (update.expiresAt && update.expiresAt < now) {
    update.expiresAt = now + 1000
  }
  if (update.finishedAt && update.finishedAt < now) {
    update.finishedAt = now
  }
  if (update.ocfpExpiresAt && update.ocfpExpiresAt < now) {
    update.ocfpExpiresAt = now + 1000
  }
  if (update.ocfpFinishedAt && update.ocfpFinishedAt < now) {
    update.ocfpFinishedAt = now
  }
  if (update.closeExpiresAt && update.closeExpiresAt < now) {
    update.closeExpiresAt = now + 1000
  }
  if (update.closedAt && update.closedAt < now) {
    update.closedAt = now
  }
  await ctx.db.patch(dbValue._id, taskExecutionStorageUpdateRequestToDBUpdate(update))
}

async function updateManyHelper(
  ctx: MutationCtx,
  dbValues: ReadonlyArray<TaskExecutionDBValue>,
  update: Infer<typeof vTaskExecutionDBUpdateRequest>,
) {
  for (const dbValue of dbValues) {
    await updateOneHelper(ctx, dbValue, update)
  }
}

export const insertMany = mutation({
  args: {
    executions: v.array(vTaskExecutionDBInsertValue),
  },
  handler: async (ctx, args) => {
    await insertManyHelper(ctx, args.executions)
  },
})

export const getManyById = query({
  args: {
    requests: v.array(
      v.object({
        executionId: v.string(),
        filters: v.optional(vTaskExecutionStorageGetByIdFilters),
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    const dbValues: Array<TaskExecutionDBValue | null> = []
    for (const { executionId, filters } of requests) {
      const dbValue = await ctx.db
        .query('taskExecutions')
        .withIndex('by_executionId', (q) => q.eq('executionId', executionId))
        .first()
      if (!dbValue || !applyTaskExecutionIdFilters(dbValue, filters)) {
        dbValues.push(null)
      } else {
        dbValues.push(dbValue)
      }
    }
    return dbValues
  },
})

export const getManyBySleepingTaskUniqueId = query({
  args: {
    requests: v.array(
      v.object({
        sleepingTaskUniqueId: v.string(),
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    const dbValues: Array<TaskExecutionDBValue | null> = []
    for (const { sleepingTaskUniqueId } of requests) {
      const dbValue = await ctx.db
        .query('taskExecutions')
        .withIndex('by_sleepingTaskUniqueId', (q) =>
          q.eq('sleepingTaskUniqueId', sleepingTaskUniqueId),
        )
        .first()
      if (!dbValue) {
        dbValues.push(null)
      } else {
        dbValues.push(dbValue)
      }
    }
    return dbValues
  },
})

export const updateManyById = mutation({
  args: {
    requests: v.array(
      v.object({
        executionId: v.string(),
        filters: v.optional(vTaskExecutionStorageGetByIdFilters),
        update: vTaskExecutionDBUpdateRequest,
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    for (const { executionId, filters, update } of requests) {
      const dbValue = await ctx.db
        .query('taskExecutions')
        .withIndex('by_executionId', (q) => q.eq('executionId', executionId))
        .first()
      if (!dbValue || !applyTaskExecutionIdFilters(dbValue, filters)) {
        continue
      }

      await updateOneHelper(ctx, dbValue, update)
    }
  },
})

export const updateManyByIdAndInsertChildrenIfUpdated = mutation({
  args: {
    requests: v.array(
      v.object({
        executionId: v.string(),
        filters: v.optional(vTaskExecutionStorageGetByIdFilters),
        update: vTaskExecutionDBUpdateRequest,
        childrenTaskExecutionsToInsertIfAnyUpdated: v.array(vTaskExecutionDBInsertValue),
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    for (const {
      executionId,
      filters,
      update,
      childrenTaskExecutionsToInsertIfAnyUpdated,
    } of requests) {
      const dbValue = await ctx.db
        .query('taskExecutions')
        .withIndex('by_executionId', (q) => q.eq('executionId', executionId))
        .first()
      if (!dbValue || !applyTaskExecutionIdFilters(dbValue, filters)) {
        continue
      }

      await updateOneHelper(ctx, dbValue, update)
      for (const execution of childrenTaskExecutionsToInsertIfAnyUpdated) {
        execution.parentExecutionDocId = dbValue._id
      }
      await insertManyHelper(ctx, childrenTaskExecutionsToInsertIfAnyUpdated)
    }
  },
})

export const updateByStatusAndStartAtLessThanAndReturn = mutation(
  async (
    ctx,
    {
      shard,
      status,
      startAtLessThan,
      update,
      updateExpiresAtWithStartedAt,
      limit,
    }: {
      shard: number
      status: TaskExecutionStatus
      startAtLessThan: number
      update: TaskExecutionDBUpdateRequest
      updateExpiresAtWithStartedAt: number
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_shard_status_startAt', (q) =>
        q.eq('shard', shard).eq('status', status).lt('startAt', startAtLessThan),
      )
      .order('asc')
      .take(limit)

    for (const dbValue of dbValues) {
      const finalUpdate: TaskExecutionDBUpdateRequest = {
        ...update,
        expiresAt: updateExpiresAtWithStartedAt + dbValue.timeoutMs,
      }
      await updateOneHelper(ctx, dbValue, finalUpdate)
    }

    return dbValues
  },
)

export const updateByStatusAndOCFPStatusAndACCZeroAndReturn = mutation(
  async (
    ctx,
    {
      shard,
      status,
      ocfpStatus,
      update,
      limit,
    }: {
      shard: number
      status: TaskExecutionStatus
      ocfpStatus: TaskExecutionOnChildrenFinishedProcessingStatus
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_shard_status_ocfpStatus_acc_updatedAt', (q) =>
        q
          .eq('shard', shard)
          .eq('status', status)
          .eq('ocfpStatus', ocfpStatus)
          .eq('acc', 0)
          .lt('updatedAt', Date.now()),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return []
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues
  },
)

export const updateByCloseStatusAndReturn = mutation(
  async (
    ctx,
    {
      shard,
      closeStatus,
      update,
      limit,
    }: {
      shard: number
      closeStatus: TaskExecutionCloseStatus
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_shard_closeStatus_updatedAt', (q) =>
        q.eq('shard', shard).eq('closeStatus', closeStatus).lt('updatedAt', Date.now()),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return []
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues
  },
)

export const updateByStatusAndIsSleepingTaskAndExpiresAtLessThan = mutation(
  async (
    ctx,
    {
      status,
      isSleepingTask,
      expiresAtLessThan,
      update,
      limit,
    }: {
      status: TaskExecutionStatus
      isSleepingTask: boolean
      expiresAtLessThan: number
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_status_isSleepingTask_expiresAt', (q) =>
        q
          .eq('status', status)
          .eq('isSleepingTask', isSleepingTask)
          .gte('expiresAt', 0)
          .lt('expiresAt', expiresAtLessThan),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return 0
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues.length
  },
)

export const updateByOCFPExpiresAt = mutation(
  async (
    ctx,
    {
      ocfpExpiresAtLessThan,
      update,
      limit,
    }: {
      ocfpExpiresAtLessThan: number
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_ocfpExpiresAt', (q) =>
        q.gte('ocfpExpiresAt', 0).lt('ocfpExpiresAt', ocfpExpiresAtLessThan),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return 0
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues.length
  },
)

export const updateByCloseExpiresAt = mutation(
  async (
    ctx,
    {
      closeExpiresAtLessThan,
      update,
      limit,
    }: {
      closeExpiresAtLessThan: number
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_closeExpiresAt', (q) =>
        q.gte('closeExpiresAt', 0).lt('closeExpiresAt', closeExpiresAtLessThan),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return 0
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues.length
  },
)

export const updateByExecutorIdAndNPCAndReturn = mutation(
  async (
    ctx,
    {
      shard,
      executorId,
      npc,
      update,
      limit,
    }: {
      shard: number
      executorId: string
      npc: boolean
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_shard_executorId_npc_updatedAt', (q) =>
        q
          .eq('shard', shard)
          .eq('executorId', executorId)
          .eq('npc', npc)
          .lt('updatedAt', Date.now()),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return []
    }

    await updateManyHelper(ctx, dbValues, update)
    return dbValues
  },
)

export const getManyByParentExecutionId = query({
  args: {
    requests: v.array(
      v.object({
        parentExecutionId: v.string(),
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    const dbValues: Array<Array<TaskExecutionDBValue>> = []
    for (const { parentExecutionId } of requests) {
      const currDbValues = await ctx.db
        .query('taskExecutions')
        .withIndex('by_parentExecutionId_isFinished', (q) =>
          q.eq('parentExecutionId', parentExecutionId),
        )
        .collect()
      dbValues.push(currDbValues)
    }
    return dbValues
  },
})

export const updateManyByParentExecutionIdAndIsFinished = mutation({
  args: {
    requests: v.array(
      v.object({
        parentExecutionId: v.string(),
        isFinished: v.boolean(),
        update: vTaskExecutionDBUpdateRequest,
      }),
    ),
  },
  handler: async (ctx, { requests }) => {
    for (const { parentExecutionId, isFinished, update } of requests) {
      const dbValues = await ctx.db
        .query('taskExecutions')
        .withIndex('by_parentExecutionId_isFinished', (q) =>
          q.eq('parentExecutionId', parentExecutionId).eq('isFinished', isFinished),
        )
        .collect()

      await updateManyHelper(ctx, dbValues, update)
    }
  },
})

export const updateAndDecrementParentACCByIsFinishedAndCloseStatus = mutation(
  async (
    ctx,
    {
      shard,
      isFinished,
      closeStatus,
      update,
      limit,
    }: {
      shard: number
      isFinished: boolean
      closeStatus: TaskExecutionCloseStatus
      update: TaskExecutionDBUpdateRequest
      limit: number
    },
  ) => {
    const dbValues = await ctx.db
      .query('taskExecutions')
      .withIndex('by_shard_isFinished_closeStatus_updatedAt', (q) =>
        q
          .eq('shard', shard)
          .eq('isFinished', isFinished)
          .eq('closeStatus', closeStatus)
          .lt('updatedAt', Date.now()),
      )
      .order('asc')
      .take(limit)
    if (dbValues.length === 0) {
      return 0
    }

    await updateManyHelper(ctx, dbValues, update)

    const parentExecutionDocIdToDecrementValueMap = new Map<Id<'taskExecutions'>, number>()
    const singleChildParentExecutionDocIds = new Set<Id<'taskExecutions'>>()
    for (const dbValue of dbValues) {
      if (dbValue.parentExecutionDocId != null && !dbValue.isFinalizeOfParent) {
        if (dbValue.isOnlyChildOfParent) {
          singleChildParentExecutionDocIds.add(dbValue.parentExecutionDocId)
        } else {
          parentExecutionDocIdToDecrementValueMap.set(
            dbValue.parentExecutionDocId,
            (parentExecutionDocIdToDecrementValueMap.get(dbValue.parentExecutionDocId) ?? 0) + 1,
          )
        }
      }
    }

    if (singleChildParentExecutionDocIds.size > 0) {
      for (const parentExecutionDocId of singleChildParentExecutionDocIds) {
        await ctx.db.patch(parentExecutionDocId, {
          acc: 0,
          updatedAt: update.updatedAt,
        })
      }
    }
    if (parentExecutionDocIdToDecrementValueMap.size > 0) {
      const parentExecutionDocIds = [...parentExecutionDocIdToDecrementValueMap.keys()].sort()
      for (const parentDBId of parentExecutionDocIds) {
        const parentDBValue = await ctx.db.get(parentDBId)
        if (parentDBValue) {
          await updateOneHelper(ctx, parentDBValue, {
            acc: parentDBValue.acc - (parentExecutionDocIdToDecrementValueMap.get(parentDBId) ?? 0),
            updatedAt: update.updatedAt,
          })
        }
      }
    }

    return dbValues.length
  },
)

export const deleteById = mutation({
  args: {
    executionId: v.string(),
  },
  handler: async (ctx, { executionId }) => {
    const dbValue = await ctx.db
      .query('taskExecutions')
      .withIndex('by_executionId', (q) => q.eq('executionId', executionId))
      .first()
    if (!dbValue) {
      return
    }

    await ctx.db.delete(dbValue._id)
  },
})

export const deleteMany = internalMutation({
  args: {
    batchSize: v.number(),
  },
  handler: async (ctx, { batchSize }) => {
    const dbValues = await ctx.db.query('taskExecutions').take(batchSize)
    if (dbValues.length === 0) {
      return 0
    }

    for (const dbValue of dbValues) {
      await ctx.db.delete(dbValue._id)
    }
    return dbValues.length
  },
})

export const deleteAll = action({
  args: {},
  handler: async (ctx) => {
    while (true) {
      const deletedCount = await ctx.runMutation(internal.lib.deleteMany, { batchSize: 100 })
      if (deletedCount < 100) {
        break
      }
    }
  },
})
