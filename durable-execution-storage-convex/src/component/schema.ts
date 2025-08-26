import { defineSchema, defineTable } from 'convex/server'
import { v, type Infer } from 'convex/values'

export const vDurableExecutionErrorType = v.union(
  v.literal('generic'),
  v.literal('not_found'),
  v.literal('timed_out'),
  v.literal('cancelled'),
)

export const vDurableExecutionError = v.object({
  errorType: vDurableExecutionErrorType,
  message: v.string(),
  isRetryable: v.boolean(),
  isInternal: v.boolean(),
})

export const vTaskExecutionStatus = v.union(
  v.literal('ready'),
  v.literal('running'),
  v.literal('failed'),
  v.literal('timed_out'),
  v.literal('waiting_for_children'),
  v.literal('waiting_for_finalize'),
  v.literal('finalize_failed'),
  v.literal('completed'),
  v.literal('cancelled'),
)

export const vTaskExecutionOnChildrenFinishedProcessingStatus = v.union(
  v.literal('idle'),
  v.literal('processing'),
  v.literal('processed'),
)

export const vTaskExecutionCloseStatus = v.union(
  v.literal('idle'),
  v.literal('ready'),
  v.literal('closing'),
  v.literal('closed'),
)

export const vTaskExecutionDBInsertValue = v.object({
  shard: v.number(),
  rootTaskId: v.optional(v.string()),
  rootExecutionId: v.optional(v.string()),
  parentTaskId: v.optional(v.string()),
  parentExecutionId: v.optional(v.string()),
  parentExecutionDocId: v.optional(v.id('taskExecutions')),
  indexInParentChildren: v.optional(v.number()),
  isOnlyChildOfParent: v.optional(v.boolean()),
  isFinalizeOfParent: v.optional(v.boolean()),

  taskId: v.string(),
  executionId: v.string(),
  isSleepingTask: v.boolean(),
  sleepingTaskUniqueId: v.optional(v.string()),
  retryOptions: v.object({
    maxAttempts: v.number(),
    baseDelayMs: v.optional(v.number()),
    maxDelayMs: v.optional(v.number()),
    delayMultiplier: v.optional(v.number()),
  }),
  sleepMsBeforeRun: v.number(),
  timeoutMs: v.number(),
  areChildrenSequential: v.boolean(),
  input: v.string(),

  executorId: v.optional(v.string()),
  status: vTaskExecutionStatus,
  isFinished: v.boolean(),
  runOutput: v.optional(v.string()),
  output: v.optional(v.string()),
  error: v.optional(vDurableExecutionError),
  retryAttempts: v.number(),
  startAt: v.number(),
  startedAt: v.optional(v.number()),
  expiresAt: v.optional(v.number()),
  finishedAt: v.optional(v.number()),

  children: v.optional(
    v.array(
      v.object({
        taskId: v.string(),
        executionId: v.string(),
      }),
    ),
  ),
  acc: v.number(),
  ocfpStatus: vTaskExecutionOnChildrenFinishedProcessingStatus,
  ocfpExpiresAt: v.optional(v.number()),
  ocfpFinishedAt: v.optional(v.number()),

  finalize: v.optional(
    v.object({
      taskId: v.string(),
      executionId: v.string(),
    }),
  ),

  closeStatus: vTaskExecutionCloseStatus,
  closeExpiresAt: v.optional(v.number()),
  closedAt: v.optional(v.number()),

  npc: v.boolean(),

  createdAt: v.number(),
  updatedAt: v.number(),
})
export type TaskExecutionDBInsertValue = Infer<typeof vTaskExecutionDBInsertValue>

export default defineSchema({
  taskExecutions: defineTable(vTaskExecutionDBInsertValue)
    .index('by_executionId', ['executionId'])
    .index('by_sleepingTaskUniqueId', ['sleepingTaskUniqueId'])
    .index('by_shard_status_startAt', ['shard', 'status', 'startAt'])
    .index('by_shard_status_ocfpStatus_acc_updatedAt', [
      'shard',
      'status',
      'ocfpStatus',
      'acc',
      'updatedAt',
    ])
    .index('by_shard_closeStatus_updatedAt', ['shard', 'closeStatus', 'updatedAt'])
    .index('by_isSleepingTask_expiresAt', ['isSleepingTask', 'expiresAt'])
    .index('by_ocfpExpiresAt', ['ocfpExpiresAt'])
    .index('by_closeExpiresAt', ['closeExpiresAt'])
    .index('by_shard_executorId_npc_updatedAt', ['shard', 'executorId', 'npc', 'updatedAt'])
    .index('by_parentExecutionId_isFinished', ['parentExecutionId', 'isFinished'])
    .index('by_shard_isFinished_closeStatus_updatedAt', [
      'shard',
      'isFinished',
      'closeStatus',
      'updatedAt',
    ]),

  locks: defineTable({
    key: v.string(),
    expiresAt: v.number(),
  })
    .index('by_key', ['key'])
    .index('by_key_expiresAt', ['key', 'expiresAt']),
})
