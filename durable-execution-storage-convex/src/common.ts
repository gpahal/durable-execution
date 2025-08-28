import { v, type Infer } from 'convex/values'
import {
  applyTaskExecutionStorageUpdate,
  type DurableExecutionErrorStorageValue,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
} from 'durable-execution'

import type { Doc } from './component/_generated/dataModel'
import {
  vDurableExecutionError,
  vTaskExecutionCloseStatus,
  vTaskExecutionOnChildrenFinishedProcessingStatus,
  vTaskExecutionStatus,
  type TaskExecutionDBInsertValue,
} from './component/schema'

export type TaskExecutionDBValue = Doc<'taskExecutions'>

export const vTaskExecutionDBUpdateRequest = v.object({
  executorId: v.optional(v.string()),
  unsetExecutorId: v.optional(v.boolean()),
  status: v.optional(vTaskExecutionStatus),
  isFinished: v.optional(v.boolean()),
  runOutput: v.optional(v.string()),
  unsetRunOutput: v.optional(v.boolean()),
  output: v.optional(v.string()),
  error: v.optional(vDurableExecutionError),
  unsetError: v.optional(v.boolean()),
  retryAttempts: v.optional(v.number()),
  startAt: v.optional(v.number()),
  startedAt: v.optional(v.number()),
  expiresAt: v.optional(v.number()),
  unsetExpiresAt: v.optional(v.boolean()),
  finishedAt: v.optional(v.number()),
  children: v.optional(
    v.array(
      v.object({
        taskId: v.string(),
        executionId: v.string(),
      }),
    ),
  ),
  acc: v.optional(v.number()),
  ocfpStatus: v.optional(vTaskExecutionOnChildrenFinishedProcessingStatus),
  ocfpExpiresAt: v.optional(v.number()),
  unsetOCFPExpiresAt: v.optional(v.boolean()),
  ocfpFinishedAt: v.optional(v.number()),
  finalize: v.optional(
    v.object({
      taskId: v.string(),
      executionId: v.string(),
    }),
  ),
  closeStatus: v.optional(vTaskExecutionCloseStatus),
  closeExpiresAt: v.optional(v.number()),
  unsetCloseExpiresAt: v.optional(v.boolean()),
  closedAt: v.optional(v.number()),
  npc: v.optional(v.boolean()),
  updatedAt: v.number(),
})
export type TaskExecutionDBUpdateRequest = Infer<typeof vTaskExecutionDBUpdateRequest>

export type TaskExecutionDBUpdate = {
  executorId?: string
  status?: TaskExecutionStatus
  isFinished?: boolean
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue
  retryAttempts?: number
  startAt?: number
  startedAt?: number
  expiresAt?: number
  finishedAt?: number

  children?: Array<TaskExecutionSummary>
  acc?: number
  ocfpStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  ocfpExpiresAt?: number
  ocfpFinishedAt?: number

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: number
  closedAt?: number

  npc?: boolean

  updatedAt: number
}

export function taskExecutionStorageValueToDBInsertValue(
  value: TaskExecutionStorageValue,
  shard: number,
): TaskExecutionDBInsertValue {
  return {
    shard,
    rootTaskId: value.root?.taskId,
    rootExecutionId: value.root?.executionId,
    parentTaskId: value.parent?.taskId,
    parentExecutionId: value.parent?.executionId,
    indexInParentChildren: value.parent?.indexInParentChildren,
    isOnlyChildOfParent: value.parent?.isOnlyChildOfParent,
    isFinalizeOfParent: value.parent?.isFinalizeOfParent,
    taskId: value.taskId,
    executionId: value.executionId,
    isSleepingTask: value.isSleepingTask,
    sleepingTaskUniqueId: value.sleepingTaskUniqueId,
    retryOptions: value.retryOptions,
    sleepMsBeforeRun: value.sleepMsBeforeRun,
    timeoutMs: value.timeoutMs,
    areChildrenSequential: value.areChildrenSequential,
    input: value.input,
    executorId: value.executorId,
    status: value.status,
    isFinished: value.isFinished,
    runOutput: value.runOutput,
    output: value.output,
    error: value.error,
    retryAttempts: value.retryAttempts,
    startAt: value.startAt,
    startedAt: value.startedAt,
    expiresAt: value.expiresAt,
    finishedAt: value.finishedAt,
    children: value.children,
    acc: value.activeChildrenCount,
    ocfpStatus: value.onChildrenFinishedProcessingStatus,
    ocfpExpiresAt: value.onChildrenFinishedProcessingExpiresAt,
    ocfpFinishedAt: value.onChildrenFinishedProcessingFinishedAt,
    finalize: value.finalize,
    closeStatus: value.closeStatus,
    closeExpiresAt: value.closeExpiresAt,
    closedAt: value.closedAt,
    npc: value.needsPromiseCancellation,
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  }
}

export function taskExecutionDBValueToStorageValue(
  dbValue: TaskExecutionDBValue,
  update?: TaskExecutionStorageUpdate,
  updateExpiresAtWithStartedAt?: number,
): TaskExecutionStorageValue {
  const value: TaskExecutionStorageValue = {
    taskId: dbValue.taskId,
    executionId: dbValue.executionId,
    isSleepingTask: dbValue.isSleepingTask,
    retryOptions: dbValue.retryOptions,
    sleepMsBeforeRun: dbValue.sleepMsBeforeRun,
    timeoutMs: dbValue.timeoutMs,
    areChildrenSequential: dbValue.areChildrenSequential,
    input: dbValue.input,
    status: dbValue.status,
    isFinished: dbValue.isFinished,
    retryAttempts: dbValue.retryAttempts,
    startAt: dbValue.startAt,
    activeChildrenCount: dbValue.acc,
    onChildrenFinishedProcessingStatus: dbValue.ocfpStatus,
    closeStatus: dbValue.closeStatus,
    needsPromiseCancellation: dbValue.npc,
    createdAt: dbValue.createdAt,
    updatedAt: dbValue.updatedAt,
  }

  if (dbValue.rootTaskId && dbValue.rootExecutionId) {
    value.root = {
      taskId: dbValue.rootTaskId,
      executionId: dbValue.rootExecutionId,
    }
  }

  if (dbValue.parentTaskId && dbValue.parentExecutionId) {
    value.parent = {
      taskId: dbValue.parentTaskId,
      executionId: dbValue.parentExecutionId,
      indexInParentChildren: dbValue.indexInParentChildren ?? 0,
      isOnlyChildOfParent: dbValue.isOnlyChildOfParent ?? false,
      isFinalizeOfParent: dbValue.isFinalizeOfParent ?? false,
    }
  }

  if (dbValue.executorId) {
    value.executorId = dbValue.executorId
  }

  if (dbValue.sleepingTaskUniqueId != null) {
    value.sleepingTaskUniqueId = dbValue.sleepingTaskUniqueId
  }

  if (dbValue.runOutput != null) {
    value.runOutput = dbValue.runOutput
  }

  if (dbValue.output != null) {
    value.output = dbValue.output
  }

  if (dbValue.error) {
    value.error = dbValue.error
  }

  if (dbValue.startedAt) {
    value.startedAt = dbValue.startedAt
  }

  if (dbValue.expiresAt) {
    value.expiresAt = dbValue.expiresAt
  }

  if (updateExpiresAtWithStartedAt) {
    value.expiresAt = updateExpiresAtWithStartedAt + dbValue.timeoutMs
  }

  if (dbValue.finishedAt) {
    value.finishedAt = dbValue.finishedAt
  }

  if (dbValue.children) {
    value.children = dbValue.children
  }

  if (dbValue.ocfpExpiresAt) {
    value.onChildrenFinishedProcessingExpiresAt = dbValue.ocfpExpiresAt
  }

  if (dbValue.ocfpFinishedAt) {
    value.onChildrenFinishedProcessingFinishedAt = dbValue.ocfpFinishedAt
  }

  if (dbValue.finalize) {
    value.finalize = dbValue.finalize
  }

  if (dbValue.closeExpiresAt) {
    value.closeExpiresAt = dbValue.closeExpiresAt
  }

  if (dbValue.closedAt) {
    value.closedAt = dbValue.closedAt
  }

  return update ? applyTaskExecutionStorageUpdate(value, update) : value
}

export function taskExecutionStorageUpdateToDBUpdateRequest(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdateRequest {
  return {
    executorId: update.executorId,
    unsetExecutorId: update.unsetExecutorId,
    status: update.status,
    isFinished: update.isFinished,
    runOutput: update.runOutput,
    unsetRunOutput: update.unsetRunOutput,
    output: update.output,
    error: update.error,
    unsetError: update.unsetError,
    retryAttempts: update.retryAttempts,
    startAt: update.startAt,
    startedAt: update.startedAt,
    expiresAt: update.expiresAt,
    unsetExpiresAt: update.unsetExpiresAt,
    finishedAt: update.finishedAt,
    children: update.children as Array<TaskExecutionSummary>,
    acc: update.activeChildrenCount,
    ocfpStatus: update.onChildrenFinishedProcessingStatus,
    ocfpExpiresAt: update.onChildrenFinishedProcessingExpiresAt,
    unsetOCFPExpiresAt: update.unsetOnChildrenFinishedProcessingExpiresAt,
    ocfpFinishedAt: update.onChildrenFinishedProcessingFinishedAt,
    finalize: update.finalize,
    closeStatus: update.closeStatus,
    closeExpiresAt: update.closeExpiresAt,
    unsetCloseExpiresAt: update.unsetCloseExpiresAt,
    closedAt: update.closedAt,
    npc: update.needsPromiseCancellation,
    updatedAt: update.updatedAt,
  }
}

export function taskExecutionStorageUpdateRequestToDBUpdate(
  update: Infer<typeof vTaskExecutionDBUpdateRequest>,
): TaskExecutionDBUpdate {
  const dbUpdate: TaskExecutionDBUpdate = {
    updatedAt: update.updatedAt,
  }

  if (update.executorId != null) {
    dbUpdate.executorId = update.executorId
  }

  if (update.unsetExecutorId) {
    dbUpdate.executorId = undefined
  }

  if (update.status != null) {
    dbUpdate.status = update.status
  }

  if (update.isFinished != null) {
    dbUpdate.isFinished = update.isFinished
  }

  if (update.runOutput != null) {
    dbUpdate.runOutput = update.runOutput
  }

  if (update.unsetRunOutput) {
    dbUpdate.runOutput = undefined
  }

  if (update.output != null) {
    dbUpdate.output = update.output
  }

  if (update.error != null) {
    dbUpdate.error = update.error
  }

  if (update.unsetError) {
    dbUpdate.error = undefined
  }

  if (update.retryAttempts != null) {
    dbUpdate.retryAttempts = update.retryAttempts
  }

  if (update.startAt != null) {
    dbUpdate.startAt = update.startAt
  }

  if (update.startedAt != null) {
    dbUpdate.startedAt = update.startedAt
  }

  if (update.expiresAt != null) {
    dbUpdate.expiresAt = update.expiresAt
  }

  if (update.unsetExpiresAt) {
    dbUpdate.expiresAt = undefined
  }

  if (update.finishedAt != null) {
    dbUpdate.finishedAt = update.finishedAt
  }

  if (update.children != null) {
    dbUpdate.children = update.children
  }

  if (update.acc != null) {
    dbUpdate.acc = update.acc
  }

  if (update.ocfpStatus != null) {
    dbUpdate.ocfpStatus = update.ocfpStatus
  }

  if (update.ocfpExpiresAt != null) {
    dbUpdate.ocfpExpiresAt = update.ocfpExpiresAt
  }

  if (update.unsetOCFPExpiresAt) {
    dbUpdate.ocfpExpiresAt = undefined
  }

  if (update.ocfpFinishedAt != null) {
    dbUpdate.ocfpFinishedAt = update.ocfpFinishedAt
  }

  if (update.finalize != null) {
    dbUpdate.finalize = update.finalize
  }

  if (update.closeStatus != null) {
    dbUpdate.closeStatus = update.closeStatus
  }

  if (update.closeExpiresAt != null) {
    dbUpdate.closeExpiresAt = update.closeExpiresAt
  }

  if (update.unsetCloseExpiresAt) {
    dbUpdate.closeExpiresAt = undefined
  }

  if (update.closedAt != null) {
    dbUpdate.closedAt = update.closedAt
  }

  if (update.npc != null) {
    dbUpdate.npc = update.npc
  }

  return dbUpdate
}

export const vTaskExecutionStorageGetByIdFilters = v.object({
  isSleepingTask: v.optional(v.boolean()),
  status: v.optional(vTaskExecutionStatus),
  isFinished: v.optional(v.boolean()),
})

export function applyTaskExecutionIdFilters(
  execution: TaskExecutionDBInsertValue,
  filters?: TaskExecutionStorageGetByIdFilters | null,
) {
  if (filters?.isSleepingTask != null && execution.isSleepingTask !== filters.isSleepingTask) {
    return false
  }
  if (filters?.status != null && execution.status !== filters.status) {
    return false
  }
  if (filters?.isFinished != null && execution.isFinished !== filters.isFinished) {
    return false
  }
  return true
}
