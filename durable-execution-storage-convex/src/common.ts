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

import { omitUndefinedValues } from '@gpahal/std/objects'

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
  status: v.optional(vTaskExecutionStatus),
  isFinished: v.optional(v.boolean()),
  runOutput: v.optional(v.string()),
  output: v.optional(v.string()),
  error: v.optional(vDurableExecutionError),
  retryAttempts: v.optional(v.number()),
  startAt: v.optional(v.number()),
  startedAt: v.optional(v.number()),
  expiresAt: v.optional(v.number()),
  waitingForChildrenStartedAt: v.optional(v.number()),
  waitingForFinalizeStartedAt: v.optional(v.number()),
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
  ocfpFinishedAt: v.optional(v.number()),
  finalize: v.optional(
    v.object({
      taskId: v.string(),
      executionId: v.string(),
    }),
  ),
  closeStatus: v.optional(vTaskExecutionCloseStatus),
  closeExpiresAt: v.optional(v.number()),
  closedAt: v.optional(v.number()),
  npc: v.optional(v.boolean()),
  updatedAt: v.number(),

  unset: v.optional(
    v.object({
      executorId: v.optional(v.boolean()),
      runOutput: v.optional(v.boolean()),
      error: v.optional(v.boolean()),
      startedAt: v.optional(v.boolean()),
      expiresAt: v.optional(v.boolean()),
      ocfpExpiresAt: v.optional(v.boolean()),
      closeExpiresAt: v.optional(v.boolean()),
    }),
  ),
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
  waitingForChildrenStartedAt?: number
  waitingForFinalizeStartedAt?: number
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
    waitingForChildrenStartedAt: value.waitingForChildrenStartedAt,
    waitingForFinalizeStartedAt: value.waitingForFinalizeStartedAt,
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
    sleepingTaskUniqueId: dbValue.sleepingTaskUniqueId,
    retryOptions: dbValue.retryOptions,
    sleepMsBeforeRun: dbValue.sleepMsBeforeRun,
    timeoutMs: dbValue.timeoutMs,
    areChildrenSequential: dbValue.areChildrenSequential,
    input: dbValue.input,
    executorId: dbValue.executorId,
    status: dbValue.status,
    isFinished: dbValue.isFinished,
    runOutput: dbValue.runOutput,
    output: dbValue.output,
    error: dbValue.error,
    retryAttempts: dbValue.retryAttempts,
    startAt: dbValue.startAt,
    startedAt: dbValue.startedAt,
    expiresAt: dbValue.expiresAt,
    waitingForChildrenStartedAt: dbValue.waitingForChildrenStartedAt,
    waitingForFinalizeStartedAt: dbValue.waitingForFinalizeStartedAt,
    finishedAt: dbValue.finishedAt,
    children: dbValue.children,
    activeChildrenCount: dbValue.acc,
    onChildrenFinishedProcessingStatus: dbValue.ocfpStatus,
    onChildrenFinishedProcessingExpiresAt: dbValue.ocfpExpiresAt,
    onChildrenFinishedProcessingFinishedAt: dbValue.ocfpFinishedAt,
    finalize: dbValue.finalize,
    closeStatus: dbValue.closeStatus,
    closeExpiresAt: dbValue.closeExpiresAt,
    closedAt: dbValue.closedAt,
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

  return update
    ? applyTaskExecutionStorageUpdate(value, update, updateExpiresAtWithStartedAt)
    : value
}

export function taskExecutionStorageUpdateToDBUpdateRequest(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdateRequest {
  return omitUndefinedValues({
    executorId: update.executorId,
    status: update.status,
    isFinished: update.isFinished,
    runOutput: update.runOutput,
    output: update.output,
    error: update.error,
    retryAttempts: update.retryAttempts,
    startAt: update.startAt,
    startedAt: update.startedAt,
    expiresAt: update.expiresAt,
    waitingForChildrenStartedAt: update.waitingForChildrenStartedAt,
    waitingForFinalizeStartedAt: update.waitingForFinalizeStartedAt,
    finishedAt: update.finishedAt,
    children: update.children as Array<TaskExecutionSummary>,
    acc: update.activeChildrenCount,
    ocfpStatus: update.onChildrenFinishedProcessingStatus,
    ocfpExpiresAt: update.onChildrenFinishedProcessingExpiresAt,
    ocfpFinishedAt: update.onChildrenFinishedProcessingFinishedAt,
    finalize: update.finalize,
    closeStatus: update.closeStatus,
    closeExpiresAt: update.closeExpiresAt,
    closedAt: update.closedAt,
    npc: update.needsPromiseCancellation,
    updatedAt: update.updatedAt,

    unset: omitUndefinedValues({
      executorId: update.unset?.executorId,
      runOutput: update.unset?.runOutput,
      error: update.unset?.error,
      startedAt: update.unset?.startedAt,
      expiresAt: update.unset?.expiresAt,
      ocfpExpiresAt: update.unset?.onChildrenFinishedProcessingExpiresAt,
      closeExpiresAt: update.unset?.closeExpiresAt,
    }),
  })
}

export function taskExecutionStorageUpdateRequestToDBUpdate(
  update: Infer<typeof vTaskExecutionDBUpdateRequest>,
): TaskExecutionDBUpdate {
  const dbUpdate = omitUndefinedValues({
    ...update,
    unset: undefined,
  })

  const updateUnset: TaskExecutionStorageUpdate['unset'] = update.unset
  if (updateUnset) {
    for (const key in updateUnset) {
      // @ts-expect-error - This is safe because we know the key is valid
      if (updateUnset[key]) {
        // @ts-expect-error - This is safe because we know the key is valid
        dbUpdate[key] = undefined
      }
    }
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
