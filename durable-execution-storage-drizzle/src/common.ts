import { applyTaskExecutionStorageUpdate } from 'durable-execution'
import type {
  DurableExecutionErrorStorageValue,
  TaskExecutionCloseStatus,
  TaskExecutionOnChildrenFinishedProcessingStatus,
  TaskExecutionStatus,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskExecutionSummary,
  TaskRetryOptions,
} from 'durable-execution'

import { omitUndefinedValues } from '@gpahal/std/objects'

export type TaskExecutionDBValue = {
  rootTaskId?: string | null
  rootExecutionId?: string | null
  parentTaskId?: string | null
  parentExecutionId?: string | null
  indexInParentChildren?: number | null
  isOnlyChildOfParent?: boolean | null
  isFinalizeOfParent?: boolean | null

  taskId: string
  executionId: string
  isSleepingTask: boolean
  sleepingTaskUniqueId?: string | null
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  areChildrenSequential: boolean
  input: string

  executorId?: string | null
  status: TaskExecutionStatus
  isFinished: boolean
  runOutput?: string | null
  output?: string | null
  error?: DurableExecutionErrorStorageValue | null
  retryAttempts: number
  startAt: number
  startedAt?: number | null
  expiresAt?: number | null
  waitingForChildrenStartedAt?: number | null
  waitingForFinalizeStartedAt?: number | null
  finishedAt?: number | null

  children?: Array<TaskExecutionSummary> | null
  activeChildrenCount: number
  onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: number | null
  onChildrenFinishedProcessingFinishedAt?: number | null

  finalize?: TaskExecutionSummary | null

  closeStatus: TaskExecutionCloseStatus
  closeExpiresAt?: number | null
  closedAt?: number | null

  needsPromiseCancellation: boolean

  createdAt: number
  updatedAt: number
}

export type TaskExecutionDBUpdate = {
  executorId?: string | null
  status?: TaskExecutionStatus
  isFinished?: boolean
  runOutput?: string | null
  output?: string
  error?: DurableExecutionErrorStorageValue | null
  retryAttempts?: number
  startAt?: number
  startedAt?: number | null
  expiresAt?: number | null
  waitingForChildrenStartedAt?: number
  waitingForFinalizeStartedAt?: number
  finishedAt?: number

  children?: Array<TaskExecutionSummary>
  activeChildrenCount?: number
  shouldDecrementParentActiveChildrenCount?: boolean
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: number | null
  onChildrenFinishedProcessingFinishedAt?: number

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: number | null
  closedAt?: number

  needsPromiseCancellation?: boolean

  updatedAt: number
}

function isNonEmptyString(value: string | null | undefined): value is string {
  return value != null && value !== ''
}

function isTruthyNumber(value: number | null | undefined): value is number {
  return value != null && value !== 0 && !Number.isNaN(value)
}

/**
 * Compares two strings by their UTF-16 code units, as `Array#sort()` does without a compare
 * function.
 */
export function compareStringsByCodeUnits(a: string, b: string): number {
  if (a < b) {
    return -1
  }
  if (a > b) {
    return 1
  }
  return 0
}

export function taskExecutionStorageValueToDBValue(
  value: TaskExecutionStorageValue,
): TaskExecutionDBValue {
  return {
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
    activeChildrenCount: value.activeChildrenCount,
    onChildrenFinishedProcessingStatus: value.onChildrenFinishedProcessingStatus,
    onChildrenFinishedProcessingExpiresAt: value.onChildrenFinishedProcessingExpiresAt,
    onChildrenFinishedProcessingFinishedAt: value.onChildrenFinishedProcessingFinishedAt,
    finalize: value.finalize,
    closeStatus: value.closeStatus,
    closeExpiresAt: value.closeExpiresAt,
    closedAt: value.closedAt,
    needsPromiseCancellation: value.needsPromiseCancellation,
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
    activeChildrenCount: dbValue.activeChildrenCount,
    onChildrenFinishedProcessingStatus: dbValue.onChildrenFinishedProcessingStatus,
    closeStatus: dbValue.closeStatus,
    needsPromiseCancellation: dbValue.needsPromiseCancellation,
    createdAt: dbValue.createdAt,
    updatedAt: dbValue.updatedAt,
  }

  if (isNonEmptyString(dbValue.rootTaskId) && isNonEmptyString(dbValue.rootExecutionId)) {
    value.root = {
      taskId: dbValue.rootTaskId,
      executionId: dbValue.rootExecutionId,
    }
  }

  if (isNonEmptyString(dbValue.parentTaskId) && isNonEmptyString(dbValue.parentExecutionId)) {
    value.parent = {
      taskId: dbValue.parentTaskId,
      executionId: dbValue.parentExecutionId,
      indexInParentChildren: dbValue.indexInParentChildren ?? 0,
      isOnlyChildOfParent: dbValue.isOnlyChildOfParent ?? false,
      isFinalizeOfParent: dbValue.isFinalizeOfParent ?? false,
    }
  }

  if (isNonEmptyString(dbValue.executorId)) {
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

  if (isTruthyNumber(dbValue.startedAt)) {
    value.startedAt = dbValue.startedAt
  }

  if (isTruthyNumber(dbValue.expiresAt)) {
    value.expiresAt = dbValue.expiresAt
  }

  if (isTruthyNumber(dbValue.waitingForChildrenStartedAt)) {
    value.waitingForChildrenStartedAt = dbValue.waitingForChildrenStartedAt
  }

  if (isTruthyNumber(dbValue.waitingForFinalizeStartedAt)) {
    value.waitingForFinalizeStartedAt = dbValue.waitingForFinalizeStartedAt
  }

  if (isTruthyNumber(dbValue.expiresAt)) {
    value.expiresAt = dbValue.expiresAt
  }

  if (isTruthyNumber(dbValue.finishedAt)) {
    value.finishedAt = dbValue.finishedAt
  }

  if (dbValue.children) {
    value.children = dbValue.children
  }

  if (isTruthyNumber(dbValue.onChildrenFinishedProcessingExpiresAt)) {
    value.onChildrenFinishedProcessingExpiresAt = dbValue.onChildrenFinishedProcessingExpiresAt
  }

  if (isTruthyNumber(dbValue.onChildrenFinishedProcessingFinishedAt)) {
    value.onChildrenFinishedProcessingFinishedAt = dbValue.onChildrenFinishedProcessingFinishedAt
  }

  if (dbValue.finalize) {
    value.finalize = dbValue.finalize
  }

  if (isTruthyNumber(dbValue.closeExpiresAt)) {
    value.closeExpiresAt = dbValue.closeExpiresAt
  }

  if (isTruthyNumber(dbValue.closedAt)) {
    value.closedAt = dbValue.closedAt
  }

  return update
    ? applyTaskExecutionStorageUpdate(value, update, updateExpiresAtWithStartedAt)
    : value
}

export function taskExecutionStorageUpdateToDBUpdate(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdate {
  const dbUpdate = omitUndefinedValues({
    ...update,
    children: update.children ? [...update.children] : undefined,
    unset: undefined,
  })

  const updateUnset: Partial<Record<string, boolean>> | undefined = update.unset
  if (updateUnset) {
    for (const key in updateUnset) {
      if (updateUnset[key]) {
        // @ts-expect-error - This is safe because we know the key is valid
        dbUpdate[key] = null
      }
    }
  }

  return dbUpdate
}
