import {
  applyTaskExecutionStorageUpdate,
  type DurableExecutionErrorStorageValue,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionStatus,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
  type TaskExecutionSummary,
  type TaskRetryOptions,
} from 'durable-execution'

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
  startedAt?: number
  expiresAt?: number | null
  finishedAt?: number | null

  children?: Array<TaskExecutionSummary>
  activeChildrenCount?: number
  shouldDecrementParentActiveChildrenCount?: boolean
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: number | null
  onChildrenFinishedProcessingFinishedAt?: number | null

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: number | null
  closedAt?: number | null

  needsPromiseCancellation?: boolean

  updatedAt: number
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

  if (dbValue.onChildrenFinishedProcessingExpiresAt) {
    value.onChildrenFinishedProcessingExpiresAt = dbValue.onChildrenFinishedProcessingExpiresAt
  }

  if (dbValue.onChildrenFinishedProcessingFinishedAt) {
    value.onChildrenFinishedProcessingFinishedAt = dbValue.onChildrenFinishedProcessingFinishedAt
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

export function taskExecutionStorageUpdateToDBUpdate(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdate {
  const dbUpdate: TaskExecutionDBUpdate = {
    updatedAt: update.updatedAt,
  }

  if (update.executorId != null) {
    dbUpdate.executorId = update.executorId
  }

  if (update.unsetExecutorId) {
    dbUpdate.executorId = null
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
    dbUpdate.runOutput = null
  }

  if (update.output != null) {
    dbUpdate.output = update.output
  }

  if (update.error != null) {
    dbUpdate.error = update.error
  }

  if (update.unsetError) {
    dbUpdate.error = null
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
    dbUpdate.expiresAt = null
  }

  if (update.finishedAt != null) {
    dbUpdate.finishedAt = update.finishedAt
  }

  if (update.children != null) {
    dbUpdate.children = update.children
  }

  if (update.activeChildrenCount != null) {
    dbUpdate.activeChildrenCount = update.activeChildrenCount
  }

  if (update.onChildrenFinishedProcessingStatus != null) {
    dbUpdate.onChildrenFinishedProcessingStatus = update.onChildrenFinishedProcessingStatus
  }

  if (update.onChildrenFinishedProcessingExpiresAt != null) {
    dbUpdate.onChildrenFinishedProcessingExpiresAt = update.onChildrenFinishedProcessingExpiresAt
  }

  if (update.unsetOnChildrenFinishedProcessingExpiresAt) {
    dbUpdate.onChildrenFinishedProcessingExpiresAt = null
  }

  if (update.onChildrenFinishedProcessingFinishedAt != null) {
    dbUpdate.onChildrenFinishedProcessingFinishedAt = update.onChildrenFinishedProcessingFinishedAt
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
    dbUpdate.closeExpiresAt = null
  }

  if (update.closedAt != null) {
    dbUpdate.closedAt = update.closedAt
  }

  if (update.needsPromiseCancellation != null) {
    dbUpdate.needsPromiseCancellation = update.needsPromiseCancellation
  }

  return dbUpdate
}
