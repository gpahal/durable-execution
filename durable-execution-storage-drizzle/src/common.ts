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

export type TaskExecutionDBValue = {
  rootTaskId?: string | null
  rootExecutionId?: string | null
  parentTaskId?: string | null
  parentExecutionId?: string | null
  indexInParentChildTaskExecutions?: number | null
  isFinalizeTaskOfParentTask?: boolean | null

  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  input: string
  status: TaskExecutionStatus
  runOutput?: string | null
  output?: string | null
  error?: DurableExecutionErrorStorageValue | null
  retryAttempts: number
  startAt: Date
  startedAt?: Date | null
  expiresAt?: Date | null
  finishedAt?: Date | null

  children?: Array<TaskExecutionSummary> | null
  activeChildrenCount: number
  onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: Date | null
  onChildrenFinishedProcessingFinishedAt?: Date | null

  finalize?: TaskExecutionSummary | null

  closeStatus: TaskExecutionCloseStatus
  closeExpiresAt?: Date | null
  closedAt?: Date | null

  needsPromiseCancellation: boolean

  createdAt: Date
  updatedAt: Date
}

export type TaskExecutionDBUpdateValue = {
  status?: TaskExecutionStatus
  runOutput?: string | null
  output?: string
  error?: DurableExecutionErrorStorageValue | null
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  expiresAt?: Date | null
  finishedAt?: Date

  children?: Array<TaskExecutionSummary>
  activeChildrenCount?: number
  decrementParentActiveChildrenCount?: boolean
  onChildrenFinishedProcessingStatus?: TaskExecutionOnChildrenFinishedProcessingStatus
  onChildrenFinishedProcessingExpiresAt?: Date | null
  onChildrenFinishedProcessingFinishedAt?: Date

  finalize?: TaskExecutionSummary

  closeStatus?: TaskExecutionCloseStatus
  closeExpiresAt?: Date | null
  closedAt?: Date

  needsPromiseCancellation?: boolean

  updatedAt: Date
}

export function taskExecutionStorageValueToInsertValue(
  value: TaskExecutionStorageValue,
): TaskExecutionDBValue {
  return {
    rootTaskId: value.root?.taskId,
    rootExecutionId: value.root?.executionId,
    parentTaskId: value.parent?.taskId,
    parentExecutionId: value.parent?.executionId,
    indexInParentChildTaskExecutions: value.parent?.indexInParentChildTaskExecutions,
    isFinalizeTaskOfParentTask: value.parent?.isFinalizeTaskOfParentTask,
    taskId: value.taskId,
    executionId: value.executionId,
    retryOptions: value.retryOptions,
    sleepMsBeforeRun: value.sleepMsBeforeRun,
    timeoutMs: value.timeoutMs,
    input: value.input,
    status: value.status,
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

export function taskExecutionSelectValueToStorageValue(
  row: TaskExecutionDBValue,
): TaskExecutionStorageValue {
  const obj: TaskExecutionStorageValue = {
    taskId: row.taskId,
    executionId: row.executionId,
    retryOptions: row.retryOptions,
    sleepMsBeforeRun: row.sleepMsBeforeRun,
    timeoutMs: row.timeoutMs,
    input: row.input,
    status: row.status,
    retryAttempts: row.retryAttempts,
    startAt: row.startAt,
    activeChildrenCount: row.activeChildrenCount,
    onChildrenFinishedProcessingStatus: row.onChildrenFinishedProcessingStatus,
    closeStatus: row.closeStatus,
    needsPromiseCancellation: row.needsPromiseCancellation,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  }

  if (row.rootTaskId && row.rootExecutionId) {
    obj.root = {
      taskId: row.rootTaskId,
      executionId: row.rootExecutionId,
    }
  }

  if (row.parentTaskId && row.parentExecutionId) {
    obj.parent = {
      taskId: row.parentTaskId,
      executionId: row.parentExecutionId,
      indexInParentChildTaskExecutions: row.indexInParentChildTaskExecutions ?? 0,
      isFinalizeTaskOfParentTask: row.isFinalizeTaskOfParentTask ?? false,
    }
  }

  if (row.runOutput != null) {
    obj.runOutput = row.runOutput
  }

  if (row.output != null) {
    obj.output = row.output
  }

  if (row.error) {
    obj.error = row.error
  }

  if (row.startedAt) {
    obj.startedAt = row.startedAt
  }

  if (row.expiresAt) {
    obj.expiresAt = row.expiresAt
  }

  if (row.finishedAt) {
    obj.finishedAt = row.finishedAt
  }

  if (row.children) {
    obj.children = row.children
  }

  if (row.onChildrenFinishedProcessingExpiresAt) {
    obj.onChildrenFinishedProcessingExpiresAt = row.onChildrenFinishedProcessingExpiresAt
  }

  if (row.onChildrenFinishedProcessingFinishedAt) {
    obj.onChildrenFinishedProcessingFinishedAt = row.onChildrenFinishedProcessingFinishedAt
  }

  if (row.finalize) {
    obj.finalize = row.finalize
  }

  if (row.closeExpiresAt) {
    obj.closeExpiresAt = row.closeExpiresAt
  }

  if (row.closedAt) {
    obj.closedAt = row.closedAt
  }

  return obj
}

export function taskExecutionStorageValueToUpdateValue(update: TaskExecutionStorageUpdate): {
  dbUpdate: TaskExecutionDBUpdateValue
  decrementParentActiveChildrenCount: boolean
} {
  const dbUpdate: TaskExecutionDBUpdateValue = {
    updatedAt: update.updatedAt,
  }

  if (update.status != null) {
    dbUpdate.status = update.status
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

  return {
    dbUpdate,
    decrementParentActiveChildrenCount: update.decrementParentActiveChildrenCount ?? false,
  }
}
