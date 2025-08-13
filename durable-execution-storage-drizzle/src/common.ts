import type {
  ChildTaskExecution,
  ChildTaskExecutionErrorStorageValue,
  DurableExecutionErrorStorageValue,
  TaskExecutionStatusStorageValue,
  TaskExecutionStorageUpdate,
  TaskExecutionStorageValue,
  TaskRetryOptions,
} from 'durable-execution'

export type TaskExecutionDBValue = {
  rootTaskId?: string | null
  rootExecutionId?: string | null
  parentTaskId?: string | null
  parentChildTaskExecutionIndex?: number | null
  parentExecutionId?: string | null
  isFinalizeTask?: boolean | null
  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  status: TaskExecutionStatusStorageValue
  input: string
  runOutput?: string | null
  output?: string | null
  error?: DurableExecutionErrorStorageValue | null
  needsPromiseCancellation: boolean
  retryAttempts: number
  startAt: Date
  startedAt?: Date | null
  expiresAt?: Date | null
  finishedAt?: Date | null

  childrenTaskExecutions?: Array<ChildTaskExecution> | null
  completedChildrenTaskExecutions?: Array<ChildTaskExecution> | null
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue> | null

  finalizeTaskExecution?: ChildTaskExecution | null
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue | null

  isClosed: boolean
  closedAt?: Date | null

  createdAt: Date
  updatedAt: Date
}

export type TaskExecutionDBUpdateValue = {
  status?: TaskExecutionStatusStorageValue
  runOutput?: string
  output?: string
  error?: DurableExecutionErrorStorageValue | null
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  expiresAt?: Date | null
  finishedAt?: Date

  childrenTaskExecutions?: Array<ChildTaskExecution>
  completedChildrenTaskExecutions?: Array<ChildTaskExecution>
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue>

  finalizeTaskExecution?: ChildTaskExecution
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue

  isClosed?: boolean
  closedAt?: Date

  updatedAt?: Date
}

export function taskExecutionStorageValueToInsertValue(
  value: TaskExecutionStorageValue,
): TaskExecutionDBValue {
  return {
    rootTaskId: value.rootTaskExecution?.taskId,
    rootExecutionId: value.rootTaskExecution?.executionId,
    parentTaskId: value.parentTaskExecution?.taskId,
    parentExecutionId: value.parentTaskExecution?.executionId,
    isFinalizeTask: value.parentTaskExecution?.isFinalizeTask,
    taskId: value.taskId,
    executionId: value.executionId,
    retryOptions: value.retryOptions,
    sleepMsBeforeRun: value.sleepMsBeforeRun,
    timeoutMs: value.timeoutMs,
    status: value.status,
    input: value.input,
    runOutput: value.runOutput,
    output: value.output,
    error: value.error,
    needsPromiseCancellation: value.needsPromiseCancellation,
    retryAttempts: value.retryAttempts,
    startAt: value.startAt,
    startedAt: value.startedAt,
    expiresAt: value.expiresAt,
    finishedAt: value.finishedAt,
    childrenTaskExecutions: value.childrenTaskExecutions,
    completedChildrenTaskExecutions: value.completedChildrenTaskExecutions,
    childrenTaskExecutionsErrors: value.childrenTaskExecutionsErrors,
    finalizeTaskExecution: value.finalizeTaskExecution,
    finalizeTaskExecutionError: value.finalizeTaskExecutionError,
    isClosed: value.isClosed,
    closedAt: value.closedAt,
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
    status: row.status,
    input: row.input,
    needsPromiseCancellation: row.needsPromiseCancellation,
    retryAttempts: row.retryAttempts,
    startAt: row.startAt,
    isClosed: row.isClosed,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  }

  if (row.rootTaskId && row.rootExecutionId) {
    obj.rootTaskExecution = {
      taskId: row.rootTaskId,
      executionId: row.rootExecutionId,
    }
  }

  if (row.parentTaskId && row.parentExecutionId) {
    obj.parentTaskExecution = {
      taskId: row.parentTaskId,
      executionId: row.parentExecutionId,
      parentChildTaskExecutionIndex: row.parentChildTaskExecutionIndex ?? 0,
      isFinalizeTask: row.isFinalizeTask ?? false,
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

  if (row.childrenTaskExecutions) {
    obj.childrenTaskExecutions = row.childrenTaskExecutions
  }

  if (row.completedChildrenTaskExecutions) {
    obj.completedChildrenTaskExecutions = row.completedChildrenTaskExecutions
  }

  if (row.childrenTaskExecutionsErrors) {
    obj.childrenTaskExecutionsErrors = row.childrenTaskExecutionsErrors
  }

  if (row.finalizeTaskExecution) {
    obj.finalizeTaskExecution = row.finalizeTaskExecution
  }

  if (row.finalizeTaskExecutionError) {
    obj.finalizeTaskExecutionError = row.finalizeTaskExecutionError
  }

  if (row.closedAt) {
    obj.closedAt = row.closedAt
  }

  return obj
}

export function taskExecutionStorageValueToUpdateValue(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdateValue {
  const row: TaskExecutionDBUpdateValue = {}
  if (update.status != null) {
    row.status = update.status
  }

  if (update.runOutput != null) {
    row.runOutput = update.runOutput
  }

  if (update.output != null) {
    row.output = update.output
  }

  if (update.error != null) {
    row.error = update.error
  }

  if (update.unsetError) {
    row.error = null
  }

  if (update.needsPromiseCancellation != null) {
    row.needsPromiseCancellation = update.needsPromiseCancellation
  }

  if (update.retryAttempts != null) {
    row.retryAttempts = update.retryAttempts
  }

  if (update.startAt != null) {
    row.startAt = update.startAt
  }

  if (update.startedAt != null) {
    row.startedAt = update.startedAt
  }

  if (update.expiresAt != null) {
    row.expiresAt = update.expiresAt
  }

  if (update.unsetExpiresAt) {
    row.expiresAt = null
  }

  if (update.finishedAt != null) {
    row.finishedAt = update.finishedAt
  }

  if (update.childrenTaskExecutions != null) {
    row.childrenTaskExecutions = update.childrenTaskExecutions
  }

  if (update.completedChildrenTaskExecutions != null) {
    row.completedChildrenTaskExecutions = update.completedChildrenTaskExecutions
  }

  if (update.childrenTaskExecutionsErrors != null) {
    row.childrenTaskExecutionsErrors = update.childrenTaskExecutionsErrors
  }

  if (update.finalizeTaskExecution != null) {
    row.finalizeTaskExecution = update.finalizeTaskExecution
  }

  if (update.finalizeTaskExecutionError != null) {
    row.finalizeTaskExecutionError = update.finalizeTaskExecutionError
  }

  if (update.isClosed != null) {
    row.isClosed = update.isClosed
  }

  if (update.closedAt != null) {
    row.closedAt = update.closedAt
  }

  if (update.updatedAt != null) {
    row.updatedAt = update.updatedAt
  }

  return row
}
