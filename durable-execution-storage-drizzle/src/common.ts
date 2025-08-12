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
  parentExecutionId?: string | null
  isFinalizeTask?: boolean | null
  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  timeoutMs: number
  sleepMsBeforeRun: number
  runInput: string
  runOutput?: string | null
  output?: string | null
  childrenTaskExecutionsCompletedCount: number
  childrenTaskExecutions?: Array<ChildTaskExecution> | null
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue> | null
  finalizeTaskExecution?: ChildTaskExecution | null
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue | null
  error?: DurableExecutionErrorStorageValue | null
  status: TaskExecutionStatusStorageValue
  isClosed: boolean
  needsPromiseCancellation: boolean
  retryAttempts: number
  startAt: Date
  startedAt?: Date | null
  finishedAt?: Date | null
  expiresAt?: Date | null
  version: number
  createdAt: Date
  updatedAt: Date
}

export type TaskExecutionDBUpdateValue = {
  runOutput?: string
  output?: string
  childrenTaskExecutionsCompletedCount?: number
  childrenTaskExecutions?: Array<ChildTaskExecution>
  childrenTaskExecutionsErrors?: Array<ChildTaskExecutionErrorStorageValue>
  finalizeTaskExecution?: ChildTaskExecution
  finalizeTaskExecutionError?: DurableExecutionErrorStorageValue
  error?: DurableExecutionErrorStorageValue | null
  status?: TaskExecutionStatusStorageValue
  isClosed?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  finishedAt?: Date
  expiresAt?: Date | null
  version?: number
  updatedAt?: Date
}

export function storageValueToInsertValue(value: TaskExecutionStorageValue): TaskExecutionDBValue {
  return {
    rootTaskId: value.rootTaskExecution?.taskId,
    rootExecutionId: value.rootTaskExecution?.executionId,
    parentTaskId: value.parentTaskExecution?.taskId,
    parentExecutionId: value.parentTaskExecution?.executionId,
    isFinalizeTask: value.parentTaskExecution?.isFinalizeTask,
    taskId: value.taskId,
    executionId: value.executionId,
    retryOptions: value.retryOptions,
    timeoutMs: value.timeoutMs,
    sleepMsBeforeRun: value.sleepMsBeforeRun,
    runInput: value.runInput,
    runOutput: value.runOutput,
    output: value.output,
    childrenTaskExecutionsCompletedCount: value.childrenTaskExecutionsCompletedCount,
    childrenTaskExecutions: value.childrenTaskExecutions,
    childrenTaskExecutionsErrors: value.childrenTaskExecutionsErrors,
    finalizeTaskExecution: value.finalizeTaskExecution,
    finalizeTaskExecutionError: value.finalizeTaskExecutionError,
    error: value.error,
    status: value.status,
    isClosed: value.isClosed,
    needsPromiseCancellation: value.needsPromiseCancellation,
    retryAttempts: value.retryAttempts,
    startAt: value.startAt,
    startedAt: value.startedAt,
    finishedAt: value.finishedAt,
    expiresAt: value.expiresAt,
    version: value.version,
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  }
}

export function selectValueToStorageValue(row: TaskExecutionDBValue): TaskExecutionStorageValue {
  const obj: TaskExecutionStorageValue = {
    taskId: row.taskId,
    executionId: row.executionId,
    retryOptions: row.retryOptions,
    timeoutMs: row.timeoutMs,
    sleepMsBeforeRun: row.sleepMsBeforeRun,
    runInput: row.runInput,
    childrenTaskExecutionsCompletedCount: row.childrenTaskExecutionsCompletedCount,
    status: row.status,
    isClosed: row.isClosed,
    needsPromiseCancellation: row.needsPromiseCancellation,
    retryAttempts: row.retryAttempts,
    startAt: row.startAt,
    version: row.version,
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
      isFinalizeTask: row.isFinalizeTask ?? false,
    }
  }

  if (row.runOutput != null) {
    obj.runOutput = row.runOutput
  }

  if (row.output != null) {
    obj.output = row.output
  }

  if (row.childrenTaskExecutions) {
    obj.childrenTaskExecutions = row.childrenTaskExecutions
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

  if (row.error) {
    obj.error = row.error
  }

  if (row.startedAt) {
    obj.startedAt = row.startedAt
  }

  if (row.finishedAt) {
    obj.finishedAt = row.finishedAt
  }

  if (row.expiresAt) {
    obj.expiresAt = row.expiresAt
  }

  return obj
}

export function storageValueToUpdateValue(
  update: TaskExecutionStorageUpdate,
): TaskExecutionDBUpdateValue {
  const row: TaskExecutionDBUpdateValue = {}
  if (update.runOutput != null) {
    row.runOutput = update.runOutput
  }

  if (update.output != null) {
    row.output = update.output
  }

  if (update.childrenTaskExecutionsCompletedCount != null) {
    row.childrenTaskExecutionsCompletedCount = update.childrenTaskExecutionsCompletedCount
  }

  if (update.childrenTaskExecutions != null) {
    row.childrenTaskExecutions = update.childrenTaskExecutions
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

  if (update.error != null) {
    row.error = update.error
  }

  if (update.unsetError) {
    row.error = null
  }

  if (update.status != null) {
    row.status = update.status
  }

  if (update.isClosed != null) {
    row.isClosed = update.isClosed
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

  if (update.finishedAt != null) {
    row.finishedAt = update.finishedAt
  }

  if (update.expiresAt != null) {
    row.expiresAt = update.expiresAt
  }

  if (update.unsetExpiresAt) {
    row.expiresAt = null
  }

  if (update.updatedAt != null) {
    row.updatedAt = update.updatedAt
  }

  return row
}
