import type {
  DurableTaskChildErrorStorageObject,
  DurableTaskChildExecutionStorageObject,
  DurableTaskErrorStorageObject,
  DurableTaskExecutionStatus,
  DurableTaskExecutionStorageObject,
  DurableTaskExecutionStorageObjectUpdate,
  DurableTaskRetryOptions,
} from 'durable-execution'

export type DurableTaskExecutionDbValue = {
  rootTaskId?: string | null
  rootExecutionId?: string | null
  parentTaskId?: string | null
  parentExecutionId?: string | null
  isFinalizeTask?: boolean | null
  taskId: string
  executionId: string
  retryOptions: DurableTaskRetryOptions
  timeoutMs: number
  sleepMsBeforeRun: number
  runInput: string
  runOutput?: string | null
  output?: string | null
  childrenTasksCompletedCount: number
  childrenTasks?: Array<DurableTaskChildExecutionStorageObject> | null
  childrenTasksErrors?: Array<DurableTaskChildErrorStorageObject> | null
  finalizeTask?: DurableTaskChildExecutionStorageObject | null
  finalizeTaskError?: DurableTaskErrorStorageObject | null
  error?: DurableTaskErrorStorageObject | null
  status: DurableTaskExecutionStatus
  isClosed: boolean
  needsPromiseCancellation: boolean
  retryAttempts: number
  startAt: Date
  startedAt?: Date | null
  finishedAt?: Date | null
  expiresAt: Date
  createdAt: Date
  updatedAt: Date
}

export type DurableTaskExecutionDbUpdateValue = {
  runOutput?: string
  output?: string
  childrenTasksCompletedCount?: number
  childrenTasks?: Array<DurableTaskChildExecutionStorageObject>
  childrenTasksErrors?: Array<DurableTaskChildErrorStorageObject>
  finalizeTask?: DurableTaskChildExecutionStorageObject
  finalizeTaskError?: DurableTaskErrorStorageObject
  error?: DurableTaskErrorStorageObject | null
  status?: DurableTaskExecutionStatus
  isClosed?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  finishedAt?: Date
  expiresAt?: Date
  updatedAt?: Date
}

export function storageObjectToInsertValue(
  obj: DurableTaskExecutionStorageObject,
): DurableTaskExecutionDbValue {
  return {
    rootTaskId: obj.rootTask?.taskId,
    rootExecutionId: obj.rootTask?.executionId,
    parentTaskId: obj.parentTask?.taskId,
    parentExecutionId: obj.parentTask?.executionId,
    isFinalizeTask: obj.parentTask?.isFinalizeTask,
    taskId: obj.taskId,
    executionId: obj.executionId,
    retryOptions: obj.retryOptions,
    timeoutMs: obj.timeoutMs,
    sleepMsBeforeRun: obj.sleepMsBeforeRun,
    runInput: obj.runInput,
    runOutput: obj.runOutput,
    output: obj.output,
    childrenTasksCompletedCount: obj.childrenTasksCompletedCount,
    childrenTasks: obj.childrenTasks,
    childrenTasksErrors: obj.childrenTasksErrors,
    finalizeTask: obj.finalizeTask,
    finalizeTaskError: obj.finalizeTaskError,
    error: obj.error,
    status: obj.status,
    isClosed: obj.isClosed,
    needsPromiseCancellation: obj.needsPromiseCancellation,
    retryAttempts: obj.retryAttempts,
    startAt: obj.startAt,
    startedAt: obj.startedAt,
    finishedAt: obj.finishedAt,
    expiresAt: obj.expiresAt,
    createdAt: obj.createdAt,
    updatedAt: obj.updatedAt,
  }
}

export function selectValueToStorageObject(
  row: DurableTaskExecutionDbValue,
): DurableTaskExecutionStorageObject {
  const obj: DurableTaskExecutionStorageObject = {
    taskId: row.taskId,
    executionId: row.executionId,
    retryOptions: row.retryOptions,
    timeoutMs: row.timeoutMs,
    sleepMsBeforeRun: row.sleepMsBeforeRun,
    runInput: row.runInput,
    childrenTasksCompletedCount: row.childrenTasksCompletedCount,
    status: row.status,
    isClosed: row.isClosed,
    needsPromiseCancellation: row.needsPromiseCancellation,
    retryAttempts: row.retryAttempts,
    startAt: row.startAt,
    expiresAt: row.expiresAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  }

  if (row.rootTaskId && row.rootExecutionId) {
    obj.rootTask = {
      taskId: row.rootTaskId,
      executionId: row.rootExecutionId,
    }
  }

  if (row.parentTaskId && row.parentExecutionId) {
    obj.parentTask = {
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

  if (row.childrenTasks) {
    obj.childrenTasks = row.childrenTasks
  }

  if (row.childrenTasksErrors) {
    obj.childrenTasksErrors = row.childrenTasksErrors
  }

  if (row.finalizeTask) {
    obj.finalizeTask = row.finalizeTask
  }

  if (row.finalizeTaskError) {
    obj.finalizeTaskError = row.finalizeTaskError
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

  return obj
}

export function storageUpdateToUpdateValue(
  update: DurableTaskExecutionStorageObjectUpdate,
): DurableTaskExecutionDbUpdateValue {
  const row: DurableTaskExecutionDbUpdateValue = {}
  if (update.runOutput !== undefined) {
    row.runOutput = update.runOutput
  }

  if (update.output !== undefined) {
    row.output = update.output
  }

  if (update.childrenTasksCompletedCount !== undefined) {
    row.childrenTasksCompletedCount = update.childrenTasksCompletedCount
  }

  if (update.childrenTasks !== undefined) {
    row.childrenTasks = update.childrenTasks
  }

  if (update.childrenTasksErrors !== undefined) {
    row.childrenTasksErrors = update.childrenTasksErrors
  }

  if (update.finalizeTask !== undefined) {
    row.finalizeTask = update.finalizeTask
  }

  if (update.finalizeTaskError !== undefined) {
    row.finalizeTaskError = update.finalizeTaskError
  }

  if (update.error !== undefined) {
    row.error = update.error
  }

  if (update.unsetError) {
    row.error = null
  }

  if (update.status !== undefined) {
    row.status = update.status
  }

  if (update.isClosed !== undefined) {
    row.isClosed = update.isClosed
  }

  if (update.needsPromiseCancellation !== undefined) {
    row.needsPromiseCancellation = update.needsPromiseCancellation
  }

  if (update.retryAttempts !== undefined) {
    row.retryAttempts = update.retryAttempts
  }

  if (update.startAt !== undefined) {
    row.startAt = update.startAt
  }

  if (update.startedAt !== undefined) {
    row.startedAt = update.startedAt
  }

  if (update.finishedAt !== undefined) {
    row.finishedAt = update.finishedAt
  }

  if (update.expiresAt !== undefined) {
    row.expiresAt = update.expiresAt
  }

  if (update.updatedAt !== undefined) {
    row.updatedAt = update.updatedAt
  }

  return row
}
