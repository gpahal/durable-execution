import type {
  DurableTaskChildErrorStorageObject,
  DurableTaskChildExecutionStorageObject,
  DurableTaskErrorStorageObject,
  DurableTaskExecutionStatus,
  DurableTaskExecutionStorageObject,
  DurableTaskExecutionStorageObjectUpdate,
} from 'durable-execution'

export type DurableTaskExecutionDbValue = {
  rootTaskId?: string | null
  rootExecutionId?: string | null
  parentTaskId?: string | null
  parentExecutionId?: string | null
  isOnRunAndChildrenCompleteChild?: boolean | null
  taskId: string
  executionId: string
  runInput: string
  runOutput?: string | null
  output?: string | null
  childrenCompletedCount: number
  children?: Array<DurableTaskChildExecutionStorageObject> | null
  childrenErrors?: Array<DurableTaskChildErrorStorageObject> | null
  onRunAndChildrenComplete?: DurableTaskChildExecutionStorageObject | null
  onRunAndChildrenCompleteError?: DurableTaskErrorStorageObject | null
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
  childrenCompletedCount?: number
  children?: Array<DurableTaskChildExecutionStorageObject>
  childrenErrors?: Array<DurableTaskChildErrorStorageObject>
  onRunAndChildrenComplete?: DurableTaskChildExecutionStorageObject
  onRunAndChildrenCompleteError?: DurableTaskErrorStorageObject
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
    isOnRunAndChildrenCompleteChild: obj.parentTask?.isOnRunAndChildrenCompleteChild,
    taskId: obj.taskId,
    executionId: obj.executionId,
    runInput: obj.runInput,
    runOutput: obj.runOutput,
    output: obj.output,
    childrenCompletedCount: obj.childrenCompletedCount,
    children: obj.children,
    childrenErrors: obj.childrenErrors,
    onRunAndChildrenComplete: obj.onRunAndChildrenComplete,
    onRunAndChildrenCompleteError: obj.onRunAndChildrenCompleteError,
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
    runInput: row.runInput,
    childrenCompletedCount: row.childrenCompletedCount,
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
      isOnRunAndChildrenCompleteChild: row.isOnRunAndChildrenCompleteChild || false,
    }
  }

  if (row.runOutput != null) {
    obj.runOutput = row.runOutput
  }

  if (row.output != null) {
    obj.output = row.output
  }

  if (row.children) {
    obj.children = row.children
  }

  if (row.childrenErrors) {
    obj.childrenErrors = row.childrenErrors
  }

  if (row.onRunAndChildrenComplete) {
    obj.onRunAndChildrenComplete = row.onRunAndChildrenComplete
  }

  if (row.onRunAndChildrenCompleteError) {
    obj.onRunAndChildrenCompleteError = row.onRunAndChildrenCompleteError
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

  if (update.childrenCompletedCount !== undefined) {
    row.childrenCompletedCount = update.childrenCompletedCount
  }

  if (update.children !== undefined) {
    row.children = update.children
  }

  if (update.childrenErrors !== undefined) {
    row.childrenErrors = update.childrenErrors
  }

  if (update.onRunAndChildrenComplete !== undefined) {
    row.onRunAndChildrenComplete = update.onRunAndChildrenComplete
  }

  if (update.onRunAndChildrenCompleteError !== undefined) {
    row.onRunAndChildrenCompleteError = update.onRunAndChildrenCompleteError
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
