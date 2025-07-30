import { createDurableExecutorError } from './errors'
import type {
  DurableFunctionExecution,
  DurableWorkflowChildExecution,
  DurableWorkflowExecution,
} from './function'

/**
 * A durable storage with support for transactions.
 *
 * @category Storage
 */
export type DurableStorage = {
  withTransaction: <T>(fn: (tx: DurableStorageTx) => Promise<T>) => Promise<T>
}

/**
 * A durable storage transaction. It is used to perform multiple operations on the storage in a
 * single transaction.
 *
 * @category Storage
 */
export type DurableStorageTx = {
  /**
   * Insert durable function executions.
   *
   * @param executions - The durable function executions to insert.
   */
  insertFunctionExecutions: (
    executions: Array<DurableFunctionExecutionStorageObject>,
  ) => Promise<void>
  /**
   * Get durable function executions.
   *
   * @param where - The where clause to filter the durable function executions.
   * @param limit - The maximum number of durable function executions to return.
   * @returns The durable function executions.
   */
  getFunctionExecutions: (
    where: DurableFunctionExecutionStorageWhere,
    limit?: number,
  ) => Promise<Array<DurableFunctionExecutionStorageObject>>
  /**
   * Update durable function executions.
   *
   * @param where - The where clause to filter the durable function executions.
   * @param update - The update object.
   * @param limit - The maximum number of durable function executions to update.
   * @returns The ids of the durable function executions that were updated.
   */
  updateFunctionExecutions: (
    where: DurableFunctionExecutionStorageWhere,
    update: DurableFunctionExecutionStorageObjectUpdate,
    limit?: number,
  ) => Promise<Array<string>>
}

/**
 * A storage object for a durable function execution. It includes both durable function and
 * durable workflow executions.
 *
 * @category Storage
 */
export type DurableFunctionExecutionStorageObject = {
  /**
   * The root workflow of the execution. It is only present for durable workflow executions.
   */
  rootWorkflow?: {
    id: string
    executionId: string
  }

  /**
   * The parent workflow of the execution. It is only present for durable workflow executions.
   */
  parentWorkflow?: {
    id: string
    executionId: string
  }

  /**
   * The type of the execution.
   */
  type: 'function' | 'workflow'
  /**
   * The id of the function or workflow.
   */
  fnId: string
  /**
   * The id of the execution.
   */
  executionId: string
  /**
   * The children of the execution. It is only present for durable workflow executions.
   */
  children?: Array<DurableWorkflowChildExecution>
  /**
   * The number of children that have been completed. It is only present for durable workflow
   * executions.
   */
  childrenCompletedCount: number
  /**
   * Whether the workflow is parallel. It is only present for durable workflow executions.
   */
  isParallel?: boolean
  /**
   * The error of the execution.
   */
  error: {
    message: string
    tag: string
    isRetryable: boolean
  }
  /**
   * The status of the execution.
   */
  status: DurableFunctionExecutionStorageStatus
  /**
   * Whether the execution is finalized. Once the execution is finished, a background process
   * will update the status of its parent workflow if present and children if present.
   */
  isFinalized: boolean
  /**
   * Whether the execution needs a promise cancellation.
   */
  needsPromiseCancellation: boolean
  /**
   * The number of attempts the execution has been retried.
   */
  retryAttempts: number
  /**
   * The start time of the execution. Used for delaying the execution.
   */
  startAt: Date
  /**
   * The time the execution started.
   */
  startedAt?: Date
  /**
   * The time the execution finished.
   */
  finishedAt?: Date
  /**
   * The time the execution expires.
   */
  expiresAt: Date
  /**
   * The time the execution was created.
   */
  createdAt: Date
  /**
   * The time the execution was updated.
   */
  updatedAt: Date
}

/**
 * The status of a durable function execution.
 *
 * @category Storage
 */
export type DurableFunctionExecutionStorageStatus =
  | 'ready'
  | 'running'
  | 'completed'
  | 'failed'
  | 'timed_out'
  | 'cancelled'

export const ALL_FUNCTION_EXECUTION_STATUSES = [
  'ready',
  'running',
  'completed',
  'failed',
  'timed_out',
  'cancelled',
] as Array<DurableFunctionExecutionStorageStatus>
export const ACTIVE_FUNCTION_EXECUTION_STATUSES = [
  'ready',
  'running',
] as Array<DurableFunctionExecutionStorageStatus>
export const FINISHED_FUNCTION_EXECUTION_STATUSES = [
  'completed',
  'failed',
  'timed_out',
  'cancelled',
] as Array<DurableFunctionExecutionStorageStatus>

/**
 * The where clause for durable function execution storage. Storage objects are filtered by
 * the where clause before being returned.
 *
 * Storage implementations should index based on these clauses to optimize the performance of
 * the storage operations.
 *
 * @category Storage
 */
export type DurableFunctionExecutionStorageWhere =
  | {
      type: 'by_ids'
      ids: Array<string>
      statuses?: Array<DurableFunctionExecutionStorageStatus>
      needsPromiseCancellation?: boolean
    }
  | {
      type: 'by_statuses'
      statuses: Array<DurableFunctionExecutionStorageStatus>
      isFinalized?: boolean
      expiresAtLessThan?: Date
    }
  | {
      type: 'by_start_at'
      startAtLessThan: Date
      statuses?: Array<DurableFunctionExecutionStorageStatus>
    }

export const EXPIRES_AT_INFINITY = new Date(Number.MAX_SAFE_INTEGER)

/**
 * The update object for a durable function execution.
 *
 * @category Storage
 */
export type DurableFunctionExecutionStorageObjectUpdate = {
  children?: Array<DurableWorkflowChildExecution>
  childrenCompletedCount?: number
  error?: {
    message: string
    tag: string
    isRetryable: boolean
  }
  status?: DurableFunctionExecutionStorageStatus
  isFinalized?: boolean
  needsPromiseCancellation?: boolean
  retryAttempts?: number
  startAt?: Date
  startedAt?: Date
  finishedAt?: Date
  expiresAt?: Date
  updatedAt?: Date
}

export function convertExecutionStorageObjectToFunctionExecution(
  execution: DurableFunctionExecutionStorageObject,
): DurableFunctionExecution {
  if (execution.type !== 'function') {
    throw new Error('Execution is not a function execution')
  }

  const error = execution.error?.tag
    ? createDurableExecutorError(
        execution.error.message,
        execution.error.tag,
        execution.error.isRetryable,
      )
    : undefined

  switch (execution.status) {
    case 'ready': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        status: 'ready',
        error,
        retryAttempts: execution.retryAttempts,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'running': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        status: 'running',
        error,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'completed': {
      return {
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        error,
        status: 'completed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'failed': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        error: error!,
        status: 'failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'timed_out': {
      return {
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        error,
        status: 'timed_out',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'cancelled': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        fnId: execution.fnId,
        executionId: execution.executionId,
        error,
        status: 'cancelled',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    default: {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw new Error(`Unknown execution status: ${execution.status}`)
    }
  }
}

export function convertExecutionStorageObjectToWorkflowExecution(
  execution: DurableFunctionExecutionStorageObject,
): DurableWorkflowExecution {
  if (execution.type !== 'workflow') {
    throw new Error('Execution is not a workflow execution')
  }

  const children = execution.children ?? []
  const isParallel = execution.isParallel ?? false
  const error = execution.error?.tag
    ? createDurableExecutorError(
        execution.error.message,
        execution.error.tag,
        execution.error.isRetryable,
      )
    : undefined

  switch (execution.status) {
    case 'ready': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'ready',
        error,
        retryAttempts: execution.retryAttempts,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'running': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'running',
        error,
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'completed': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        error,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'completed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'failed': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        error: error!,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'failed',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'timed_out': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        error,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'timed_out',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    case 'cancelled': {
      return {
        rootWorkflow: execution.rootWorkflow,
        parentWorkflow: execution.parentWorkflow,
        workflowId: execution.fnId,
        executionId: execution.executionId,
        error,
        children,
        childrenCompletedCount: execution.childrenCompletedCount,
        isParallel,
        status: 'cancelled',
        retryAttempts: execution.retryAttempts,
        startedAt: execution.startedAt!,
        finishedAt: execution.finishedAt!,
        expiresAt: execution.expiresAt,
        createdAt: execution.createdAt,
        updatedAt: execution.updatedAt,
      }
    }
    default: {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw new Error(`Unknown execution status: ${execution.status}`)
    }
  }
}
