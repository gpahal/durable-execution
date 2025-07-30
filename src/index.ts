import { getErrorMessage } from '@gpahal/std/errors'
import { sleep } from '@gpahal/std/promises'

import { createCancellablePromise, createCancelSignal, type CancelSignal } from './cancel'
import {
  createDurableExecutorError,
  DurableExecutorCancelledError,
  DurableExecutorError,
  DurableExecutorTimedOutError,
} from './errors'
import {
  getDurableFunctionDelayMsAfterAttempt,
  getDurableFunctionTimeoutMs,
  type DurableFunction,
  type DurableFunctionContext,
  type DurableFunctionExecution,
  type DurableFunctionFinishedExecution,
  type DurableWorkflow,
  type DurableWorkflowChild,
  type DurableWorkflowChildExecution,
  type DurableWorkflowExecution,
  type DurableWorkflowFinishedExecution,
} from './function'
import { createConsoleLogger, createLoggerDebugDisabled, type Logger } from './logger'
import {
  ACTIVE_FUNCTION_EXECUTION_STATUSES,
  convertExecutionStorageObjectToFunctionExecution,
  convertExecutionStorageObjectToWorkflowExecution,
  EXPIRES_AT_INFINITY,
  FINISHED_FUNCTION_EXECUTION_STATUSES,
  type DurableFunctionExecutionStorageObject,
  type DurableFunctionExecutionStorageObjectUpdate,
  type DurableFunctionExecutionStorageStatus,
  type DurableStorage,
  type DurableStorageTx,
} from './storage'
import { sleepWithJitter, validateId } from './utils'

export {
  type CancelSignal,
  createCancelSignal,
  createTimeoutCancelSignal,
  createCancellablePromise,
} from './cancel'
export { DurableExecutorError, DurableExecutorCancelledError } from './errors'
export type {
  DurableFunction,
  DurableFunctionContext,
  DurableFunctionReadyExecution,
  DurableFunctionRunningExecution,
  DurableFunctionCompletedExecution,
  DurableFunctionFailedExecution,
  DurableFunctionTimedOutExecution,
  DurableFunctionCancelledExecution,
  DurableFunctionExecution,
  DurableFunctionFinishedExecution,
  DurableWorkflow,
  DurableWorkflowChild,
  DurableWorkflowFunctionChild,
  DurableWorkflowWorkflowChild,
  DurableWorkflowExecution,
  DurableWorkflowReadyExecution,
  DurableWorkflowRunningExecution,
  DurableWorkflowCompletedExecution,
  DurableWorkflowFailedExecution,
  DurableWorkflowTimedOutExecution,
  DurableWorkflowCancelledExecution,
  DurableWorkflowChildExecution,
  DurableWorkflowFinishedExecution,
} from './function'
export { createConsoleLogger, type Logger } from './logger'
export type {
  DurableStorage,
  DurableStorageTx,
  DurableFunctionExecutionStorageStatus,
  DurableFunctionExecutionStorageObject,
  DurableFunctionExecutionStorageObjectUpdate,
  DurableFunctionExecutionStorageWhere,
} from './storage'

/**
 * A handle to a durable function execution. See
 * [Durable function execution](https://gpahal.github.io/durable-execution/index.html#durable-function-execution)
 * docs for more details on how function executions work.
 *
 * @example
 * ```ts
 * const handle = await durableExecutor.enqueueFunction('uploadFile', 'uploadFileExecution0')
 * const execution = await handle.getExecution() // Get the execution
 *
 * const finishedExecution = await handle.waitAndGetExecution() // Wait for the execution to be finished and get it
 * if (finishedExecution.status === 'completed') {
 *   // Do something with the result
 * } else if (finishedExecution.status === 'failed') {
 *   // Do something with the error
 * } else if (finishedExecution.status === 'timed_out') {
 *   // Do something with the timeout
 * } else if (finishedExecution.status === 'cancelled') {
 *   // Do something with the cancellation
 * }
 * ```
 *
 * @category Functions
 */
export type DurableFunctionHandle = {
  /**
   * Get the durable function execution.
   *
   * @returns The durable function execution.
   */
  getExecution: () => Promise<DurableFunctionExecution>
  /**
   * Wait for the durable function execution to be ready and get it.
   *
   * @param options - The options for waiting for the durable function execution.
   * @returns The durable function execution.
   */
  waitAndGetExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    delayMs?: number
  }) => Promise<DurableFunctionFinishedExecution>
  /**
   * Cancel the durable function execution.
   */
  cancel: () => Promise<void>
}

/**
 * A handle to a durable workflow execution. See
 * [Durable workflow execution](https://gpahal.github.io/durable-execution/index.html#durable-workflow-execution)
 * docs for more details on how workflow executions work.
 *
 * @example
 * ```ts
 * const handle = await durableExecutor.enqueueWorkflow('fileWorkflow', 'fileWorkflowExecution0')
 * const execution = await handle.getExecution() // Get the execution
 *
 * const finishedExecution = await handle.waitAndGetExecution() // Wait for the execution to be finished and get it
 * if (finishedExecution.status === 'completed') {
 *   // Do something with the result
 * } else if (finishedExecution.status === 'failed') {
 *   // Do something with the error
 * } else if (finishedExecution.status === 'timed_out') {
 *   // Do something with the timeout
 * } else if (finishedExecution.status === 'cancelled') {
 *   // Do something with the cancellation
 * }
 * ```
 *
 * @category Workflows
 */
export type DurableWorkflowHandle = {
  /**
   * Get the durable workflow execution.
   *
   * @returns The durable workflow execution.
   */
  getExecution: () => Promise<DurableWorkflowExecution>
  /**
   * Wait for the durable workflow execution to be ready and get it.
   *
   * @param options - The options for waiting for the durable workflow execution.
   * @returns The durable workflow execution.
   */
  waitAndGetExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    delayMs?: number
  }) => Promise<DurableWorkflowFinishedExecution>
  /**
   * Cancel the durable workflow execution.
   */
  cancel: () => Promise<void>
}

/**
 * A durable executor. It is used to execute durable functions and workflows.
 *
 * NOTE: Multiple durable executors with the same storage will work in tandem. In this case, all
 * the functions and workflows should be present for all the durable executors.
 *
 * @example
 * ```ts
 * const durableExecutor = new DurableExecutor(storage)
 *
 * // Create a durable executor and manage its lifecycle
 * async function durableExecutorApp() {
 *   try {
 *     await durableExecutor.start()
 *   } finally {
 *     await durableExecutor.shutdown()
 *   }
 * }
 *
 * // Create durable functions and workflows
 * durableExecutor.addFunction({
 *   id: 'uploadFile',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... upload a file
 *   },
 * })
 *
 * durableExecutor.addFunction({
 *   id: 'extractFileTitle',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... extract the file title
 *   },
 * })
 *
 * durableExecutor.addFunction({
 *   id: 'summarizeFile',
 *   timeoutMs: 10000, // 10 seconds
 *   execute: async (ctx) => {
 *     // ... summarize the file
 *   },
 * })
 *
 * durableExecutor.addWorkflow({
 *   id: 'uploadedFileWorkflow',
 *   children: [
 *     { type: 'function', fnId: 'extractFileTitle' },
 *     { type: 'function', fnId: 'summarizeFile' },
 *   ],
 *   isParallel: true,
 * })
 *
 * durableExecutor.addWorkflow({
 *   id: 'fileWorkflow',
 *   children: [
 *     { type: 'function', fnId: 'uploadFile' },
 *     { type: 'workflow', workflowId: 'uploadedFileWorkflow' },
 *   ],
 *   isParallel: false,
 * })
 *
 * // Enqueue the functions and workflows
 * async function app() {
 *   // Enqueue function and manage its lifecycle
 *   const uploadFileHandle = await durableExecutor.enqueueFunction('uploadFile', 'uploadFileExecution0')
 *   const uploadFileExecution = await uploadFileHandle.getExecution()
 *   const uploadFileFinishedExecution = await uploadFileHandle.waitAndGetExecution()
 *   await uploadFileHandle.cancel()
 *
 *   // Enqueue workflow and manage its lifecycle
 *   const workflowHandle = await durableExecutor.enqueueWorkflow('fileWorkflow', 'fileWorkflowExecution0')
 *   const workflowExecution = await workflowHandle.getExecution()
 *   const workflowFinishedExecution = await workflowHandle.waitAndGetExecution()
 *   await workflowHandle.cancel()
 *
 *   console.log(uploadFileExecution, workflowExecution)
 * }
 *
 * // Start the durable executor and run the app
 * Promise.all([
 *   durableExecutorApp(),
 *   app(),
 * ])
 *
 * // Shutdown the durable executor when the app is done
 * await durableExecutor.shutdown()
 * ```
 *
 * @category Executor
 */
export class DurableExecutor {
  private readonly storage: DurableStorage
  private readonly logger: Logger
  private readonly fnMap: Map<
    string,
    { type: 'function'; fn: DurableFunction } | { type: 'workflow'; workflow: DurableWorkflow }
  >
  private readonly shutdownSignal: CancelSignal
  private readonly internalCancel: () => void
  private readonly runningExecutionsMap: Map<
    string,
    { type: 'function'; promise: Promise<void>; cancel: () => void } | { type: 'workflow' }
  >
  private startPromise: Promise<void> | undefined

  /**
   * Create a durable executor.
   *
   * @param storage - The storage to use for the durable executor.
   * @param options - The options for the durable executor.
   * @param options.logger - The logger to use for the durable executor. If not provided, a console
   *   logger will be used.
   * @param options.enableDebug - Whether to enable debug logging. If `true`, debug logging will
   *   be enabled.
   */
  constructor(
    storage: DurableStorage,
    { logger, enableDebug = false }: { logger?: Logger; enableDebug?: boolean } = {},
  ) {
    this.storage = storage
    this.logger = logger ?? createConsoleLogger('DurableExecutor')
    if (!enableDebug) {
      this.logger = createLoggerDebugDisabled(this.logger)
    }

    this.fnMap = new Map()
    const [cancelSignal, cancel] = createCancelSignal({ logger: this.logger })
    this.shutdownSignal = cancelSignal
    this.internalCancel = cancel
    this.runningExecutionsMap = new Map()
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw new Error('Executor shutdown')
    }
  }

  /**
   * Add a function to the durable executor.
   *
   * @param fn - The function to add.
   */
  addFunction(fn: DurableFunction): void {
    this.throwIfShutdown()
    validateId(fn.id)
    this.fnMap.set(fn.id, { type: 'function', fn })
    this.logger.debug(`Added function ${fn.id}`)
  }

  /**
   * Add a workflow to the durable executor.
   *
   * @param workflow - The workflow to add.
   */
  addWorkflow(workflow: DurableWorkflow): void {
    this.throwIfShutdown()
    this.validateWorkflowInternal(workflow)
    this.fnMap.set(workflow.id, { type: 'workflow', workflow })
    this.logger.debug(`Added workflow ${workflow.id}`)
  }

  private validateWorkflowInternal(workflow: DurableWorkflow): void {
    validateId(workflow.id)
    if (workflow.children.length === 0) {
      throw new Error(`Workflow ${workflow.id} must have at least one child`)
    }

    for (const child of workflow.children) {
      if (child.type === 'workflow') {
        this.validateWorkflowIdInternal(child.workflowId)
      }
    }
  }

  private validateWorkflowIdInternal(workflowId: string): void {
    validateId(workflowId)
    const existingChild = this.fnMap.get(workflowId)
    if (!existingChild) {
      throw new Error(
        `Workflow ${workflowId} not found. Use DurableExecutor.addWorkflow() to add it before enqueuing it.`,
      )
    }
    if (existingChild.type === 'function') {
      throw new Error(
        `Workflow ${workflowId} is already added as a function. Use unique ids for functions and workflows.`,
      )
    }

    this.validateWorkflowInternal(existingChild.workflow)
  }

  /**
   * Execute a function with a transaction. Supports retries.
   *
   * @param fn - The function to execute.
   * @param maxRetryAttempts - The maximum number of times to retry the transaction.
   * @returns The result of the function.
   */
  private async withTransaction<T>(
    fn: (tx: DurableStorageTx) => Promise<T>,
    maxRetryAttempts = 1,
  ): Promise<T> {
    if (maxRetryAttempts <= 0) {
      return await this.storage.withTransaction(fn)
    }

    for (let i = 0; ; i++) {
      try {
        return await this.storage.withTransaction(fn)
      } catch (error) {
        if (error instanceof DurableExecutorCancelledError) {
          throw error
        }
        if (i === maxRetryAttempts) {
          throw error
        }

        this.logger.error('Error in withTransaction', error)
        await sleepWithJitter(50)
      }
    }
  }

  /**
   * Execute a function with an existing transaction if present. Otherwise, execute the function
   * with a new transaction. Supports retries.
   *
   * @param fn - The function to execute.
   * @param tx - The transaction to use.
   * @param maxRetryAttempts - The maximum number of times to retry the transaction.
   * @returns The result of the function.
   */
  private async withExistingTransaction<T>(
    fn: (tx: DurableStorageTx) => Promise<T>,
    tx?: DurableStorageTx,
    maxRetryAttempts = 1,
  ): Promise<T> {
    if (tx) {
      return await fn(tx)
    }

    return await this.withTransaction(fn, maxRetryAttempts)
  }

  /**
   * Enqueue a function or workflow execution.
   */
  private async enqueueFunctionInternal(
    child: DurableWorkflowChild,
    executionId: string,
    {
      tx,
      rootWorkflow,
      parentWorkflow,
      children,
      isParallel,
      status,
    }: {
      tx?: DurableStorageTx
      rootWorkflow?: {
        id: string
        executionId: string
      }
      parentWorkflow?: {
        id: string
        executionId: string
      }
      children?: Array<DurableWorkflowChildExecution>
      isParallel?: boolean
      status?: DurableFunctionExecutionStorageStatus
    } = {},
  ): Promise<void> {
    const now = new Date()
    const txFn = async (tx: DurableStorageTx) => {
      await tx.insertFunctionExecutions([
        {
          rootWorkflow,
          parentWorkflow,
          type: child.type,
          fnId: child.type === 'function' ? child.fnId : child.workflowId,
          executionId: executionId,
          children,
          childrenCompletedCount: 0,
          isParallel,
          error: {
            message: '',
            tag: '',
            isRetryable: false,
          },
          status: status ?? 'ready',
          isFinalized: false,
          needsPromiseCancellation: false,
          retryAttempts: 0,
          startAt: now,
          expiresAt: EXPIRES_AT_INFINITY,
          createdAt: now,
          updatedAt: now,
        },
      ])
    }
    await this.withExistingTransaction(txFn, tx)
  }

  /**
   * Enqueue a function execution.
   *
   * @param fnId - The id of the function to enqueue.
   * @param executionId - The id of the execution. Should be unique among all the durable function
   *   and workflow executions in the same durable executor.
   * @returns A handle to the function execution.
   */
  async enqueueFunction(fnId: string, executionId: string): Promise<DurableFunctionHandle> {
    this.throwIfShutdown()
    validateId(fnId)
    validateId(executionId)

    const existingChild = this.fnMap.get(fnId)
    if (!existingChild) {
      throw new Error(
        `Function ${fnId} not found. Use DurableExecutor.addFunction() to add it before enqueuing it.`,
      )
    }
    if (existingChild.type === 'workflow') {
      throw new Error(
        `Function ${fnId} is already added as a workflow. Use unique ids for functions and workflows.`,
      )
    }

    await this.enqueueFunctionInternal({ type: 'function', fnId: existingChild.fn.id }, executionId)
    this.logger.debug(`Enqueued function ${fnId} with execution id ${executionId}`)

    return {
      getExecution: async () => {
        const execution = await this.withTransaction(async (tx) => {
          const executions = await tx.getFunctionExecutions({ type: 'by_ids', ids: [executionId] })
          return executions.length === 0 ? undefined : executions[0]
        })
        if (!execution) {
          throw new Error(`Execution ${executionId} not found`)
        }
        return convertExecutionStorageObjectToFunctionExecution(execution)
      },
      waitAndGetExecution: async ({
        signal,
        delayMs,
      }: {
        signal?: CancelSignal | AbortSignal
        delayMs?: number
      } = {}) => {
        const cancelSignal =
          signal instanceof AbortSignal
            ? createCancelSignal({ abortSignal: signal, logger: this.logger })[0]
            : signal
        let isFirstIteration = true
        while (true) {
          if (cancelSignal?.isCancelled()) {
            throw new Error('Execution cancelled')
          }

          if (isFirstIteration) {
            isFirstIteration = false
          } else {
            const finalDelayMs = delayMs && delayMs > 0 ? delayMs : 1000
            await createCancellablePromise(sleep(finalDelayMs), cancelSignal)

            if (cancelSignal?.isCancelled()) {
              throw new Error('Execution cancelled')
            }
          }

          const execution = await this.withTransaction(async (tx) => {
            const executions = await tx.getFunctionExecutions({
              type: 'by_ids',
              ids: [executionId],
            })
            return executions.length === 0 ? undefined : executions[0]
          })
          if (execution && FINISHED_FUNCTION_EXECUTION_STATUSES.includes(execution.status)) {
            return convertExecutionStorageObjectToFunctionExecution(
              execution,
            ) as DurableFunctionFinishedExecution
          } else {
            this.logger.debug(
              `Waiting for function ${executionId} to be finished. Status: ${execution?.status}`,
            )
          }
        }
      },
      cancel: async () => {
        const now = new Date()
        await this.withTransaction(async (tx) => {
          await tx.updateFunctionExecutions(
            { type: 'by_ids', ids: [executionId], statuses: ACTIVE_FUNCTION_EXECUTION_STATUSES },
            {
              error: {
                message: 'Function cancelled',
                tag: 'DurableExecutorCancelledError',
                isRetryable: false,
              },
              needsPromiseCancellation: true,
              status: 'cancelled',
              finishedAt: now,
              updatedAt: now,
            },
          )
        })
        this.logger.debug(`Cancelled function ${executionId}`)
      },
    }
  }

  /**
   * Enqueue a workflow execution. Handles the children of the workflow.
   */
  private async enqueueWorkflowInternal(
    workflowId: string,
    executionId: string,
    {
      tx,
      rootWorkflow,
      parentWorkflow,
    }: {
      tx?: DurableStorageTx
      rootWorkflow?: {
        id: string
        executionId: string
      }
      parentWorkflow?: {
        id: string
        executionId: string
      }
    } = {},
  ): Promise<void> {
    const existingChild = this.fnMap.get(workflowId)
    if (!existingChild) {
      throw new Error(
        `Workflow ${workflowId} not found. Use DurableExecutor.addWorkflow() to add it before enqueuing it.`,
      )
    }
    if (existingChild.type === 'function') {
      throw new Error(
        `Workflow ${workflowId} is already added as a function. Use unique ids for functions and workflows.`,
      )
    }

    const workflow = existingChild.workflow
    const children: Array<DurableWorkflowChildExecution> = []
    if (workflow.isParallel) {
      for (const [i, child] of workflow.children.entries()) {
        if (child.type === 'function') {
          const childExecutionId = `${executionId}[${i}]`
          children.push({
            type: 'function',
            id: child.fnId,
            executionId: childExecutionId,
          })
        } else {
          const childExecutionId = `${executionId}[${i}]`
          children.push({
            type: 'workflow',
            id: child.workflowId,
            executionId: childExecutionId,
          })
        }
      }
    } else {
      const firstChild = workflow.children[0]!
      if (firstChild.type === 'function') {
        const childExecutionId = `${executionId}[0]`
        children.push({
          type: 'function',
          id: firstChild.fnId,
          executionId: childExecutionId,
        })
      } else {
        const childExecutionId = `${executionId}[0]`
        children.push({
          type: 'workflow',
          id: firstChild.workflowId,
          executionId: childExecutionId,
        })
      }

      for (const child of workflow.children.slice(1)) {
        if (child.type === 'function') {
          children.push({
            type: 'function',
            id: child.fnId,
          })
        } else {
          children.push({
            type: child.type,
            id: child.workflowId,
          })
        }
      }
    }

    rootWorkflow = rootWorkflow ?? {
      id: workflow.id,
      executionId,
    }

    await this.withExistingTransaction(async (tx) => {
      await this.enqueueFunctionInternal({ type: 'workflow', workflowId }, executionId, {
        tx,
        rootWorkflow,
        parentWorkflow,
        children,
        isParallel: workflow.isParallel,
        status: 'running',
      })

      for (let i = 0; i < workflow.children.length; i++) {
        const child = workflow.children[i]!
        const childExecution = children[i]!
        if (childExecution.executionId) {
          const parentWorkflow = {
            id: workflow.id,
            executionId,
          }
          await (child.type === 'function'
            ? this.enqueueFunctionInternal(child, childExecution.executionId, {
                tx,
                rootWorkflow,
                parentWorkflow,
              })
            : this.enqueueWorkflowInternal(child.workflowId, childExecution.executionId, {
                tx,
                rootWorkflow,
                parentWorkflow,
              }))
        }
      }
    }, tx)
  }

  /**
   * Enqueue a workflow execution.
   *
   * @param workflowId - The id of the workflow to enqueue.
   * @param executionId - The id of the execution. Should be unique among all the durable function
   *   and workflow executions in the same durable executor.
   * @returns A handle to the workflow execution.
   */
  async enqueueWorkflow(workflowId: string, executionId: string): Promise<DurableWorkflowHandle> {
    this.throwIfShutdown()
    validateId(workflowId)
    validateId(executionId)

    await this.enqueueWorkflowInternal(workflowId, executionId)
    this.logger.debug(`Enqueued workflow ${workflowId} with execution id ${executionId}`)

    return {
      getExecution: async () => {
        const execution = await this.withTransaction(async (tx) => {
          const executions = await tx.getFunctionExecutions({ type: 'by_ids', ids: [executionId] })
          return executions.length === 0 ? undefined : executions[0]
        })
        if (!execution) {
          throw new Error(`Execution ${executionId} not found`)
        }
        return convertExecutionStorageObjectToWorkflowExecution(execution)
      },
      waitAndGetExecution: async ({
        signal,
        delayMs,
      }: {
        signal?: CancelSignal | AbortSignal
        delayMs?: number
      } = {}) => {
        const cancelSignal =
          signal instanceof AbortSignal
            ? createCancelSignal({ abortSignal: signal, logger: this.logger })[0]
            : signal
        let isFirstIteration = true
        while (true) {
          if (cancelSignal?.isCancelled()) {
            throw new Error('Execution cancelled')
          }

          if (isFirstIteration) {
            isFirstIteration = false
          } else {
            const finalDelayMs = delayMs && delayMs > 0 ? delayMs : 1000
            await createCancellablePromise(sleep(finalDelayMs), cancelSignal)

            if (cancelSignal?.isCancelled()) {
              throw new Error('Execution cancelled')
            }
          }

          const execution = await this.withTransaction(async (tx) => {
            const executions = await tx.getFunctionExecutions({
              type: 'by_ids',
              ids: [executionId],
            })
            return executions.length === 0 ? undefined : executions[0]
          })
          if (execution && FINISHED_FUNCTION_EXECUTION_STATUSES.includes(execution.status)) {
            return convertExecutionStorageObjectToWorkflowExecution(
              execution,
            ) as DurableWorkflowFinishedExecution
          } else {
            this.logger.debug(
              `Waiting for workflow ${executionId} to be finished. Status: ${execution?.status}`,
            )
          }
        }
      },
      cancel: async () => {
        const now = new Date()
        await this.withTransaction(async (tx) => {
          await tx.updateFunctionExecutions(
            { type: 'by_ids', ids: [executionId], statuses: ACTIVE_FUNCTION_EXECUTION_STATUSES },
            {
              error: {
                message: 'Workflow cancelled',
                tag: 'DurableExecutorCancelledError',
                isRetryable: false,
              },
              status: 'cancelled',
              finishedAt: now,
              updatedAt: now,
            },
          )
        })
        this.logger.debug(`Cancelled workflow ${executionId}`)
      },
    }
  }

  /**
   * Start the durable executor. Starts a background process. Use {@link DurableExecutor.shutdown}
   * to stop the durable executor.
   */
  async start(): Promise<void> {
    this.throwIfShutdown()
    this.startPromise = this.startBackgroundProcesses()
    await this.startPromise
  }

  private async startBackgroundProcesses(): Promise<void> {
    await Promise.all([
      this.finalizeExecutions(),
      this.expireRunningFunctionExecutions(),
      this.cancelNeedPromiseCancellationFunctionExecutions(),
      this.processReadyFunctionExecutions(),
    ])
  }

  /**
   * Shutdown the durable executor. Cancels all active executions and stops the background
   * process.
   *
   * On shutdown, these happen in this order:
   * - All new enqueue operations are stopped
   * - All background processes stop after the current iteration
   * - Executor waits for active executions to finish. Function execution context contains a
   *   shutdown signal that can be used to gracefully shutdown the function when executor is
   *   shutting down.
   */
  async shutdown(): Promise<void> {
    this.logger.info('Shutting down durable executor')
    if (!this.shutdownSignal.isCancelled()) {
      try {
        this.internalCancel()
      } catch (error) {
        this.logger.error('Error in cancelling executor', error)
      }
    }
    this.logger.info('Executor cancelled. Waiting for background processes to stop')

    if (this.startPromise) {
      await this.startPromise
    }
    this.logger.info('Background processes stopped. Waiting for active executions to finish')

    const runningExecutions = [...this.runningExecutionsMap.entries()]
    for (const [executionId, runningExecution] of runningExecutions) {
      try {
        if (runningExecution.type === 'function') {
          await runningExecution.promise
        }
        this.runningExecutionsMap.delete(executionId)
      } catch (error) {
        this.logger.error(`Error in waiting for running function execution ${executionId}`, error)
      }
    }
    this.logger.info('Active executions finished. Durable executor shut down')
  }

  private async finalizeExecutions(): Promise<void> {
    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info('Executor cancelled. Stopping processing finalized executions')
        return
      }

      try {
        await createCancellablePromise(this.finalizeExecutionsSingleBatch(), this.shutdownSignal)
      } catch (error) {
        if (error instanceof DurableExecutorCancelledError) {
          this.logger.info('Executor cancelled. Stopping finalizing executions')
          return
        }
        this.logger.error('Error in finalizing executions', error)
      }
    }
  }

  /**
   * Finalize executions that are finished.
   */
  private async finalizeExecutionsSingleBatch(): Promise<void> {
    let shouldSleep = false
    await this.withTransaction(async (tx) => {
      const now = new Date()
      const updatedExecutionIds = await tx.updateFunctionExecutions(
        {
          type: 'by_statuses',
          statuses: FINISHED_FUNCTION_EXECUTION_STATUSES,
          isFinalized: false,
        },
        { isFinalized: true, updatedAt: now },
        1,
      )

      this.logger.debug(`Finalizing ${updatedExecutionIds.length} executions`)
      if (updatedExecutionIds.length === 0) {
        shouldSleep = true
        return
      }

      const executions = await tx.getFunctionExecutions({
        type: 'by_ids',
        ids: updatedExecutionIds,
      })
      if (executions.length !== updatedExecutionIds.length) {
        throw new Error('Some executions not found')
      }
      for (const execution of executions) {
        await this.finalizeFunctionExecutionParent(tx, execution, now)
        await this.finalizeFunctionExecutionChildren(tx, execution, now)
      }
    })

    if (shouldSleep) {
      await sleepWithJitter(100)
    }
  }

  /**
   * Finalize function execution parent. If the execution status is completed, it will update the
   * status of the parent workflow execution to completed if all the children are completed,
   * otherwise starts the next child if the parent workflow is not parallel and there have been no
   * errors. If the execution status is not completed, it will update the status of the parent
   * workflow execution to the execution status if it didn't already have an error. In all other
   * cases, it will either just update the children state or do nothing.
   */
  private async finalizeFunctionExecutionParent(
    tx: DurableStorageTx,
    execution: DurableFunctionExecutionStorageObject,
    now: Date,
  ): Promise<void> {
    if (!execution.parentWorkflow) {
      return
    }

    const parentExecutions = await tx.getFunctionExecutions({
      type: 'by_ids',
      ids: [execution.parentWorkflow.executionId],
    })
    if (parentExecutions.length === 0) {
      this.logger.error(`Parent execution ${execution.parentWorkflow.executionId} not found`)
      return
    }

    const status = execution.status
    const parentExecution = parentExecutions[0]!
    const parentChildren = parentExecution.children ?? []
    const childIdx = parentChildren.findIndex(
      (child) => child.executionId === execution.executionId,
    )
    if (childIdx === -1) {
      this.logger.error(
        `Child execution ${execution.executionId} not found for parent execution ${execution.parentWorkflow.executionId}`,
      )
      return
    }

    const parentChild = parentChildren[childIdx]!
    if (status === 'completed') {
      const areAllChildrenCompleted =
        parentExecution.childrenCompletedCount >= parentChildren.length - 1
      if (parentExecution.status === 'running' && areAllChildrenCompleted) {
        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [parentExecution.executionId] },
          {
            childrenCompletedCount: parentExecution.childrenCompletedCount + 1,
            error: {
              message: '',
              tag: '',
              isRetryable: false,
            },
            status: 'completed',
            finishedAt: now,
            updatedAt: now,
          },
        )
      } else if (parentExecution.status === 'running' && !parentExecution.isParallel) {
        const nextParentChild = parentChildren[childIdx + 1]!
        nextParentChild.executionId = `${parentExecution.executionId}[${childIdx + 1}]`
        const existingChild = this.fnMap.get(nextParentChild.id)
        if (!existingChild) {
          this.logger.error(`Child execution ${parentChild.id} not found`)
          return
        }
        if (existingChild.type !== nextParentChild.type) {
          this.logger.error(
            `Child execution ${nextParentChild.id} is a ${nextParentChild.type} but ${existingChild.type} is expected`,
          )
          return
        }

        await (existingChild.type === 'function'
          ? this.enqueueFunctionInternal(
              { type: 'function', fnId: existingChild.fn.id },
              nextParentChild.executionId,
              {
                tx,
                rootWorkflow: execution.rootWorkflow,
                parentWorkflow: execution.parentWorkflow,
              },
            )
          : this.enqueueWorkflowInternal(existingChild.workflow.id, nextParentChild.executionId, {
              tx,
              rootWorkflow: execution.rootWorkflow,
              parentWorkflow: execution.parentWorkflow,
            }))

        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [parentExecution.executionId] },
          {
            children: parentChildren,
            childrenCompletedCount: parentExecution.childrenCompletedCount + 1,
            updatedAt: now,
          },
        )
      } else {
        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [parentExecution.executionId] },
          {
            childrenCompletedCount: parentExecution.childrenCompletedCount + 1,
            updatedAt: now,
          },
        )
      }
    } else if (parentExecution.status === 'running') {
      await tx.updateFunctionExecutions(
        { type: 'by_ids', ids: [parentExecution.executionId] },
        {
          error: execution.error
            ? {
                message: `Child ${execution.type} ${execution.fnId} failed: ${execution.error.message}`,
                tag: execution.error.tag,
                isRetryable: false,
              }
            : undefined,
          status,
          finishedAt: now,
          updatedAt: now,
        },
      )
    }
  }

  /**
   * Finalize function execution children. Cancels running children function executions and updates
   * the status of the children workflow executions to cancelled.
   */
  private async finalizeFunctionExecutionChildren(
    tx: DurableStorageTx,
    execution: DurableFunctionExecutionStorageObject,
    now: Date,
  ): Promise<void> {
    if (!execution.children || execution.children.length === 0) {
      return
    }

    const status = execution.status
    if (status !== 'completed') {
      const fnExecutionIds = [] as Array<string>
      const workflowExecutionIds = [] as Array<string>
      for (const child of execution.children) {
        if (child.executionId) {
          if (child.type === 'function') {
            fnExecutionIds.push(child.executionId)
          } else {
            workflowExecutionIds.push(child.executionId)
          }
        }
      }

      if (fnExecutionIds.length > 0) {
        await tx.updateFunctionExecutions(
          {
            type: 'by_ids',
            ids: fnExecutionIds,
            statuses: ACTIVE_FUNCTION_EXECUTION_STATUSES,
          },
          {
            error: execution.error
              ? {
                  message: `Parent workflow ${execution.fnId} failed: ${execution.error.message}`,
                  tag: 'DurableExecutorCancelledError',
                  isRetryable: false,
                }
              : undefined,
            needsPromiseCancellation: true,
            status: 'cancelled',
            finishedAt: now,
            updatedAt: now,
          },
        )
      }

      if (workflowExecutionIds.length > 0) {
        await tx.updateFunctionExecutions(
          {
            type: 'by_ids',
            ids: workflowExecutionIds,
            statuses: ACTIVE_FUNCTION_EXECUTION_STATUSES,
          },
          {
            error: execution.error
              ? {
                  message: `Parent workflow ${execution.fnId} failed: ${execution.error.message}`,
                  tag: 'DurableExecutorCancelledError',
                  isRetryable: false,
                }
              : undefined,
            status: 'cancelled',
            finishedAt: now,
            updatedAt: now,
          },
        )
      }
    }
  }

  private async expireRunningFunctionExecutions(): Promise<void> {
    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info('Executor cancelled. Stopping expiring running function executions')
        return
      }

      try {
        await createCancellablePromise(
          this.expireRunningFunctionExecutionsSingleBatch(),
          this.shutdownSignal,
        )
      } catch (error) {
        if (error instanceof DurableExecutorCancelledError) {
          this.logger.info('Executor cancelled. Stopping expiring running function executions')
          return
        }
        this.logger.error('Error in expiring running function executions', error)
      }
    }
  }

  /**
   * Expire executions that are running and have expired. Doesn't affect workflow executions as
   * they don't have an expiry. Retries the function and cancels existing running execution.
   *
   * This will only happen when the process running the execution previously crashed.
   *
   * It will sleep for a random amount of time if there are no executions to process.
   */
  private async expireRunningFunctionExecutionsSingleBatch(): Promise<void> {
    let shouldSleep = false
    await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await tx.updateFunctionExecutions(
        {
          type: 'by_statuses',
          statuses: ['running'],
          isFinalized: false,
          expiresAtLessThan: now,
        },
        {
          error: {
            message: 'Function expired',
            tag: 'DurableExecutorError',
            isRetryable: true,
          },
          status: 'ready',
          startAt: now,
          expiresAt: EXPIRES_AT_INFINITY,
          updatedAt: now,
        },
        3,
      )
      this.logger.debug(`Expiring ${executionIds.length} running function executions`)
      if (executionIds.length === 0) {
        shouldSleep = true
        return
      }
    })

    if (shouldSleep) {
      await sleepWithJitter(500)
    }
  }

  private async cancelNeedPromiseCancellationFunctionExecutions(): Promise<void> {
    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info(
          'Executor cancelled. Stopping cancelling promise cancellation function executions',
        )
        return
      }

      try {
        await createCancellablePromise(
          this.cancelNeedPromiseCancellationFunctionExecutionsSingleBatch(),
          this.shutdownSignal,
        )
      } catch (error) {
        if (error instanceof DurableExecutorCancelledError) {
          this.logger.info(
            'Executor cancelled. Stopping cancelling promise cancellation function executions',
          )
          return
        }
        this.logger.error('Error in cancelling promise cancellation function executions', error)
      }
    }
  }

  /**
   * Cancel function executions that need promise cancellation. Doesn't affect workflow executions
   * as they don't have a promise cancellation.
   */
  private async cancelNeedPromiseCancellationFunctionExecutionsSingleBatch(): Promise<void> {
    if (this.runningExecutionsMap.size === 0) {
      await sleepWithJitter(500)
      return
    }

    let shouldSleep = false
    await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await tx.updateFunctionExecutions(
        {
          type: 'by_ids',
          ids: [...this.runningExecutionsMap.keys()],
          needsPromiseCancellation: true,
        },
        {
          needsPromiseCancellation: false,
          updatedAt: now,
        },
        5,
      )
      this.logger.debug(
        `Cancelling ${executionIds.length} function executions that need promise cancellation`,
      )
      if (executionIds.length === 0) {
        shouldSleep = true
        return
      }

      for (const executionId of executionIds) {
        const runningExecution = this.runningExecutionsMap.get(executionId)
        if (runningExecution && runningExecution.type === 'function') {
          try {
            runningExecution.cancel()
          } catch (error) {
            this.logger.error(`Error in cancelling function ${executionId}`, error)
          }
          this.runningExecutionsMap.delete(executionId)
        }
      }
    })

    if (shouldSleep) {
      await sleepWithJitter(500)
    }
  }

  private async processReadyFunctionExecutions(): Promise<void> {
    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info('Executor cancelled. Stopping processing ready function executions')
        return
      }

      try {
        await createCancellablePromise(
          this.processReadyFunctionExecutionsSingleBatch(),
          this.shutdownSignal,
        )
      } catch (error) {
        if (error instanceof DurableExecutorCancelledError) {
          this.logger.info('Executor cancelled. Stopping processing ready function executions')
          return
        }
        this.logger.error('Error in processing ready function executions', error)
      }
    }
  }

  /**
   * Process function executions that are ready to run based on status being ready and startAt
   * being in the past. Doesn't affect workflow executions as they are always in running state.
   */
  private async processReadyFunctionExecutionsSingleBatch(): Promise<void> {
    let shouldSleep = false
    await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await tx.updateFunctionExecutions(
        {
          type: 'by_start_at',
          startAtLessThan: now,
          statuses: ['ready'],
        },
        { status: 'running', startedAt: now, updatedAt: now },
        1,
      )
      this.logger.debug(`Processing ${executionIds.length} ready function executions`)
      if (executionIds.length === 0) {
        shouldSleep = true
        return
      }

      const executions = await tx.getFunctionExecutions({
        type: 'by_ids',
        ids: executionIds,
      })
      if (executions.length !== executionIds.length) {
        throw new Error('Some executions not found')
      }

      const toRunExecutions = [] as Array<DurableFunctionExecutionStorageObject>
      for (const execution of executions) {
        const existingChild = this.fnMap.get(execution.fnId)
        if (!existingChild) {
          // This should never happen.
          this.logger.error(`Function ${execution.fnId} not found`)
          continue
        }
        if (existingChild.type !== 'function') {
          // This should never happen.
          this.logger.error(
            `Function ${execution.fnId} is a ${existingChild.type} but a function is expected`,
          )
          continue
        }

        const timeoutMs = getDurableFunctionTimeoutMs(existingChild.fn, execution.retryAttempts)
        if (timeoutMs == null) {
          const errorMessage = `Invalid timeout value for function ${execution.fnId} on attempt ${execution.retryAttempts}`
          this.logger.error(errorMessage)
          await tx.updateFunctionExecutions(
            { type: 'by_ids', ids: [execution.executionId] },
            {
              error: {
                message: errorMessage,
                tag: 'DurableExecutorTimedOutError',
                isRetryable: false,
              },
              status: 'timed_out',
              finishedAt: now,
            },
          )
          continue
        }

        const expireMs = timeoutMs + 5 * 60 * 1000 // timeout + 5 minutes
        const expiresAt = new Date(now.getTime() + expireMs)
        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [execution.executionId] },
          { expiresAt },
        )

        execution.expiresAt = expiresAt
        toRunExecutions.push(execution)
      }

      await Promise.all(
        toRunExecutions.map((execution) => this.runFunctionWithCancelSignal(tx, execution)),
      )
    })

    if (shouldSleep) {
      await sleepWithJitter(100)
    }
  }

  /**
   * Run a function. It is expected to be in running state.
   *
   * It will add the execution to the running executions map and make sure it is removed from the
   * running executions map if the function completes. If the process crashes, the execution will
   * be retried later on expiration.
   *
   * @param execution - The execution to run.
   */
  private async runFunctionWithCancelSignal(
    tx: DurableStorageTx,
    execution: DurableFunctionExecutionStorageObject,
  ): Promise<void> {
    if (this.runningExecutionsMap.has(execution.executionId)) {
      return
    }

    const existingChild = this.fnMap.get(execution.fnId)
    if (!existingChild) {
      // This should never happen.
      this.logger.error(`Function ${execution.fnId} not found`)

      const now = new Date()
      await tx.updateFunctionExecutions(
        {
          type: 'by_ids',
          ids: [execution.executionId],
          statuses: ACTIVE_FUNCTION_EXECUTION_STATUSES,
        },
        {
          error: {
            message: `${execution.type === 'function' ? 'Function' : 'Workflow'} ${execution.fnId} not found`,
            tag: 'DurableExecutorError',
            isRetryable: false,
          },
          status: 'cancelled',
          finishedAt: now,
          updatedAt: now,
        },
      )

      return
    }

    if (existingChild.type === 'function') {
      const [cancelSignal, cancel] = createCancelSignal({ logger: this.logger })
      const promise = this.runFunction(existingChild.fn, execution, cancelSignal)
        .catch((error) => {
          // This should not happen.
          this.logger.error(`Error in running function ${execution.executionId}`, error)
        })
        .finally(() => {
          // If runFunction fails (should not happen), cancel the execution and remove it from the
          // running executions map. It is retried later on expiration.
          try {
            cancel()
          } catch (error) {
            this.logger.error(`Error in cancelling function ${execution.executionId}`, error)
          }
          this.runningExecutionsMap.delete(execution.executionId)
        })
      this.runningExecutionsMap.set(execution.executionId, {
        type: 'function',
        promise,
        cancel,
      })
    } else {
      this.runningExecutionsMap.set(execution.executionId, {
        type: 'workflow',
      })
    }
  }

  /**
   * Run a function. It is expected to be in running state and present in the running executions
   * map.
   *
   * It will update the execution status to completed, failed, cancelled, or ready depending on
   * the result of the function. If the function completes successfully, it will update the
   * execution status to completed. If the function fails, it will update the execution status to
   * failed or cancelled depending on the error. If the error is retryable and the retry attempts
   * are less than the maximum retry attempts, it will update the execution status to ready. All
   * the errors are saved in storage even if the function is retried. They only get cleared if the
   * execution is completed later.
   *
   * If runFunction runs to completion, the running executions map is updated to remove the
   * execution. All this is atomic so the execution is guaranteed to be removed from the running
   * executions map if runFunction completes. If it fails but process does not crash, the running
   * executions map is updated in runFunctionWithCancelSignal. The function remains in the running
   * state and retried later on expiration.
   *
   * @param fn - The function to run.
   * @param execution - The execution to run.
   * @param cancelSignal - The cancel signal.
   */
  private async runFunction(
    fn: DurableFunction,
    execution: DurableFunctionExecutionStorageObject,
    cancelSignal: CancelSignal,
  ): Promise<void> {
    const context = {
      attempt: execution.retryAttempts,
      prevError: execution.error?.tag
        ? createDurableExecutorError(
            execution.error.message,
            execution.error.tag,
            execution.error.isRetryable,
          )
        : undefined,
      rootWorkflow: execution.rootWorkflow,
      parentWorkflow: execution.parentWorkflow,
      shutdownSignal: this.shutdownSignal,
    }

    // Make sure the execution is in running state to not do any wasteful work.
    // This can happend in a rare case where an execution is cancelled in between the execution
    // getting picked up and the function being added to the running executions map.
    await this.withTransaction(async (tx) => {
      const existingExecutions = await tx.getFunctionExecutions({
        type: 'by_ids',
        ids: [execution.executionId],
      })
      if (existingExecutions.length === 0 || existingExecutions[0]?.status !== 'running') {
        this.logger.error(`Execution ${execution.executionId} is not running`)
        return
      }
    })

    try {
      await this.executeFunctionWithTimeoutAndCancellation(fn, context, cancelSignal)
      const now = new Date()
      await this.withTransaction(async (tx) => {
        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [execution.executionId], statuses: ['running'] },
          {
            error: {
              message: '',
              tag: '',
              isRetryable: false,
            },
            status: 'completed',
            finishedAt: now,
            updatedAt: now,
          },
        )

        this.runningExecutionsMap.delete(execution.executionId)
      })
    } catch (error) {
      const durableFunctionError =
        error instanceof DurableExecutorError
          ? error
          : new DurableExecutorError(getErrorMessage(error), true)

      const now = new Date()
      let update: DurableFunctionExecutionStorageObjectUpdate

      // Check if the error is retryable and the retry attempts are less than the maximum retry
      // attempts. If so, update the execution status to ready.
      if (
        durableFunctionError.isRetryable &&
        execution.retryAttempts < (fn.maxRetryAttempts ?? 0)
      ) {
        const delayMs = getDurableFunctionDelayMsAfterAttempt(fn, execution.retryAttempts)
        const startAt = delayMs && delayMs > 0 ? new Date(now.getTime() + delayMs) : now
        update = {
          error: {
            message: durableFunctionError.message,
            tag: durableFunctionError.tag,
            isRetryable: durableFunctionError.isRetryable,
          },
          status: 'ready',
          retryAttempts: execution.retryAttempts + 1,
          startAt,
          expiresAt: EXPIRES_AT_INFINITY,
          updatedAt: now,
        }
      } else {
        update =
          error instanceof DurableExecutorTimedOutError
            ? {
                error: {
                  message: 'Function timed out',
                  tag: 'DurableExecutorTimedOutError',
                  isRetryable: durableFunctionError.isRetryable,
                },
                status: 'timed_out',
                finishedAt: now,
                updatedAt: now,
              }
            : error instanceof DurableExecutorCancelledError
              ? {
                  error: {
                    message: 'Function cancelled',
                    tag: 'DurableExecutorCancelledError',
                    isRetryable: durableFunctionError.isRetryable,
                  },
                  status: 'cancelled',
                  finishedAt: now,
                  updatedAt: now,
                }
              : {
                  error: {
                    message: durableFunctionError.message,
                    tag: 'DurableExecutorError',
                    isRetryable: durableFunctionError.isRetryable,
                  },
                  status: 'failed',
                  finishedAt: now,
                  updatedAt: now,
                }
      }

      await this.withTransaction(async (tx) => {
        await tx.updateFunctionExecutions(
          { type: 'by_ids', ids: [execution.executionId], statuses: ['running'] },
          update,
        )

        this.runningExecutionsMap.delete(execution.executionId)
      })
    }
  }

  private async executeFunctionWithTimeoutAndCancellation(
    fn: DurableFunction,
    context: DurableFunctionContext,
    cancelSignal: CancelSignal,
  ): Promise<void> {
    const resultPromise = createCancellablePromise(fn.execute(context), cancelSignal)
      .then(() => {
        return undefined
      })
      .catch((error) => {
        return error instanceof DurableExecutorError
          ? error
          : new DurableExecutorError(getErrorMessage(error), true)
      })
    const timeoutMs = getDurableFunctionTimeoutMs(fn, context.attempt)
    if (timeoutMs == null) {
      throw new Error(`Invalid timeout value for function ${fn.id} on attempt ${context.attempt}`)
    }

    const timeoutPromise = sleep(timeoutMs).then(() => {
      return new DurableExecutorTimedOutError()
    })
    const error = await Promise.race([resultPromise, timeoutPromise])
    if (error) {
      throw error
    }
  }
}
