import type { StandardSchemaV1 } from '@standard-schema/spec'

import { getErrorMessage } from '@gpahal/std/errors'
import { sleep } from '@gpahal/std/promises'

import { createCancellablePromise, createCancelSignal, type CancelSignal } from './cancel'
import {
  convertDurableExecutionErrorStorageObjectToError,
  convertDurableExecutionErrorToStorageObject,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionTimedOutError,
} from './errors'
import { createConsoleLogger, createLoggerDebugDisabled, type Logger } from './logger'
import { createSuperjsonSerializer, wrapSerializer, type Serializer } from './serializer'
import {
  convertTaskExecutionStorageObjectToTaskExecution,
  createDurableTaskExecutionStorageObject,
  EXPIRES_AT_INFINITY,
  updateTaskExecutionsWithLimit,
  type DurableStorage,
  type DurableStorageTx,
  type DurableTaskExecutionStorageObject,
  type DurableTaskExecutionStorageObjectUpdate,
} from './storage'
import {
  ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
  DurableTaskInternal,
  FINISHED_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
  generateTaskExecutionId,
  type DurableChildTaskExecution,
  type DurableChildTaskExecutionOutput,
  type DurableParentTaskOptions,
  type DurableTask,
  type DurableTaskEnqueueOptions,
  type DurableTaskFinishedExecution,
  type DurableTaskHandle,
  type DurableTaskOptions,
  type DurableTaskRunContext,
} from './task'
import { generateId, sleepWithJitter, summarizeStandardSchemaIssues } from './utils'

export {
  type CancelSignal,
  createCancelSignal,
  createTimeoutCancelSignal,
  createCancellablePromise,
} from './cancel'
export {
  DurableExecutionError,
  DurableExecutionTimedOutError,
  DurableExecutionCancelledError,
  type DurableExecutionErrorStorageObject,
} from './errors'
export { type Logger, createConsoleLogger } from './logger'
export type {
  DurableTask,
  DurableTaskCommonOptions,
  DurableTaskRetryOptions,
  DurableTaskOptions,
  DurableParentTaskOptions,
  DurableFinalizeTaskOptions,
  DurableTaskOnChildrenCompleteInput,
  DurableTaskRunContext,
  DurableTaskExecution,
  DurableTaskFinishedExecution,
  DurableTaskReadyExecution,
  DurableTaskRunningExecution,
  DurableTaskFailedExecution,
  DurableTaskTimedOutExecution,
  DurableTaskCancelledExecution,
  DurableTaskWaitingForChildrenTasksExecution,
  DurableTaskChildrenTasksFailedExecution,
  DurableTaskWaitingForFinalizeTaskExecution,
  DurableTaskFinalizeTaskFailedExecution,
  DurableTaskCompletedExecution,
  DurableChildTask,
  DurableChildTaskExecution,
  DurableChildTaskExecutionOutput,
  DurableChildTaskExecutionError,
  DurableChildTaskExecutionErrorStorageObject,
  DurableTaskExecutionStatusStorageObject,
  DurableTaskEnqueueOptions,
  DurableTaskHandle,
} from './task'
export { type Serializer, createSuperjsonSerializer, wrapSerializer } from './serializer'
export {
  createInMemoryStorage,
  createTransactionMutex,
  type TransactionMutex,
  type DurableStorage,
  type DurableStorageTx,
  type DurableTaskExecutionStorageObject,
  type DurableTaskExecutionStorageWhere,
  type DurableTaskExecutionStorageObjectUpdate,
} from './storage'

/**
 * A durable executor. It is used to execute durable tasks.
 *
 * Multiple durable executors can share the same storage. In such a case, all the tasks should be
 * present for all the durable executors. The work is distributed among the durable executors.
 * See the [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * sections for more details on creating and enqueuing tasks.
 *
 * @example
 * ```ts
 * const executor = new DurableExecutor(storage)
 *
 * // Create tasks
 * const extractFileTitle = executor
 *   .inputSchema(v.object({ filePath: v.string() }))
 *   .task({
 *     id: 'extractFileTitle',
 *     timeoutMs: 30_000, // 30 seconds
 *     run: async (ctx, input) => {
 *       // ... extract the file title
 *       return {
 *         title: 'File Title',
 *       }
 *     },
 *   })
 *
 * const summarizeFile = executor
 *   .validateInput(async (input: { filePath: string }) => {
 *     if (!isValidFilePath(input.filePath)) {
 *       throw new Error('Invalid file path')
 *     }
 *     return {
 *       filePath: input.filePath,
 *     }
 *   })
 *   .task({
 *     id: 'summarizeFile',
 *     timeoutMs: 30_000, // 30 seconds
 *     run: async (ctx, input) => {
 *       // ... summarize the file
 *       return {
 *         summary: 'File summary',
 *       }
 *     },
 *   })
 *
 * const uploadFile = executor
 *   .inputSchema(v.object({ filePath: v.string(), uploadUrl: v.string() }))
 *   .parentTask({
 *     id: 'uploadFile',
 *     timeoutMs: 60_000, // 1 minute
 *     runParent: async (ctx, input) => {
 *       // ... upload file to the given uploadUrl
 *       // Extract the file title and summarize the file in parallel
 *       return {
 *         output: {
 *           filePath: input.filePath,
 *           uploadUrl: input.uploadUrl,
 *           fileSize: 100,
 *         },
 *         childrenTasks: [
 *           {
 *             task: extractFileTitle,
 *             input: { filePath: input.filePath },
 *           },
 *           {
 *             task: summarizeFile,
 *             input: { filePath: input.filePath },
 *           },
 *         ],
 *       }
 *     },
 *     finalizeTask: {
 *       id: 'onUploadFileAndChildrenComplete',
 *       timeoutMs: 60_000, // 1 minute
 *       run: async (ctx, { input, output, childrenTasksOutputs }) => {
 *         // ... combine the output of the run function and children tasks
 *         return {
 *           filePath: input.filePath,
 *           uploadUrl: input.uploadUrl,
 *           fileSize: 100,
 *           title: 'File Title',
 *           summary: 'File summary',
 *         }
 *       }
 *     },
 *   })
 *
 * async function app() {
 *   // Enqueue task and manage its execution lifecycle
 *   const uploadFileHandle = await executor.enqueueTask(uploadFile, {filePath: 'file.txt'})
 *   const uploadFileExecution = await uploadFileHandle.getExecution()
 *   const uploadFileFinishedExecution = await uploadFileHandle.waitAndGetExecution()
 *   await uploadFileHandle.cancel()
 *
 *   console.log(uploadFileExecution)
 * }
 *
 * // Start the durable executor and run the app
 * await Promise.all([
 *   executor.start(), // Start the durable executor in the background
 *   app(), // Run the app
 * ])
 *
 * // Shutdown the durable executor when the app is done
 * await executor.shutdown()
 * ```
 *
 * @category Executor
 */
export class DurableExecutor {
  private readonly storage: DurableStorage
  private readonly serializer: Serializer
  private readonly logger: Logger
  private readonly expireMs: number
  private readonly backgroundProcessIntraBatchSleepMs: number
  private readonly taskInternalsMap: Map<string, DurableTaskInternal>
  private readonly runningTaskExecutionsMap: Map<
    string,
    {
      promise: Promise<void>
      cancel: () => void
    }
  >
  private readonly shutdownSignal: CancelSignal
  private readonly internalCancel: () => void
  private startPromise: Promise<void> | undefined

  /**
   * Create a durable executor.
   *
   * @param storage - The storage to use for the durable executor.
   * @param options - The options for the durable executor.
   * @param options.serializer - The serializer to use for the durable executor. If not provided, a
   *   default serializer using superjson will be used.
   * @param options.logger - The logger to use for the durable executor. If not provided, a console
   *   logger will be used.
   * @param options.enableDebug - Whether to enable debug logging. If `true`, debug logging will
   *   be enabled.
   */
  constructor(
    storage: DurableStorage,
    {
      serializer,
      logger,
      enableDebug = false,
      expireMs,
      backgroundProcessIntraBatchSleepMs,
    }: {
      serializer?: Serializer
      logger?: Logger
      enableDebug?: boolean
      expireMs?: number
      backgroundProcessIntraBatchSleepMs?: number
    } = {},
  ) {
    this.storage = storage
    this.serializer = wrapSerializer(serializer ?? createSuperjsonSerializer())
    this.logger = logger ?? createConsoleLogger('DurableExecutor')
    if (!enableDebug) {
      this.logger = createLoggerDebugDisabled(this.logger)
    }
    this.expireMs = expireMs && expireMs > 0 ? expireMs : 60_000 // 1 minute
    this.backgroundProcessIntraBatchSleepMs =
      backgroundProcessIntraBatchSleepMs && backgroundProcessIntraBatchSleepMs > 0
        ? backgroundProcessIntraBatchSleepMs
        : 500 // 500ms

    this.taskInternalsMap = new Map()
    this.runningTaskExecutionsMap = new Map()
    const [cancelSignal, cancel] = createCancelSignal({ logger: this.logger })
    this.shutdownSignal = cancelSignal
    this.internalCancel = cancel
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
        if (error instanceof DurableExecutionError && !error.isRetryable) {
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

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw new DurableExecutionError('Executor shutdown', false)
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
      this.closeFinishedTaskExecutions(),
      this.retryExpiredRunningTaskExecutions(),
      this.cancelNeedPromiseCancellationTaskExecutions(),
      this.processReadyTaskExecutions(),
    ])
  }

  /**
   * Shutdown the durable executor. Cancels all active executions and stops the background
   * process.
   *
   * On shutdown, these happen in this order:
   * - Stop enqueuing new tasks
   * - Stop background processes after the current iteration
   * - Wait for active task executions to finish. Task execution context contains a shutdown signal
   *   that can be used to gracefully shutdown the task when executor is shutting down.
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
    this.logger.info('Background processes stopped. Waiting for active task executions to finish')

    const runningExecutions = [...this.runningTaskExecutionsMap.entries()]
    for (const [executionId, runningExecution] of runningExecutions) {
      try {
        await runningExecution.promise
        this.runningTaskExecutionsMap.delete(executionId)
      } catch (error) {
        this.logger.error(`Error in waiting for running task execution ${executionId}`, error)
      }
    }
    this.logger.info('Active task executions finished. Durable executor shut down')
  }

  /**
   * Add a task to the durable executor. See the
   * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) section
   * for more details on creating tasks.
   *
   * @param taskOptions - The task options. See {@link DurableTaskOptions} for more details on the
   *   task options.
   * @returns The durable task.
   */
  task<TInput = unknown, TOutput = unknown>(
    taskOptions: DurableTaskOptions<TInput, TOutput>,
  ): DurableTask<TInput, TOutput> {
    this.throwIfShutdown()
    const taskInternal = DurableTaskInternal.fromDurableTaskOptions(
      this.taskInternalsMap,
      taskOptions,
    )
    this.logger.debug(`Added task ${taskOptions.id}`)
    return {
      id: taskInternal.id,
    } as DurableTask<TInput, TOutput>
  }

  /**
   * Add a parent task to the durable executor. See the
   * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) section
   * for more details on creating parent tasks.
   *
   * @param parentTaskOptions - The parent task options. See {@link DurableParentTaskOptions} for
   *   more details on the parent task options.
   * @returns The durable parent task.
   */
  parentTask<
    TInput = unknown,
    TRunOutput = unknown,
    TOutput = {
      output: TRunOutput
      childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
    },
    TFinalizeTaskRunOutput = unknown,
  >(
    parentTaskOptions: DurableParentTaskOptions<
      TInput,
      TRunOutput,
      TOutput,
      TFinalizeTaskRunOutput
    >,
  ): DurableTask<TInput, TOutput> {
    this.throwIfShutdown()
    const taskInternal = DurableTaskInternal.fromDurableParentTaskOptions(
      this.taskInternalsMap,
      parentTaskOptions,
    )
    this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
    return {
      id: taskInternal.id,
    } as DurableTask<TInput, TOutput>
  }

  validateInput<TRunInput, TInput>(
    validateInputFn: (input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput = unknown>(
      taskOptions: DurableTaskOptions<TRunInput, TOutput>,
    ) => DurableTask<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
      },
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: DurableParentTaskOptions<
        TRunInput,
        TRunOutput,
        TOutput,
        TFinalizeTaskRunOutput
      >,
    ) => DurableTask<TInput, TOutput>
  } {
    this.throwIfShutdown()

    const wrappedValidateInputFn = async (id: string, input: TInput) => {
      try {
        const runInput = await validateInputFn(input)
        return runInput
      } catch (error) {
        throw new DurableExecutionError(
          `Invalid input to task ${id}: ${getErrorMessage(error)}`,
          false,
        )
      }
    }

    return this.withValidateInputInternal(wrappedValidateInputFn)
  }

  inputSchema<TInputSchema extends StandardSchemaV1>(
    inputSchema: TInputSchema,
  ): {
    task: <TOutput = unknown>(
      taskOptions: DurableTaskOptions<StandardSchemaV1.InferOutput<TInputSchema>, TOutput>,
    ) => DurableTask<StandardSchemaV1.InferInput<TInputSchema>, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
      },
    >(
      parentTaskOptions: DurableParentTaskOptions<
        StandardSchemaV1.InferOutput<TInputSchema>,
        TRunOutput,
        TOutput
      >,
    ) => DurableTask<StandardSchemaV1.InferInput<TInputSchema>, TOutput>
  } {
    this.throwIfShutdown()

    const validateInputFn = async (
      id: string,
      input: StandardSchemaV1.InferInput<TInputSchema>,
    ): Promise<StandardSchemaV1.InferOutput<TInputSchema>> => {
      try {
        const validateResult = await inputSchema['~standard'].validate(input)
        if (validateResult.issues != null) {
          throw new DurableExecutionError(
            `Invalid input to task ${id}: ${summarizeStandardSchemaIssues(validateResult.issues)}`,
            false,
          )
        }
        return validateResult.value
      } catch (error) {
        if (error instanceof DurableExecutionError) {
          throw error
        }
        throw new DurableExecutionError(
          `Invalid input to task ${id}: ${getErrorMessage(error)}`,
          false,
        )
      }
    }

    return this.withValidateInputInternal(validateInputFn)
  }

  private withValidateInputInternal<TRunInput, TInput>(
    validateInputFn: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput>(
      taskOptions: DurableTaskOptions<TRunInput, TOutput>,
    ) => DurableTask<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
      },
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: DurableParentTaskOptions<
        TRunInput,
        TRunOutput,
        TOutput,
        TFinalizeTaskRunOutput
      >,
    ) => DurableTask<TInput, TOutput>
  } {
    this.throwIfShutdown()
    return {
      task: <TOutput>(
        taskOptions: DurableTaskOptions<TRunInput, TOutput>,
      ): DurableTask<TInput, TOutput> => {
        this.throwIfShutdown()
        const taskInternal = DurableTaskInternal.fromDurableTaskOptions(
          this.taskInternalsMap,
          taskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added task ${taskOptions.id}`)
        return {
          id: taskInternal.id,
        } as DurableTask<TInput, TOutput>
      },
      parentTask: <
        TRunOutput = unknown,
        TOutput = {
          output: TRunOutput
          childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
        },
        TFinalizeTaskRunOutput = unknown,
      >(
        parentTaskOptions: DurableParentTaskOptions<
          TRunInput,
          TRunOutput,
          TOutput,
          TFinalizeTaskRunOutput
        >,
      ): DurableTask<TInput, TOutput> => {
        this.throwIfShutdown()
        const taskInternal = DurableTaskInternal.fromDurableParentTaskOptions(
          this.taskInternalsMap,
          parentTaskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
        return {
          id: taskInternal.id,
        } as DurableTask<TInput, TOutput>
      },
    }
  }

  /**
   * Create a new task that runs a sequence of tasks sequentially.
   *
   * The tasks list must be a list of tasks that are compatible with each other. The input of any
   * task must be the same as the output of the previous task. The output of the last task will be
   * the output of the sequential task.
   *
   * The tasks list cannot be empty.
   *
   * @param tasks - The tasks to run sequentially.
   * @returns The sequential task.
   */
  sequentialTasks<T extends ReadonlyArray<DurableTask<unknown, unknown>>>(
    ...tasks: SequentialDurableTasks<T>
  ): DurableTask<ExtractDurableTaskInput<T[0]>, ExtractDurableTaskOutput<LastElement<T>>> {
    if (tasks.length === 0) {
      throw new DurableExecutionError('No tasks provided', false)
    }
    if (tasks.length === 1) {
      return tasks[0]
    }

    const firstTask = tasks[0]
    const secondTask = this.sequentialTasks(
      ...(tasks.slice(1) as SequentialDurableTasks<ReadonlyArray<DurableTask<unknown, unknown>>>),
    )
    const taskId = `st_${generateId(16)}`
    return this.parentTask({
      id: taskId,
      timeoutMs: 1000,
      runParent: (_, input) => {
        return {
          output: undefined,
          childrenTasks: [{ task: firstTask, input }],
        }
      },
      finalizeTask: {
        id: `${taskId}_finalize_1`,
        timeoutMs: 1000,
        runParent: (_, { childrenTasksOutputs }) => {
          const firstTaskOutput = childrenTasksOutputs[0]!.output
          return {
            output: undefined,
            childrenTasks: [{ task: secondTask, input: firstTaskOutput }],
          }
        },
        finalizeTask: {
          id: `${taskId}_finalize_2`,
          timeoutMs: 1000,
          run: (_, { childrenTasksOutputs }) => {
            const secondTaskOutput = childrenTasksOutputs[0]!.output as ExtractDurableTaskOutput<
              LastElement<T>
            >
            return secondTaskOutput
          },
        },
      },
    })
  }

  /**
   * Enqueue a task for execution.
   *
   * @param task - The task to enqueue.
   * @param input - The input to the task.
   * @returns A handle to the task execution.
   */
  async enqueueTask<TTask extends DurableTask<unknown, unknown>>(
    task: TTask,
    input: ExtractDurableTaskInput<TTask>,
    options: DurableTaskEnqueueOptions = {},
  ): Promise<DurableTaskHandle<ExtractDurableTaskOutput<TTask>>> {
    this.throwIfShutdown()

    const taskInternal = this.taskInternalsMap.get(task.id)
    if (!taskInternal) {
      throw new DurableExecutionError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it.`,
        false,
      )
    }

    const runInput = await taskInternal.validateInput(input)
    const executionId = generateTaskExecutionId()
    const now = new Date()
    const retryOptions = taskInternal.getRetryOptions(options)
    const sleepMsBeforeRun = taskInternal.getSleepMsBeforeRun(options)
    const timeoutMs = taskInternal.getTimeoutMs(options)
    await this.withTransaction(async (tx) => {
      await tx.insertTaskExecutions([
        createDurableTaskExecutionStorageObject({
          now,
          taskId: taskInternal.id,
          executionId,
          retryOptions,
          sleepMsBeforeRun,
          timeoutMs,
          runInput: this.serializer.serialize(runInput),
        }),
      ])
    })

    this.logger.debug(`Enqueued task ${task.id} with execution id ${executionId}`)
    return this.getTaskHandleInternal(taskInternal.id, executionId)
  }

  /**
   * Get a handle to a task execution.
   *
   * @param task - The task to get the handle for.
   * @param executionId - The id of the execution to get the handle for.
   * @returns The handle to the task execution.
   */
  getTaskHandle<TInput = unknown, TOutput = unknown>(
    task: DurableTask<TInput, TOutput>,
    executionId: string,
  ): DurableTaskHandle<TOutput> {
    return this.getTaskHandleInternal(task.id, executionId)
  }

  private getTaskHandleInternal<TOutput>(
    taskId: string,
    executionId: string,
  ): DurableTaskHandle<TOutput> {
    return {
      getTaskId: () => taskId,
      getTaskExecutionId: () => executionId,
      getTaskExecution: async () => {
        const execution = await this.withTransaction(async (tx) => {
          const executions = await tx.getTaskExecutions({
            type: 'by_execution_ids',
            executionIds: [executionId],
          })
          return executions.length === 0 ? undefined : executions[0]
        })
        if (!execution) {
          throw new DurableExecutionError(`Execution ${executionId} not found`, false)
        }
        return convertTaskExecutionStorageObjectToTaskExecution(execution, this.serializer)
      },
      waitAndGetTaskFinishedExecution: async ({
        signal,
        pollingIntervalMs,
      }: {
        signal?: CancelSignal | AbortSignal
        pollingIntervalMs?: number
      } = {}) => {
        const cancelSignal =
          signal instanceof AbortSignal
            ? createCancelSignal({ abortSignal: signal, logger: this.logger })[0]
            : signal

        const resolvedPollingIntervalMs =
          pollingIntervalMs && pollingIntervalMs > 0 ? pollingIntervalMs : 1000
        let isFirstIteration = true
        while (true) {
          if (cancelSignal?.isCancelled()) {
            throw new DurableExecutionCancelledError()
          }

          if (isFirstIteration) {
            isFirstIteration = false
          } else {
            await createCancellablePromise(sleep(resolvedPollingIntervalMs), cancelSignal)

            if (cancelSignal?.isCancelled()) {
              throw new DurableExecutionCancelledError()
            }
          }

          const execution = await this.withTransaction(async (tx) => {
            const executions = await tx.getTaskExecutions({
              type: 'by_execution_ids',
              executionIds: [executionId],
            })
            return executions.length === 0 ? undefined : executions[0]
          })
          if (!execution) {
            throw new DurableExecutionError(`Execution ${executionId} not found`, false)
          }

          if (FINISHED_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS.includes(execution.status)) {
            return convertTaskExecutionStorageObjectToTaskExecution(
              execution,
              this.serializer,
            ) as DurableTaskFinishedExecution<TOutput>
          } else {
            this.logger.debug(
              `Waiting for task ${executionId} to be finished. Status: ${execution.status}`,
            )
          }
        }
      },
      cancel: async () => {
        const now = new Date()
        await this.withTransaction(async (tx) => {
          await tx.updateTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [executionId],
              statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
            },
            {
              error: convertDurableExecutionErrorToStorageObject(
                new DurableExecutionCancelledError(),
              ),
              status: 'cancelled',
              needsPromiseCancellation: true,
              finishedAt: now,
              updatedAt: now,
            },
          )
        })
        this.logger.debug(`Cancelled function ${executionId}`)
      },
    }
  }

  private async runBackgroundProcess(
    processName: string,
    singleBatchProcessFn: () => Promise<boolean>,
  ): Promise<void> {
    let consecutiveErrors = 0
    const maxConsecutiveErrors = 10
    let backoffMs = 1000

    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info(`Executor cancelled. Stopping ${processName}`)
        return
      }

      try {
        const hasNonEmptyResult = await createCancellablePromise(
          singleBatchProcessFn(),
          this.shutdownSignal,
        )
        if (!hasNonEmptyResult) {
          await sleepWithJitter(this.backgroundProcessIntraBatchSleepMs)
        }

        consecutiveErrors = 0
        backoffMs = 1000
      } catch (error) {
        if (error instanceof DurableExecutionCancelledError) {
          this.logger.info(`Executor cancelled. Stopping ${processName}`)
          return
        }

        consecutiveErrors++
        this.logger.error(`Error in ${processName}: consecutive_errors=${consecutiveErrors}`, error)

        const isRetryableError = error instanceof DurableExecutionError ? error.isRetryable : true
        const waitTime = isRetryableError ? Math.min(backoffMs, 5000) : backoffMs
        if (consecutiveErrors >= maxConsecutiveErrors) {
          this.logger.error(
            `Too many consecutive errors (${consecutiveErrors}) in ${processName}. Backing off for ${backoffMs}ms before retrying.`,
          )
          await sleepWithJitter(waitTime)
          backoffMs = Math.min(backoffMs * 2, 30_000)
          consecutiveErrors = 0
        } else {
          await sleepWithJitter(waitTime)
        }
      }
    }
  }

  private async closeFinishedTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('closing finished task executions', () =>
      this.closeFinishedTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Close finished task executions.
   */
  private async closeFinishedTaskExecutionsSingleBatch(): Promise<boolean> {
    return await this.withTransaction(async (tx) => {
      const now = new Date()
      const updatedExecutionIds = await updateTaskExecutionsWithLimit(
        tx,
        {
          type: 'by_statuses',
          statuses: FINISHED_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
          isClosed: false,
        },
        { isClosed: true, updatedAt: now },
        1,
      )

      this.logger.debug(`Closing ${updatedExecutionIds.length} finished task executions`)
      if (updatedExecutionIds.length === 0) {
        return false
      }

      const executions = await tx.getTaskExecutions({
        type: 'by_execution_ids',
        executionIds: updatedExecutionIds,
      })
      if (executions.length !== updatedExecutionIds.length) {
        throw new DurableExecutionError('Some task executions not found', true)
      }

      for (const execution of executions) {
        await this.closeFinishedTaskExecutionParent(tx, execution, now)
        await this.closeFinishedTaskExecutionChildren(tx, execution, now)
      }
      return true
    })
  }

  /**
   * Close finished task execution parent. If the execution status is completed, it will update the
   * status of the parent to completed if it is waiting for children and all the children are
   * completed. If the parent execution has already finished, it will just update the children
   * state.
   */
  private async closeFinishedTaskExecutionParent(
    tx: DurableStorageTx,
    execution: DurableTaskExecutionStorageObject,
    now: Date,
  ): Promise<void> {
    if (!execution.parentTask) {
      return
    }

    const parentTaskInternal = this.taskInternalsMap.get(execution.parentTask.taskId)
    if (!parentTaskInternal) {
      this.logger.error(`Parent task ${execution.parentTask.taskId} not found`)
      return
    }
    const parentExecutions = await tx.getTaskExecutions({
      type: 'by_execution_ids',
      executionIds: [execution.parentTask.executionId],
    })
    if (parentExecutions.length === 0) {
      this.logger.error(`Parent task execution ${execution.parentTask.executionId} not found`)
      return
    }
    const parentExecution = parentExecutions[0]!

    const status = execution.status
    const parentChildren = parentExecution.childrenTasks ?? []

    // Handle finished finalize task child.
    if (execution.parentTask.isFinalizeTask) {
      const parentExecutionFinalizeTask = parentExecution.finalizeTask
      if (parentExecution.status === 'waiting_for_finalize_task' && status === 'completed') {
        // If the parent execution is waiting for the finalize task to complete, and it got
        // completed, update the output and status to completed. We're done with the parent task
        // execution.
        await tx.updateTaskExecutions(
          { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
          {
            output: execution.output!,
            finalizeTask: parentExecutionFinalizeTask,
            unsetError: true,
            status: 'completed',
            finishedAt: now,
            updatedAt: now,
          },
        )
      } else if (status !== 'completed') {
        // If the child execution is failed, mark the parent execution as failed if it was not
        // finished.
        await tx.updateTaskExecutions(
          { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
          {
            finalizeTaskError:
              execution.error ??
              convertDurableExecutionErrorToStorageObject(
                new DurableExecutionError('Unknown error', false),
              ),
            status:
              parentExecution.status === 'waiting_for_finalize_task'
                ? 'finalize_task_failed'
                : parentExecution.status,
            finishedAt: now,
            updatedAt: now,
          },
        )
      }
      return
    }

    // Handle finished child.
    const childIdx = parentChildren.findIndex(
      (child) => child.executionId === execution.executionId,
    )
    if (childIdx === -1) {
      this.logger.error(
        `Child execution ${execution.executionId} not found for parent task execution ${execution.parentTask.executionId}`,
      )
      return
    }
    if (status === 'completed') {
      const areAllChildrenCompleted =
        parentExecution.childrenTasksCompletedCount >= parentChildren.length - 1
      if (parentExecution.status === 'waiting_for_children_tasks' && areAllChildrenCompleted) {
        const childExecutionIdToIndexMap = new Map<string, number>(
          parentChildren.map((parentChild, index) => [parentChild.executionId, index]),
        )

        // If the parent execution is waiting for all the children to complete, and all the children
        // are completed, we can run the finalize task if present, otherwise we can just mark the
        // parent execution as completed.
        const childrenExecutions = await tx.getTaskExecutions({
          type: 'by_execution_ids',
          executionIds: parentChildren.map((child) => child.executionId),
        })
        const childrenTasksOutputs = childrenExecutions.map((childExecution) => {
          return {
            index: childExecutionIdToIndexMap.get(childExecution.executionId)!,
            taskId: childExecution.taskId,
            executionId: childExecution.executionId,
            output: this.serializer.deserialize(childExecution.output!),
          }
        })
        childrenTasksOutputs.sort((a, b) => a.index - b.index)

        if (parentTaskInternal.finalizeTask) {
          const finalizeTaskTaskInternal = this.taskInternalsMap.get(
            parentTaskInternal.finalizeTask.id,
          )
          if (!finalizeTaskTaskInternal) {
            throw new DurableExecutionError(
              `Parent finalize task ${parentTaskInternal.finalizeTask.id} not found`,
              false,
            )
          }

          const finalizeTaskInput = {
            input: this.serializer.deserialize(parentExecution.runInput),
            output: this.serializer.deserialize(parentExecution.runOutput!),
            childrenTasksOutputs,
          }
          const finalizeTaskRunInput =
            await finalizeTaskTaskInternal.validateInput(finalizeTaskInput)
          const executionId = generateTaskExecutionId()
          const retryOptions = finalizeTaskTaskInternal.getRetryOptions()
          const sleepMsBeforeRun = finalizeTaskTaskInternal.getSleepMsBeforeRun()
          const timeoutMs = finalizeTaskTaskInternal.getTimeoutMs()
          await tx.insertTaskExecutions([
            createDurableTaskExecutionStorageObject({
              now,
              rootTask: execution.rootTask,
              parentTask: {
                ...execution.parentTask,
                isFinalizeTask: true,
              },
              taskId: finalizeTaskTaskInternal.id,
              executionId,
              retryOptions,
              sleepMsBeforeRun,
              timeoutMs,
              runInput: this.serializer.serialize(finalizeTaskRunInput),
            }),
          ])

          await tx.updateTaskExecutions(
            { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
            {
              childrenTasksCompletedCount: parentExecution.childrenTasksCompletedCount + 1,
              finalizeTask: {
                taskId: finalizeTaskTaskInternal.id,
                executionId,
              },
              unsetError: true,
              status: 'waiting_for_finalize_task',
              updatedAt: now,
            },
          )
        } else {
          await tx.updateTaskExecutions(
            { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
            {
              output: parentTaskInternal.disableChildrenTasksOutputsInOutput
                ? parentExecution.runOutput!
                : this.serializer.serialize({
                    output: this.serializer.deserialize(parentExecution.runOutput!),
                    childrenTasksOutputs,
                  }),
              childrenTasksCompletedCount: parentExecution.childrenTasksCompletedCount + 1,
              unsetError: true,
              status: 'completed',
              finishedAt: now,
              updatedAt: now,
            },
          )
        }
      } else {
        // If the parent execution is finished or some children haven't finished yet, we can just
        // update the children count and children.
        await tx.updateTaskExecutions(
          {
            type: 'by_execution_ids',
            executionIds: [parentExecution.executionId],
          },
          {
            childrenTasksCompletedCount: parentExecution.childrenTasksCompletedCount + 1,
            updatedAt: now,
          },
        )
      }
    } else {
      // If the child failed, update the children errors. Update the parent execution status if it
      // is waiting for children to finish. Otherwise, the parent execution status is not updated
      // because it has already finished (failed).
      const childrenTasksErrors = parentExecution.childrenTasksErrors ?? []
      childrenTasksErrors.push({
        index: childIdx,
        taskId: execution.taskId,
        executionId: execution.executionId,
        error:
          execution.error ??
          convertDurableExecutionErrorToStorageObject(
            new DurableExecutionError('Unknown error', false),
          ),
      })
      await tx.updateTaskExecutions(
        { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
        {
          childrenTasksErrors,
          status:
            parentExecution.status === 'waiting_for_children_tasks'
              ? 'children_tasks_failed'
              : parentExecution.status,
          finishedAt: now,
          updatedAt: now,
        },
      )
    }
  }

  /**
   * Close finished task execution children and cancel running children task executions.
   */
  private async closeFinishedTaskExecutionChildren(
    tx: DurableStorageTx,
    execution: DurableTaskExecutionStorageObject,
    now: Date,
  ): Promise<void> {
    if (
      !execution.childrenTasks ||
      execution.childrenTasks.length === 0 ||
      execution.status === 'completed'
    ) {
      return
    }

    const childrenExecutionIds = execution.childrenTasks.map((child) => child.executionId)
    await tx.updateTaskExecutions(
      {
        type: 'by_execution_ids',
        executionIds: childrenExecutionIds,
        statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
      },
      {
        error: convertDurableExecutionErrorToStorageObject(
          new DurableExecutionCancelledError(
            `Parent task ${execution.taskId} with execution id ${execution.executionId} failed: ${execution.error?.message ?? 'Unknown error'}`,
          ),
        ),
        status: 'cancelled',
        needsPromiseCancellation: true,
        finishedAt: now,
        updatedAt: now,
      },
    )
  }

  private async retryExpiredRunningTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('retrying expired running task executions', () =>
      this.retryExpiredRunningTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Retry expired running task executions. This will only happen when the process running the
   * execution previously crashed.
   */
  private async retryExpiredRunningTaskExecutionsSingleBatch(): Promise<boolean> {
    return await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await updateTaskExecutionsWithLimit(
        tx,
        {
          type: 'by_statuses',
          statuses: ['running'],
          isClosed: false,
          expiresAtLessThan: now,
        },
        {
          error: convertDurableExecutionErrorToStorageObject(
            new DurableExecutionError('Task expired', false),
          ),
          status: 'ready',
          startAt: now,
          expiresAt: EXPIRES_AT_INFINITY,
          updatedAt: now,
        },
        3,
      )

      this.logger.debug(`Expiring ${executionIds.length} running task executions`)
      return executionIds.length > 0
    })
  }

  private async cancelNeedPromiseCancellationTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('cancelling promise cancellation task executions', () =>
      this.cancelNeedPromiseCancellationTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Cancel task executions that need promise cancellation.
   */
  private async cancelNeedPromiseCancellationTaskExecutionsSingleBatch(): Promise<boolean> {
    if (this.runningTaskExecutionsMap.size === 0) {
      return false
    }

    return await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await updateTaskExecutionsWithLimit(
        tx,
        {
          type: 'by_execution_ids',
          executionIds: [...this.runningTaskExecutionsMap.keys()],
          needsPromiseCancellation: true,
        },
        {
          needsPromiseCancellation: false,
          updatedAt: now,
        },
        5,
      )

      this.logger.debug(
        `Cancelling ${executionIds.length} task executions that need promise cancellation`,
      )
      if (executionIds.length === 0) {
        return false
      }

      for (const executionId of executionIds) {
        const runningExecution = this.runningTaskExecutionsMap.get(executionId)
        if (runningExecution) {
          try {
            runningExecution.cancel()
          } catch (error) {
            this.logger.error(`Error in cancelling task ${executionId}`, error)
          }
          this.runningTaskExecutionsMap.delete(executionId)
        }
      }
      return true
    })
  }

  private async processReadyTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('processing ready task executions', () =>
      this.processReadyTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Process task executions that are ready to run based on status being ready and startAt
   * being in the past.
   */
  private async processReadyTaskExecutionsSingleBatch(): Promise<boolean> {
    return await this.withTransaction(async (tx) => {
      const now = new Date()
      const executionIds = await updateTaskExecutionsWithLimit(
        tx,
        {
          type: 'by_start_at_less_than',
          statuses: ['ready'],
          startAtLessThan: now,
        },
        { status: 'running', startedAt: now, updatedAt: now },
        1,
      )

      this.logger.debug(`Processing ${executionIds.length} ready task executions`)
      if (executionIds.length === 0) {
        return false
      }

      const executions = await tx.getTaskExecutions({
        type: 'by_execution_ids',
        executionIds,
      })
      if (executions.length !== executionIds.length) {
        throw new DurableExecutionError('Some task executions not found', true)
      }

      const toRunExecutions = [] as Array<[DurableTaskInternal, DurableTaskExecutionStorageObject]>
      for (const execution of executions) {
        const taskInternal = this.taskInternalsMap.get(execution.taskId)
        if (!taskInternal) {
          // This will only happen if the task is not registered in this executor.
          // Mark the execution as failed.
          await tx.updateTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [execution.executionId],
              statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS,
            },
            {
              error: convertDurableExecutionErrorToStorageObject(
                new DurableExecutionError('Task not found', false),
              ),
              status: 'failed',
              finishedAt: now,
              updatedAt: now,
            },
          )
          continue
        }

        const expireMs = execution.timeoutMs + this.expireMs
        const expiresAt = new Date(now.getTime() + expireMs)
        await tx.updateTaskExecutions(
          { type: 'by_execution_ids', executionIds: [execution.executionId] },
          { expiresAt, updatedAt: now },
        )
        execution.expiresAt = expiresAt
        toRunExecutions.push([taskInternal, execution])
      }

      for (const [taskInternal, execution] of toRunExecutions) {
        this.runTaskExecutionWithCancelSignal(taskInternal, execution)
      }
      return true
    })
  }

  /**
   * Run a task execution with a cancel signal. It is expected to be in running state.
   *
   * It will add the execution to the running executions map and make sure it is removed from the
   * running executions map if the task execution completes. If the process crashes, the execution
   * will be retried later on expiration.
   *
   * @param taskInternal - The task internal to run.
   * @param execution - The task execution to run.
   */
  private runTaskExecutionWithCancelSignal(
    taskInternal: DurableTaskInternal,
    execution: DurableTaskExecutionStorageObject,
  ): void {
    if (this.runningTaskExecutionsMap.has(execution.executionId)) {
      return
    }

    const [cancelSignal, cancel] = createCancelSignal({ logger: this.logger })
    const promise = this.runTaskExecutionWithContext(taskInternal, execution, cancelSignal)
      .catch((error) => {
        // This should not happen.
        this.logger.error(`Error in running task execution ${execution.executionId}`, error)
      })
      .finally(() => {
        // If runTaskExecutionWithContext fails (should not happen), cancel the execution and
        // remove it from the running executions map. It is retried later on expiration.
        try {
          cancel()
        } catch (error) {
          this.logger.error(`Error in cancelling task execution ${execution.executionId}`, error)
        }
        this.runningTaskExecutionsMap.delete(execution.executionId)
      })
    this.runningTaskExecutionsMap.set(execution.executionId, {
      promise,
      cancel,
    })
  }

  /**
   * Run a task execution with a context. It is expected to be in running state and present in the
   * running executions map.
   *
   * It will update the execution status to failed, timed_out, cancelled,
   * waiting_for_children_tasks, waiting_for_finalize_task, completed, or ready depending on the
   * result of the task. If the task completes successfully, it will update the execution status to
   * waiting_for_children_tasks, waiting_for_finalize_task, or completed. If the task fails, it will
   * update the execution status to failed, timed_out, cancelled, or ready depending on the error.
   * If the error is retryable and the retry attempts are less than the maximum retry attempts, it
   * will update the execution status to ready. All the errors are saved in storage even if the task
   * is retried. They only get cleared if the execution is completed later.
   *
   * If `runTaskExecutionWithContext` runs to completion, the running executions map is updated to
   * remove the execution. All this is atomic so the execution is guaranteed to be removed from the
   * running executions map if `runTaskExecutionWithContext` completes. If it fails but process
   * does not crash, the running executions map is updated in `runTaskExecutionWithCancelSignal`.
   * The task execution remains in the running state and retried later on expiration.
   *
   * @param taskInternal - The task internal to run.
   * @param execution - The task execution to run.
   * @param cancelSignal - The cancel signal.
   */
  private async runTaskExecutionWithContext(
    taskInternal: DurableTaskInternal,
    execution: DurableTaskExecutionStorageObject,
    cancelSignal: CancelSignal,
  ): Promise<void> {
    const ctx: DurableTaskRunContext = {
      taskId: taskInternal.id,
      executionId: execution.executionId,
      cancelSignal,
      shutdownSignal: this.shutdownSignal,
      attempt: execution.retryAttempts,
      prevError: execution.error
        ? convertDurableExecutionErrorStorageObjectToError(execution.error)
        : undefined,
    }

    // Make sure the execution is in running state to not do any wasteful work. This can happen in
    // a rare case where an execution is cancelled in between the execution getting picked up and
    // the function being added to the running executions map.
    await this.withTransaction(async (tx) => {
      const existingExecutions = await tx.getTaskExecutions({
        type: 'by_execution_ids',
        executionIds: [execution.executionId],
      })
      if (existingExecutions.length === 0 || existingExecutions[0]?.status !== 'running') {
        this.logger.error(
          `Task execution ${execution.executionId} is not running: ${existingExecutions[0]!.status}`,
        )
        return
      }
    })

    try {
      const result = await taskInternal.runParentWithTimeoutAndCancellation(
        ctx,
        this.serializer.deserialize(execution.runInput),
        execution.timeoutMs,
        cancelSignal,
      )
      const runOutput = result.output
      const runOutputSerialized = this.serializer.serialize(runOutput)
      const childrenTasks = result.childrenTasks

      const now = new Date()
      if (childrenTasks.length === 0) {
        if (taskInternal.finalizeTask) {
          const finalizeTaskTaskInternal = this.taskInternalsMap.get(taskInternal.finalizeTask.id)
          if (!finalizeTaskTaskInternal) {
            throw new DurableExecutionError(
              `Finalize task ${taskInternal.finalizeTask.id} not found`,
              false,
            )
          }

          const finalizeTaskInput = {
            input: this.serializer.deserialize(execution.runInput),
            output: runOutput,
            childrenTasksOutputs: [],
          }
          const runInput = await finalizeTaskTaskInternal.validateInput(finalizeTaskInput)
          const executionId = generateTaskExecutionId()
          const retryOptions = finalizeTaskTaskInternal.getRetryOptions()
          const sleepMsBeforeRun = finalizeTaskTaskInternal.getSleepMsBeforeRun()
          const timeoutMs = finalizeTaskTaskInternal.getTimeoutMs()
          await this.withTransaction(async (tx) => {
            await tx.insertTaskExecutions([
              createDurableTaskExecutionStorageObject({
                now,
                rootTask: execution.rootTask ?? {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                },
                parentTask: {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                  isFinalizeTask: true,
                },
                taskId: finalizeTaskTaskInternal.id,
                executionId,
                retryOptions,
                sleepMsBeforeRun,
                timeoutMs,
                runInput: this.serializer.serialize(runInput),
              }),
            ])

            await tx.updateTaskExecutions(
              {
                type: 'by_execution_ids',
                executionIds: [execution.executionId],
                statuses: ['running'],
              },
              {
                runOutput: runOutputSerialized,
                childrenTasks: [],
                finalizeTask: {
                  taskId: finalizeTaskTaskInternal.id,
                  executionId,
                },
                unsetError: true,
                status: 'waiting_for_finalize_task',
                updatedAt: now,
              },
            )
          })
        } else {
          await this.withTransaction(async (tx) => {
            await tx.updateTaskExecutions(
              {
                type: 'by_execution_ids',
                executionIds: [execution.executionId],
                statuses: ['running'],
              },
              {
                runOutput: runOutputSerialized,
                output: taskInternal.disableChildrenTasksOutputsInOutput
                  ? runOutputSerialized
                  : this.serializer.serialize({
                      output: runOutput,
                      childrenTasksOutputs: [],
                    }),
                childrenTasks: [],
                unsetError: true,
                status: 'completed',
                finishedAt: now,
                updatedAt: now,
              },
            )

            this.runningTaskExecutionsMap.delete(execution.executionId)
          })
        }
      } else {
        const childrenTaskExecutions: Array<DurableTaskExecutionStorageObject> = []
        const childrenTaskExecutionsStorageObjects: Array<DurableChildTaskExecution> = []
        for (const child of childrenTasks) {
          const childTaskInternal = this.taskInternalsMap.get(child.task.id)
          if (!childTaskInternal) {
            throw new DurableExecutionError(`Child task ${child.task.id} not found`, false)
          }

          const runInput = await childTaskInternal.validateInput(child.input)
          const executionId = generateTaskExecutionId()
          const retryOptions = childTaskInternal.getRetryOptions(child.options)
          const sleepMsBeforeRun = childTaskInternal.getSleepMsBeforeRun(child.options)
          const timeoutMs = childTaskInternal.getTimeoutMs(child.options)
          childrenTaskExecutions.push(
            createDurableTaskExecutionStorageObject({
              now,
              rootTask: execution.rootTask ?? {
                taskId: execution.taskId,
                executionId: execution.executionId,
              },
              parentTask: {
                taskId: execution.taskId,
                executionId: execution.executionId,
              },
              taskId: child.task.id,
              executionId,
              retryOptions,
              sleepMsBeforeRun,
              timeoutMs,
              runInput: this.serializer.serialize(runInput),
            }),
          )
          childrenTaskExecutionsStorageObjects.push({
            taskId: child.task.id,
            executionId,
          })
        }

        await this.withTransaction(async (tx) => {
          await tx.insertTaskExecutions(childrenTaskExecutions)

          await tx.updateTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [execution.executionId],
              statuses: ['running'],
            },
            {
              runOutput: runOutputSerialized,
              childrenTasks: childrenTaskExecutionsStorageObjects,
              unsetError: true,
              status: 'waiting_for_children_tasks',
              updatedAt: now,
            },
          )
        })
      }
    } catch (error) {
      const durableExecutionError =
        error instanceof DurableExecutionError
          ? error
          : new DurableExecutionError(getErrorMessage(error), true)

      const now = new Date()
      let update: DurableTaskExecutionStorageObjectUpdate

      // Check if the error is retryable and the retry attempts are less than the maximum retry
      // attempts. If so, update the execution status to ready.
      if (
        durableExecutionError.isRetryable &&
        execution.retryAttempts < execution.retryOptions.maxAttempts
      ) {
        const baseDelayMs = execution.retryOptions.baseDelayMs ?? 0
        const delayMultiplier = execution.retryOptions.delayMultiplier ?? 1
        const maxDelayMs = execution.retryOptions.maxDelayMs
        let delayMs = baseDelayMs
        if (execution.retryAttempts > 0) {
          delayMs *= Math.pow(delayMultiplier, execution.retryAttempts)
        }
        if (maxDelayMs != null) {
          delayMs = Math.min(delayMs, maxDelayMs)
        }
        update = {
          error: convertDurableExecutionErrorToStorageObject(durableExecutionError),
          status: 'ready',
          retryAttempts: execution.retryAttempts + 1,
          startAt: new Date(now.getTime() + delayMs),
          expiresAt: EXPIRES_AT_INFINITY,
          updatedAt: now,
        }
      } else {
        const status =
          error instanceof DurableExecutionTimedOutError
            ? 'timed_out'
            : error instanceof DurableExecutionCancelledError
              ? 'cancelled'
              : 'failed'
        update = {
          error: convertDurableExecutionErrorToStorageObject(durableExecutionError),
          status,
          finishedAt: now,
          updatedAt: now,
        }
      }

      await this.withTransaction(async (tx) => {
        await tx.updateTaskExecutions(
          {
            type: 'by_execution_ids',
            executionIds: [execution.executionId],
            statuses: ['running'],
          },
          update,
        )

        this.runningTaskExecutionsMap.delete(execution.executionId)
      })
    }
  }

  /**
   * Get the running task execution ids.
   *
   * @returns The running task execution ids.
   */
  getRunningTaskExecutionIds(): ReadonlySet<string> {
    return new Set(this.runningTaskExecutionsMap.keys())
  }
}

type LastElement<T extends ReadonlyArray<unknown>> = T extends readonly [...Array<unknown>, infer L]
  ? L
  : never

type ExtractDurableTaskInput<T> = T extends DurableTask<infer I, unknown> ? I : never
type ExtractDurableTaskOutput<T> = T extends DurableTask<unknown, infer O> ? O : never

/**
 * The type of a sequence of tasks. Disallows empty sequences and sequences with tasks that have
 * different input and output types.
 *
 * @category Task
 */
export type SequentialDurableTasks<T extends ReadonlyArray<DurableTask<unknown, unknown>>> =
  T extends readonly [] ? never : SequentialDurableTasksHelper<T>

/**
 * A helper type to create a sequence of tasks. See {@link SequentialDurableTasks} for more details.
 *
 * @category Task
 */
export type SequentialDurableTasksHelper<T extends ReadonlyArray<DurableTask<unknown, unknown>>> =
  T extends readonly []
    ? T
    : T extends readonly [DurableTask<infer _I1, infer _O1>]
      ? T
      : T extends readonly [
            DurableTask<infer I1, infer O1>,
            DurableTask<infer I2, infer O2>,
            ...infer Rest,
          ]
        ? O1 extends I2
          ? Rest extends ReadonlyArray<DurableTask<unknown, unknown>>
            ? readonly [
                DurableTask<I1, O1>,
                ...SequentialDurableTasksHelper<readonly [DurableTask<I2, O2>, ...Rest]>,
              ]
            : never
          : never
        : T
