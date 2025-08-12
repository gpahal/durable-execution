import type { StandardSchemaV1 } from '@standard-schema/spec'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
import { getErrorMessage } from '@gpahal/std/errors'
import { sleep } from '@gpahal/std/promises'

import { createCancellablePromiseCustom } from './cancel'
import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from './errors'
import { createConsoleLogger, createLoggerWithDebugDisabled, type Logger } from './logger'
import { createSuperjsonSerializer, WrappedSerializer, type Serializer } from './serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  createTaskExecutionStorageValue,
  getTaskExecutionStorageValueParentExecutionError,
  StorageInternal,
  type Storage,
  type StorageTx,
  type StorageTxInternal,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from './storage'
import {
  ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
  FINISHED_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
  type ChildTaskExecution,
  type ChildTaskExecutionOutput,
  type InferTaskInput,
  type InferTaskOutput,
  type LastTaskElementInArray,
  type ParentTaskOptions,
  type SequentialTasks,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
  type TaskFinishedExecution,
  type TaskOptions,
  type TaskRunContext,
} from './task'
import { generateTaskExecutionId, TaskInternal } from './task-internal'
import { generateId, sleepWithJitter, summarizeStandardSchemaIssues } from './utils'

export {
  type CancelSignal,
  createCancelSignal,
  createTimeoutCancelSignal,
  createCancellablePromise,
} from '@gpahal/std/cancel'
export {
  type DurableExecutionErrorType,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
  DurableExecutionCancelledError,
  type DurableExecutionErrorStorageValue,
} from './errors'
export { type Logger, createConsoleLogger } from './logger'
export type {
  Task,
  InferTaskInput,
  InferTaskOutput,
  TaskCommonOptions,
  TaskRetryOptions,
  TaskOptions,
  ParentTaskOptions,
  FinalizeTaskOptions,
  FinalizeTaskInput,
  TaskRunContext,
  TaskExecution,
  TaskFinishedExecution,
  TaskReadyExecution,
  TaskRunningExecution,
  TaskFailedExecution,
  TaskTimedOutExecution,
  TaskCancelledExecution,
  TaskWaitingForChildrenTasksExecution,
  TaskChildrenTasksFailedExecution,
  TaskWaitingForFinalizeTaskExecution,
  TaskFinalizeTaskFailedExecution,
  TaskCompletedExecution,
  ChildTask,
  ChildTaskExecution,
  ChildTaskExecutionOutput,
  ChildTaskExecutionError,
  ChildTaskExecutionErrorStorageValue,
  TaskExecutionStatusStorageValue,
  TaskEnqueueOptions,
  TaskExecutionHandle,
  SequentialTasks,
  SequentialTasksHelper,
  LastTaskElementInArray,
} from './task'
export { type Serializer, createSuperjsonSerializer, WrappedSerializer } from './serializer'
export {
  type Storage,
  type StorageTx,
  type TaskExecutionStorageValue,
  type TaskExecutionStorageWhere,
  type TaskExecutionStorageUpdate,
  InMemoryStorage,
  InMemoryStorageTx,
} from './storage'
export { type Mutex, createMutex } from '@gpahal/std/promises'

/**
 * A durable executor. It is used to execute tasks durably, reliably and resiliently.
 *
 * Multiple durable executors can share the same storage. In such a case, all the tasks should be
 * present for all the durable executors. The work is distributed among the durable executors. See
 * the [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
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
 *       run: async (ctx, { input, output, childrenTaskExecutionsOutputs }) => {
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
  private readonly logger: Logger
  private readonly storage: StorageInternal
  private readonly serializer: Serializer
  private readonly expireMs: number
  private readonly backgroundProcessIntraBatchSleepMs: number
  private readonly maxConcurrentTaskExecutions: number
  private readonly maxTasksPerBatch: number
  private readonly taskInternalsMap: Map<string, TaskInternal>
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
   * @param options.logger - The logger to use for the durable executor. If not provided, a console
   *   logger will be used.
   * @param options.serializer - The serializer to use for the durable executor. If not provided, a
   *   default serializer using superjson will be used.
   * @param options.enableDebug - Whether to enable debug logging. If `true`, debug logging will
   *   be enabled.
   * @param options.expireMs - The duration after which a task execution is considered expired.
   *   If not provided, defaults to 300_000 (5 minutes).
   * @param options.backgroundProcessIntraBatchSleepMs - The duration to sleep between batches of
   *   background processes. If not provided, defaults to 500 (500ms).
   * @param options.maxConcurrentTaskExecutions - The maximum number of tasks that can run concurrently.
   *   If not provided, defaults to 1000.
   * @param options.maxTasksPerBatch - The maximum number of tasks to process in each batch.
   *   If not provided, defaults to 3.
   * @param options.storageMaxRetryAttempts - The maximum number of times to retry a storage
   *   operation. If not provided, defaults to 1.
   */
  constructor(
    storage: Storage,
    {
      logger,
      serializer,
      enableDebug = false,
      expireMs,
      backgroundProcessIntraBatchSleepMs,
      maxConcurrentTaskExecutions,
      maxTasksPerBatch,
      storageMaxRetryAttempts,
    }: {
      logger?: Logger
      serializer?: Serializer
      enableDebug?: boolean
      expireMs?: number
      backgroundProcessIntraBatchSleepMs?: number
      maxConcurrentTaskExecutions?: number
      maxTasksPerBatch?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    this.logger = logger ?? createConsoleLogger('DurableExecutor')
    if (!enableDebug) {
      this.logger = createLoggerWithDebugDisabled(this.logger)
    }

    this.storage = new StorageInternal(this.logger, storage, storageMaxRetryAttempts)
    this.serializer = new WrappedSerializer(serializer ?? createSuperjsonSerializer())
    this.expireMs = expireMs && expireMs > 0 ? expireMs : 300_000 // 5 minutes
    this.backgroundProcessIntraBatchSleepMs =
      backgroundProcessIntraBatchSleepMs && backgroundProcessIntraBatchSleepMs > 0
        ? backgroundProcessIntraBatchSleepMs
        : 500 // 500ms
    this.maxConcurrentTaskExecutions =
      maxConcurrentTaskExecutions && maxConcurrentTaskExecutions > 0
        ? maxConcurrentTaskExecutions
        : 1000
    this.maxTasksPerBatch = maxTasksPerBatch && maxTasksPerBatch > 0 ? maxTasksPerBatch : 3

    this.taskInternalsMap = new Map()
    this.runningTaskExecutionsMap = new Map()
    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.internalCancel = cancel
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
   * @param taskOptions - The task options. See {@link TaskOptions} for more details on the
   *   task options.
   * @returns The task.
   */
  task<TInput = undefined, TOutput = unknown>(
    taskOptions: TaskOptions<TInput, TOutput>,
  ): Task<TInput, TOutput> {
    this.throwIfShutdown()
    const taskInternal = TaskInternal.fromTaskOptions(this.taskInternalsMap, taskOptions)
    this.logger.debug(`Added task ${taskOptions.id}`)
    return {
      id: taskInternal.id,
    } as Task<TInput, TOutput>
  }

  /**
   * Add a parent task to the durable executor. See the
   * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) section
   * for more details on creating parent tasks.
   *
   * @param parentTaskOptions - The parent task options. See {@link ParentTaskOptions} for
   *   more details on the parent task options.
   * @returns The parent task.
   */
  parentTask<
    TInput = undefined,
    TRunOutput = unknown,
    TOutput = {
      output: TRunOutput
      childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
    },
    TFinalizeTaskRunOutput = unknown,
  >(
    parentTaskOptions: ParentTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
  ): Task<TInput, TOutput> {
    this.throwIfShutdown()
    const taskInternal = TaskInternal.fromParentTaskOptions(
      this.taskInternalsMap,
      parentTaskOptions,
    )
    this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
    return {
      id: taskInternal.id,
    } as Task<TInput, TOutput>
  }

  validateInput<TRunInput, TInput>(
    validateInputFn: (input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput = unknown>(taskOptions: TaskOptions<TRunInput, TOutput>) => Task<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
      },
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    ) => Task<TInput, TOutput>
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
      taskOptions: TaskOptions<StandardSchemaV1.InferOutput<TInputSchema>, TOutput>,
    ) => Task<StandardSchemaV1.InferInput<TInputSchema>, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
      },
    >(
      parentTaskOptions: ParentTaskOptions<
        StandardSchemaV1.InferOutput<TInputSchema>,
        TRunOutput,
        TOutput
      >,
    ) => Task<StandardSchemaV1.InferInput<TInputSchema>, TOutput>
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
    task: <TOutput>(taskOptions: TaskOptions<TRunInput, TOutput>) => Task<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = {
        output: TRunOutput
        childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
      },
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    ) => Task<TInput, TOutput>
  } {
    this.throwIfShutdown()
    return {
      task: <TOutput>(taskOptions: TaskOptions<TRunInput, TOutput>): Task<TInput, TOutput> => {
        this.throwIfShutdown()
        const taskInternal = TaskInternal.fromTaskOptions(
          this.taskInternalsMap,
          taskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added task ${taskOptions.id}`)
        return {
          id: taskInternal.id,
        } as Task<TInput, TOutput>
      },
      parentTask: <
        TRunOutput = unknown,
        TOutput = {
          output: TRunOutput
          childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
        },
        TFinalizeTaskRunOutput = unknown,
      >(
        parentTaskOptions: ParentTaskOptions<
          TRunInput,
          TRunOutput,
          TOutput,
          TFinalizeTaskRunOutput
        >,
      ): Task<TInput, TOutput> => {
        this.throwIfShutdown()
        const taskInternal = TaskInternal.fromParentTaskOptions(
          this.taskInternalsMap,
          parentTaskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
        return {
          id: taskInternal.id,
        } as Task<TInput, TOutput>
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
  sequentialTasks<T extends ReadonlyArray<Task<unknown, unknown>>>(
    ...tasks: SequentialTasks<T>
  ): Task<InferTaskInput<T[0]>, InferTaskOutput<LastTaskElementInArray<T>>> {
    if (tasks.length === 0) {
      throw new DurableExecutionError('No tasks provided', false)
    }
    if (tasks.length === 1) {
      return tasks[0]
    }

    const firstTask = tasks[0]
    const secondTask = this.sequentialTasks(
      ...(tasks.slice(1) as SequentialTasks<ReadonlyArray<Task<unknown, unknown>>>),
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
        runParent: (_, { childrenTaskExecutionsOutputs }) => {
          const firstTaskOutput = childrenTaskExecutionsOutputs[0]!.output
          return {
            output: undefined,
            childrenTasks: [{ task: secondTask, input: firstTaskOutput }],
          }
        },
        finalizeTask: {
          id: `${taskId}_finalize_2`,
          timeoutMs: 1000,
          run: (_, { childrenTaskExecutionsOutputs }) => {
            const secondTaskOutput = childrenTaskExecutionsOutputs[0]!.output
            return secondTaskOutput
          },
        },
      },
    })
  }

  /**
   * Enqueue a task for execution.
   *
   * @param rest - The task to enqueue, input, and options.
   * @returns A handle to the task execution.
   */
  async enqueueTask<TTask extends Task<unknown, unknown>>(
    ...rest: undefined extends InferTaskInput<TTask>
      ? [
          task: TTask,
          input?: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions & { tx?: StorageTx },
        ]
      : [
          task: TTask,
          input: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions & { tx?: StorageTx },
        ]
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    this.throwIfShutdown()

    const task = rest[0]
    const input = rest.length > 1 ? rest[1]! : undefined
    const options = rest.length > 2 ? rest[2]! : undefined
    const taskInternal = this.taskInternalsMap.get(task.id)
    if (!taskInternal) {
      throw new DurableExecutionNotFoundError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it.`,
      )
    }

    const runInput = await taskInternal.validateInput(input)
    const executionId = generateTaskExecutionId()
    const now = new Date()
    const retryOptions = taskInternal.getRetryOptions(options)
    const sleepMsBeforeRun = taskInternal.getSleepMsBeforeRun(options)
    const timeoutMs = taskInternal.getTimeoutMs(options)
    await this.storage.withExistingTransaction(options?.tx, async (tx) => {
      await tx.insertTaskExecutions([
        createTaskExecutionStorageValue({
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
  async getTaskHandle<TTask extends Task<unknown, unknown>>(
    task: TTask,
    executionId: string,
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    const taskInternal = this.taskInternalsMap.get(task.id)
    if (!taskInternal) {
      throw new DurableExecutionNotFoundError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it.`,
      )
    }

    const execution = await this.storage.getTaskExecutionById(executionId)
    if (!execution) {
      throw new DurableExecutionNotFoundError(`Execution ${executionId} not found`)
    }

    return this.getTaskHandleInternal(task.id, executionId)
  }

  private getTaskHandleInternal<TOutput>(
    taskId: string,
    executionId: string,
  ): TaskExecutionHandle<TOutput> {
    return {
      getTaskId: () => taskId,
      getExecutionId: () => executionId,
      getExecution: async () => {
        const execution = await this.storage.getTaskExecutionById(executionId)
        if (!execution) {
          throw new DurableExecutionNotFoundError(`Execution ${executionId} not found`)
        }
        return convertTaskExecutionStorageValueToTaskExecution(execution, this.serializer)
      },
      waitAndGetFinishedExecution: async ({
        signal,
        pollingIntervalMs,
      }: {
        signal?: CancelSignal | AbortSignal
        pollingIntervalMs?: number
      } = {}) => {
        const cancelSignal =
          signal instanceof AbortSignal ? createCancelSignal({ abortSignal: signal })[0] : signal

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
            await createCancellablePromiseCustom(sleep(resolvedPollingIntervalMs), cancelSignal)

            if (cancelSignal?.isCancelled()) {
              throw new DurableExecutionCancelledError()
            }
          }

          const execution = await this.storage.getTaskExecutionById(executionId)
          if (!execution) {
            throw new DurableExecutionNotFoundError(`Execution ${executionId} not found`)
          }

          if (FINISHED_TASK_EXECUTION_STATUSES_STORAGE_VALUES.includes(execution.status)) {
            return convertTaskExecutionStorageValueToTaskExecution(
              execution,
              this.serializer,
            ) as TaskFinishedExecution<TOutput>
          } else {
            this.logger.debug(
              `Waiting for task ${executionId} to be finished. Status: ${execution.status}`,
            )
          }
        }
      },
      cancel: async () => {
        const now = new Date()
        await this.storage.updateAllTaskExecutions(
          {
            type: 'by_execution_ids',
            executionIds: [executionId],
            statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
          },
          {
            error: convertDurableExecutionErrorToStorageValue(new DurableExecutionCancelledError()),
            status: 'cancelled',
            needsPromiseCancellation: true,
            finishedAt: now,
            updatedAt: now,
          },
        )
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
        const hasNonEmptyResult = await createCancellablePromiseCustom(
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
    return await this.storage.withTransaction(async (tx) => {
      const now = new Date()
      const updatedExecutions = await tx.updateTaskExecutionsReturningTaskExecutions(
        {
          type: 'by_statuses',
          statuses: FINISHED_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
          isClosed: false,
        },
        { isClosed: true, updatedAt: now },
        1,
      )

      this.logger.debug(`Closing ${updatedExecutions.length} finished task executions`)
      if (updatedExecutions.length === 0) {
        return false
      }

      for (const execution of updatedExecutions) {
        execution.isClosed = true
        execution.updatedAt = now
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
    tx: StorageTxInternal,
    execution: TaskExecutionStorageValue,
    now: Date,
  ): Promise<void> {
    if (!execution.parentTaskExecution) {
      return
    }

    const parentTaskInternal = this.taskInternalsMap.get(execution.parentTaskExecution.taskId)
    if (!parentTaskInternal) {
      this.logger.error(`Parent task ${execution.parentTaskExecution.taskId} not found`)
      return
    }
    const parentExecution = await tx.getTaskExecutionById(execution.parentTaskExecution.executionId)
    if (!parentExecution) {
      this.logger.error(
        `Parent task execution ${execution.parentTaskExecution.executionId} not found`,
      )
      return
    }

    const status = execution.status
    const parentChildren = parentExecution.childrenTaskExecutions ?? []

    // Handle finished finalize task child.
    if (execution.parentTaskExecution.isFinalizeTask) {
      const parentExecutionFinalizeTask = parentExecution.finalizeTaskExecution
      if (parentExecution.status === 'waiting_for_finalize_task' && status === 'completed') {
        // If the parent execution is waiting for the finalize task to complete, and it got
        // completed, update the output and status to completed. We're done with the parent task
        // execution.
        await tx.updateAllTaskExecutions(
          { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
          {
            output: execution.output!,
            finalizeTaskExecution: parentExecutionFinalizeTask,
            unsetError: true,
            status: 'completed',
            finishedAt: now,
            updatedAt: now,
          },
        )
      } else if (status !== 'completed') {
        // If the child execution is failed, mark the parent execution as failed if it was not
        // finished.
        await tx.updateAllTaskExecutions(
          { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
          {
            finalizeTaskExecutionError: getTaskExecutionStorageValueParentExecutionError(execution),
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
        `Child execution ${execution.executionId} not found for parent task execution ${execution.parentTaskExecution.executionId}`,
      )
      return
    }
    if (status === 'completed') {
      const areAllChildrenCompleted =
        parentExecution.childrenTaskExecutionsCompletedCount >= parentChildren.length - 1
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
        const childrenTaskExecutionsOutputs = childrenExecutions.map((childExecution) => {
          return {
            index: childExecutionIdToIndexMap.get(childExecution.executionId)!,
            taskId: childExecution.taskId,
            executionId: childExecution.executionId,
            output: this.serializer.deserialize(childExecution.output!),
          }
        })
        childrenTaskExecutionsOutputs.sort((a, b) => a.index - b.index)

        if (parentTaskInternal.finalizeTask) {
          const finalizeTaskTaskInternal = this.taskInternalsMap.get(
            parentTaskInternal.finalizeTask.id,
          )
          if (!finalizeTaskTaskInternal) {
            this.logger.error(
              `Parent finalize task ${parentTaskInternal.finalizeTask.id} not found`,
            )
            return
          }

          const finalizeTaskInput = {
            input: this.serializer.deserialize(parentExecution.runInput),
            output: this.serializer.deserialize(parentExecution.runOutput!),
            childrenTaskExecutionsOutputs,
          }
          const finalizeTaskRunInput =
            await finalizeTaskTaskInternal.validateInput(finalizeTaskInput)
          const executionId = generateTaskExecutionId()
          const retryOptions = finalizeTaskTaskInternal.getRetryOptions()
          const sleepMsBeforeRun = finalizeTaskTaskInternal.getSleepMsBeforeRun()
          const timeoutMs = finalizeTaskTaskInternal.getTimeoutMs()
          await tx.insertTaskExecutions([
            createTaskExecutionStorageValue({
              now,
              rootTaskExecution: execution.rootTaskExecution,
              parentTaskExecution: {
                ...execution.parentTaskExecution,
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

          await tx.updateTaskExecutionWithOptimisticLocking(
            parentExecution.executionId,
            (currentExecution) => ({
              childrenTaskExecutionsCompletedCount:
                currentExecution.childrenTaskExecutionsCompletedCount + 1,
              finalizeTaskExecution: {
                taskId: finalizeTaskTaskInternal.id,
                executionId,
              },
              unsetError: true,
              status: 'waiting_for_finalize_task',
              updatedAt: now,
            }),
          )
        } else {
          await tx.updateTaskExecutionWithOptimisticLocking(
            parentExecution.executionId,
            (currentExecution) => ({
              output: parentTaskInternal.disableChildrenTaskExecutionsOutputsInOutput
                ? currentExecution.runOutput!
                : this.serializer.serialize({
                    output: this.serializer.deserialize(currentExecution.runOutput!),
                    childrenTaskExecutionsOutputs,
                  }),
              childrenTaskExecutionsCompletedCount:
                currentExecution.childrenTaskExecutionsCompletedCount + 1,
              unsetError: true,
              status: 'completed',
              finishedAt: now,
              updatedAt: now,
            }),
          )
        }
      } else {
        // If the parent execution is finished or some children haven't finished yet, we can just
        // update the children count and children using optimistic locking.
        await tx.updateTaskExecutionWithOptimisticLocking(
          parentExecution.executionId,
          (currentExecution) => ({
            childrenTaskExecutionsCompletedCount:
              currentExecution.childrenTaskExecutionsCompletedCount + 1,
            updatedAt: now,
          }),
        )
      }
    } else {
      // If the child failed, update the children errors. Update the parent execution status if it
      // is waiting for children to finish. Otherwise, the parent execution status is not updated
      // because it has already finished (failed).
      const childrenTaskExecutionsErrors = parentExecution.childrenTaskExecutionsErrors ?? []
      childrenTaskExecutionsErrors.push({
        index: childIdx,
        taskId: execution.taskId,
        executionId: execution.executionId,
        error: getTaskExecutionStorageValueParentExecutionError(execution),
      })
      await tx.updateAllTaskExecutions(
        { type: 'by_execution_ids', executionIds: [parentExecution.executionId] },
        {
          childrenTaskExecutionsErrors,
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
    tx: StorageTxInternal,
    execution: TaskExecutionStorageValue,
    now: Date,
  ): Promise<void> {
    if (execution.status === 'completed') {
      return
    }

    const childrenExecutionIds: Array<string> = []
    if (execution.childrenTaskExecutions && execution.childrenTaskExecutions.length > 0) {
      for (const child of execution.childrenTaskExecutions) {
        childrenExecutionIds.push(child.executionId)
      }
    }
    if (execution.finalizeTaskExecution) {
      childrenExecutionIds.push(execution.finalizeTaskExecution.executionId)
    }
    if (childrenExecutionIds.length === 0) {
      return
    }

    await tx.updateAllTaskExecutions(
      {
        type: 'by_execution_ids',
        executionIds: childrenExecutionIds,
        statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
      },
      {
        error: convertDurableExecutionErrorToStorageValue(
          new DurableExecutionCancelledError(
            `Parent task ${execution.taskId} with execution id ${execution.executionId} failed: ${getTaskExecutionStorageValueParentExecutionError(execution).message}`,
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
    const now = new Date()
    const executions = await this.storage.updateTaskExecutionsReturningTaskExecutions(
      {
        type: 'by_statuses',
        statuses: ['running'],
        isClosed: false,
        expiresAtLessThan: now,
      },
      {
        error: convertDurableExecutionErrorToStorageValue(
          new DurableExecutionError('Task expired', false),
        ),
        status: 'ready',
        startAt: now,
        unsetExpiresAt: true,
        updatedAt: now,
      },
      3,
    )

    this.logger.debug(`Expiring ${executions.length} running task executions`)
    return executions.length > 0
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

    const executions = await this.storage.getTaskExecutions(
      {
        type: 'by_execution_ids',
        executionIds: [...this.runningTaskExecutionsMap.keys()],
        needsPromiseCancellation: true,
      },
      5,
    )

    this.logger.debug(
      `Cancelling ${executions.length} task executions that need promise cancellation`,
    )
    if (executions.length === 0) {
      return false
    }

    for (const execution of executions) {
      const runningExecution = this.runningTaskExecutionsMap.get(execution.executionId)
      if (runningExecution) {
        try {
          runningExecution.cancel()
        } catch (error) {
          this.logger.error(`Error in cancelling task ${execution.executionId}`, error)
        }
        this.runningTaskExecutionsMap.delete(execution.executionId)
      }
    }

    const now = new Date()
    await this.storage.updateAllTaskExecutions(
      {
        type: 'by_execution_ids',
        executionIds: executions.map((execution) => execution.executionId),
        needsPromiseCancellation: true,
      },
      {
        needsPromiseCancellation: false,
        updatedAt: now,
      },
    )
    return true
  }

  private async processReadyTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('processing ready task executions', () =>
      this.processReadyTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Process task executions that are ready to run based on status being ready and startAt
   * being in the past. Implements backpressure by respecting maxConcurrentTaskExecutions limit.
   */
  private async processReadyTaskExecutionsSingleBatch(): Promise<boolean> {
    const currConcurrentTaskExecutions = this.runningTaskExecutionsMap.size
    if (currConcurrentTaskExecutions >= this.maxConcurrentTaskExecutions) {
      this.logger.debug(
        `At max concurrent task execution limit (${currConcurrentTaskExecutions}/${this.maxConcurrentTaskExecutions}), skipping processing ready task executions`,
      )
      return false
    }

    const availableLimit = this.maxConcurrentTaskExecutions - currConcurrentTaskExecutions
    const batchLimit = Math.min(this.maxTasksPerBatch, availableLimit)
    return await this.storage.withTransaction(async (tx) => {
      const now = new Date()
      const executions = await tx.updateTaskExecutionsReturningTaskExecutions(
        {
          type: 'by_start_at_less_than',
          statuses: ['ready'],
          startAtLessThan: now,
        },
        { status: 'running', startedAt: now, updatedAt: now },
        batchLimit,
      )

      this.logger.debug(`Processing ${executions.length} ready task executions`)
      if (executions.length === 0) {
        return false
      }

      const processExecution = async (
        execution: TaskExecutionStorageValue,
      ): Promise<[TaskInternal, TaskExecutionStorageValue] | undefined> => {
        execution.status = 'running'
        execution.startedAt = now
        execution.updatedAt = now

        const taskInternal = this.taskInternalsMap.get(execution.taskId)
        if (!taskInternal) {
          // This will only happen if the task is not registered in this executor.
          // Mark the execution as failed.
          await tx.updateAllTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [execution.executionId],
              statuses: ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_VALUES,
            },
            {
              error: convertDurableExecutionErrorToStorageValue(
                new DurableExecutionNotFoundError('Task not found'),
              ),
              status: 'failed',
              finishedAt: now,
              updatedAt: now,
            },
          )
          return undefined
        }

        const expireMs = execution.timeoutMs + this.expireMs
        const expiresAt = new Date(now.getTime() + expireMs)
        await tx.updateAllTaskExecutions(
          { type: 'by_execution_ids', executionIds: [execution.executionId] },
          { expiresAt, updatedAt: now },
        )
        execution.expiresAt = expiresAt
        return [taskInternal, execution]
      }

      const toRunExecutions = await Promise.all(
        executions.map((execution) => processExecution(execution)),
      )

      for (const toRunExecution of toRunExecutions) {
        if (toRunExecution) {
          this.runTaskExecutionWithCancelSignal(toRunExecution[0], toRunExecution[1])
        }
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
    taskInternal: TaskInternal,
    execution: TaskExecutionStorageValue,
  ): void {
    if (this.runningTaskExecutionsMap.has(execution.executionId)) {
      return
    }

    const [cancelSignal, cancel] = createCancelSignal()
    const promise = this.runTaskExecutionWithContext(taskInternal, execution, cancelSignal)
      .catch((error) => {
        this.logger.error(`Error in running task execution ${execution.executionId}`, error)
      })
      .finally(() => {
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
   * @param taskInternal - The task internal to run.
   * @param execution - The task execution to run.
   * @param cancelSignal - The cancel signal.
   */
  private async runTaskExecutionWithContext(
    taskInternal: TaskInternal,
    execution: TaskExecutionStorageValue,
    cancelSignal: CancelSignal,
  ): Promise<void> {
    const ctx: TaskRunContext = {
      taskId: taskInternal.id,
      executionId: execution.executionId,
      cancelSignal,
      shutdownSignal: this.shutdownSignal,
      attempt: execution.retryAttempts,
      prevError: execution.error,
    }

    // Make sure the execution is in running state to not do any wasteful work. This can happen in
    // a rare case where an execution is cancelled in between the execution getting picked up and
    // the function being added to the running executions map.
    const existingExecution = await this.storage.getTaskExecutionById(execution.executionId)
    if (!existingExecution) {
      this.logger.error(`Task execution ${execution.executionId} not found`)
      return
    }
    if (existingExecution.status !== 'running') {
      this.logger.error(
        `Task execution ${execution.executionId} is not running: ${existingExecution.status}`,
      )
      return
    }

    execution = existingExecution
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
            throw new DurableExecutionNotFoundError(
              `Finalize task ${taskInternal.finalizeTask.id} not found`,
            )
          }

          const finalizeTaskInput = {
            input: this.serializer.deserialize(execution.runInput),
            output: runOutput,
            childrenTaskExecutionsOutputs: [],
          }
          const runInput = await finalizeTaskTaskInternal.validateInput(finalizeTaskInput)
          const executionId = generateTaskExecutionId()
          const retryOptions = finalizeTaskTaskInternal.getRetryOptions()
          const sleepMsBeforeRun = finalizeTaskTaskInternal.getSleepMsBeforeRun()
          const timeoutMs = finalizeTaskTaskInternal.getTimeoutMs()
          await this.storage.withTransaction(async (tx) => {
            await tx.insertTaskExecutions([
              createTaskExecutionStorageValue({
                now,
                rootTaskExecution: execution.rootTaskExecution ?? {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                },
                parentTaskExecution: {
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

            await tx.updateAllTaskExecutions(
              {
                type: 'by_execution_ids',
                executionIds: [execution.executionId],
                statuses: ['running'],
              },
              {
                runOutput: runOutputSerialized,
                childrenTaskExecutions: [],
                finalizeTaskExecution: {
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
          await this.storage.updateAllTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [execution.executionId],
              statuses: ['running'],
            },
            {
              runOutput: runOutputSerialized,
              output: taskInternal.disableChildrenTaskExecutionsOutputsInOutput
                ? runOutputSerialized
                : this.serializer.serialize({
                    output: runOutput,
                    childrenTaskExecutionsOutputs: [],
                  }),
              childrenTaskExecutions: [],
              unsetError: true,
              status: 'completed',
              finishedAt: now,
              updatedAt: now,
            },
          )
        }
      } else {
        const childrenTaskExecutionsStorageValues: Array<TaskExecutionStorageValue> = []
        const childrenTaskExecutions: Array<ChildTaskExecution> = []
        for (const child of childrenTasks) {
          const childTaskInternal = this.taskInternalsMap.get(child.task.id)
          if (!childTaskInternal) {
            throw new DurableExecutionNotFoundError(`Child task ${child.task.id} not found`)
          }

          const runInput = await childTaskInternal.validateInput(child.input)
          const executionId = generateTaskExecutionId()
          const retryOptions = childTaskInternal.getRetryOptions(child.options)
          const sleepMsBeforeRun = childTaskInternal.getSleepMsBeforeRun(child.options)
          const timeoutMs = childTaskInternal.getTimeoutMs(child.options)
          childrenTaskExecutionsStorageValues.push(
            createTaskExecutionStorageValue({
              now,
              rootTaskExecution: execution.rootTaskExecution ?? {
                taskId: execution.taskId,
                executionId: execution.executionId,
              },
              parentTaskExecution: {
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
          childrenTaskExecutions.push({
            taskId: child.task.id,
            executionId,
          })
        }

        await this.storage.withTransaction(async (tx) => {
          await tx.insertTaskExecutions(childrenTaskExecutionsStorageValues)

          await tx.updateAllTaskExecutions(
            {
              type: 'by_execution_ids',
              executionIds: [execution.executionId],
              statuses: ['running'],
            },
            {
              runOutput: runOutputSerialized,
              childrenTaskExecutions,
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
      let update: TaskExecutionStorageUpdate

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
          error: convertDurableExecutionErrorToStorageValue(durableExecutionError),
          status: 'ready',
          retryAttempts: execution.retryAttempts + 1,
          startAt: new Date(now.getTime() + delayMs),
          unsetExpiresAt: true,
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
          error: convertDurableExecutionErrorToStorageValue(durableExecutionError),
          status,
          finishedAt: now,
          updatedAt: now,
        }
      }

      await this.storage.updateAllTaskExecutions(
        {
          type: 'by_execution_ids',
          executionIds: [execution.executionId],
          statuses: ['running'],
        },
        update,
      )
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

  /**
   * Get executor statistics for monitoring and debugging.
   *
   * @returns The executor statistics.
   */
  getExecutorStats(): {
    expireMs: number
    backgroundProcessIntraBatchSleepMs: number
    currConcurrentTaskExecutions: number
    maxConcurrentTaskExecutions: number
    maxTasksPerBatch: number
    registeredTasksCount: number
    isShutdown: boolean
  } {
    return {
      expireMs: this.expireMs,
      backgroundProcessIntraBatchSleepMs: this.backgroundProcessIntraBatchSleepMs,
      currConcurrentTaskExecutions: this.runningTaskExecutionsMap.size,
      maxConcurrentTaskExecutions: this.maxConcurrentTaskExecutions,
      maxTasksPerBatch: this.maxTasksPerBatch,
      registeredTasksCount: this.taskInternalsMap.size,
      isShutdown: this.shutdownSignal.isCancelled(),
    }
  }
}
