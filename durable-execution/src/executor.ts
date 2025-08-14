import type { StandardSchemaV1 } from '@standard-schema/spec'
import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
import { getErrorMessage } from '@gpahal/std/errors'

import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from './errors'
import { LoggerInternal, zLogger, zLogLevel, type Logger, type LogLevel } from './logger'
import { SerializerInternal, zSerializer, type Serializer } from './serializer'
import {
  createTaskExecutionStorageValue,
  TaskExecutionsStorageInternal,
  zStorageMaxRetryAttempts,
  type TaskExecutionsStorage,
  type TaskExecutionStorageUpdateInternal,
  type TaskExecutionStorageValue,
} from './storage'
import {
  ACTIVE_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  type DefaultParentTaskOutput,
  type FinishedChildTaskExecution,
  type InferTaskInput,
  type InferTaskOutput,
  type LastTaskElementInArray,
  type ParentTaskOptions,
  type SequentialTasks,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
  type TaskExecutionSummary,
  type TaskOptions,
  type TaskRunContext,
} from './task'
import {
  generateTaskExecutionId,
  getTaskHandleInternal,
  overrideTaskEnqueueOptions,
  TaskInternal,
  validateCommonTaskOptions,
  validateEnqueueOptions,
} from './task-internal'
import {
  createCancellablePromiseCustom,
  generateId,
  sleepWithJitter,
  summarizeStandardSchemaIssues,
} from './utils'

export const zDurableExecutorOptions = z.object({
  serializer: zSerializer.nullish(),
  logger: zLogger.nullish(),
  logLevel: zLogLevel.nullish(),
  expireMs: z
    .number()
    .nullish()
    .transform((val) => val ?? 300_000),
  backgroundProcessIntraBatchSleepMs: z
    .number()
    .nullish()
    .transform((val) => val ?? 1000),
  maxConcurrentTaskExecutions: z
    .number()
    .nullish()
    .transform((val) => val ?? 1000),
  maxTaskExecutionsPerBatch: z
    .number()
    .nullish()
    .transform((val) => val ?? 3),
  maxChildrenPerTaskExecution: z
    .number()
    .int()
    .min(1)
    .max(1000)
    .nullish()
    .transform((val) => val ?? 1000),
  maxSerializedInputDataSize: z
    .number()
    .int()
    .min(0)
    .max(1024 * 1024) // 1MB
    .nullish()
    .transform((val) => val ?? 1024 * 1024), // 1MB
  maxSerializedOutputDataSize: z
    .number()
    .int()
    .min(1024) // 1KB
    .max(10 * 1024 * 1024) // 10MB
    .nullish()
    .transform((val) => val ?? 1024 * 1024), // 1MB
  storageMaxRetryAttempts: zStorageMaxRetryAttempts,
})

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
 *         children: [
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
 *     finalize: {
 *       id: 'uploadFileFinalize',
 *       timeoutMs: 60_000, // 1 minute
 *       run: async (ctx, { output, children }) => {
 *         // ... combine the output of the run function and children tasks
 *         return {
 *           filePath: output.filePath,
 *           uploadUrl: output.uploadUrl,
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
 *   const uploadFileFinishedExecution = await uploadFileHandle.waitAndGetFinishedExecution()
 *   await uploadFileHandle.cancel()
 *
 *   console.log(uploadFileExecution)
 * }
 *
 * // Start the durable executor background processes
 * executor.startBackgroundProcesses()
 *
 * // Run the app
 * await app()
 *
 * // Shutdown the durable executor when the app is done
 * await executor.shutdown()
 * ```
 *
 * @category Executor
 */
export class DurableExecutor {
  private readonly logger: LoggerInternal
  private readonly storage: TaskExecutionsStorageInternal
  private readonly serializer: SerializerInternal
  private readonly expireMs: number
  private readonly backgroundProcessIntraBatchSleepMs: number
  private readonly maxConcurrentTaskExecutions: number
  private readonly maxTaskExecutionsPerBatch: number
  private readonly maxChildrenPerTaskExecution: number
  private readonly maxSerializedInputDataSize: number
  private readonly maxSerializedOutputDataSize: number
  private readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private readonly taskInternalsMap: Map<string, TaskInternal>
  private readonly runningTaskExecutionsMap: Map<
    string,
    {
      promise: Promise<void>
      cancel: () => void
    }
  >
  private backgroundProcessesPromise: Promise<void> | undefined

  /**
   * Create a durable executor.
   *
   * @param storage - The storage to use for the durable executor.
   * @param options - The options for the durable executor.
   * @param options.serializer - The serializer to use for the durable executor. If not provided, a
   *   default serializer using superjson will be used.
   * @param options.logger - The logger to use for the durable executor. If not provided, a console
   *   logger will be used.
   * @param options.logLevel - The log level to use for the durable executor. If not provided,
   *   defaults to `info`.
   * @param options.expireMs - The duration after which a task execution is considered expired.
   *   If not provided, defaults to 300_000 (5 minutes).
   * @param options.backgroundProcessIntraBatchSleepMs - The duration to sleep between batches of
   *   background processes. If not provided, defaults to 1000 (1 second).
   * @param options.maxConcurrentTaskExecutions - The maximum number of tasks that can run concurrently.
   *   If not provided, defaults to 1000.
   * @param options.maxTaskExecutionsPerBatch - The maximum number of tasks to process in each batch.
   *   If not provided, defaults to 3.
   * @param options.maxChildrenPerTaskExecution - The maximum number of children tasks per parent task.
   *   If not provided, defaults to 1000.
   * @param options.maxSerializedInputDataSize - The maximum size of serialized input data in bytes.
   *   If not provided, defaults to 1MB.
   * @param options.maxSerializedOutputDataSize - The maximum size of serialized output data in bytes.
   *   If not provided, defaults to 1MB.
   * @param options.storageMaxRetryAttempts - The maximum number of times to retry a storage
   *   operation. If not provided, defaults to 1.
   */
  constructor(
    storage: TaskExecutionsStorage,
    options: {
      serializer?: Serializer
      logger?: Logger
      logLevel?: LogLevel
      expireMs?: number
      backgroundProcessIntraBatchSleepMs?: number
      maxConcurrentTaskExecutions?: number
      maxTaskExecutionsPerBatch?: number
      maxChildrenPerTaskExecution?: number
      maxSerializedInputDataSize?: number
      maxSerializedOutputDataSize?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    const parsedOptions = zDurableExecutorOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    const {
      serializer,
      logger,
      logLevel,
      expireMs,
      backgroundProcessIntraBatchSleepMs,
      maxConcurrentTaskExecutions,
      maxTaskExecutionsPerBatch,
      maxChildrenPerTaskExecution,
      maxSerializedInputDataSize,
      maxSerializedOutputDataSize,
      storageMaxRetryAttempts,
    } = parsedOptions.data

    this.serializer = new SerializerInternal(serializer)
    this.logger = new LoggerInternal(logger, logLevel)
    this.expireMs = expireMs
    this.backgroundProcessIntraBatchSleepMs = backgroundProcessIntraBatchSleepMs
    this.maxConcurrentTaskExecutions = maxConcurrentTaskExecutions
    this.maxTaskExecutionsPerBatch = maxTaskExecutionsPerBatch
    this.maxChildrenPerTaskExecution = maxChildrenPerTaskExecution
    this.maxSerializedInputDataSize = maxSerializedInputDataSize
    this.maxSerializedOutputDataSize = maxSerializedOutputDataSize
    this.storage = new TaskExecutionsStorageInternal(this.logger, storage, storageMaxRetryAttempts)

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.taskInternalsMap = new Map()
    this.runningTaskExecutionsMap = new Map()
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw DurableExecutionError.nonRetryable('Durable executor shutdown')
    }
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
    const validatedCommonTaskOptions = validateCommonTaskOptions(taskOptions)
    const taskInternal = TaskInternal.fromTaskOptions(this.taskInternalsMap, taskOptions)
    this.logger.debug(`Added task ${taskOptions.id}`)
    return {
      id: taskInternal.id,
      retryOptions: validatedCommonTaskOptions.retryOptions,
      sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
      timeoutMs: validatedCommonTaskOptions.timeoutMs,
    }
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
    TOutput = DefaultParentTaskOutput<TRunOutput>,
    TFinalizeTaskRunOutput = unknown,
  >(
    parentTaskOptions: ParentTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
  ): Task<TInput, TOutput> {
    this.throwIfShutdown()
    const validatedCommonTaskOptions = validateCommonTaskOptions(parentTaskOptions)
    const taskInternal = TaskInternal.fromParentTaskOptions(
      this.taskInternalsMap,
      parentTaskOptions,
    )
    this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
    return {
      id: taskInternal.id,
      retryOptions: validatedCommonTaskOptions.retryOptions,
      sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
      timeoutMs: validatedCommonTaskOptions.timeoutMs,
    }
  }

  validateInput<TRunInput, TInput>(
    validateInputFn: (input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput = unknown>(taskOptions: TaskOptions<TRunInput, TOutput>) => Task<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
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
        throw DurableExecutionError.nonRetryable(
          `Invalid input to task ${id}: ${getErrorMessage(error)}`,
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
    parentTask: <TRunOutput = unknown, TOutput = DefaultParentTaskOutput<TRunOutput>>(
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
      const validateResult = await inputSchema['~standard'].validate(input)
      if (validateResult.issues != null) {
        throw DurableExecutionError.nonRetryable(
          `Invalid input to task ${id}: ${summarizeStandardSchemaIssues(validateResult.issues)}`,
        )
      }
      return validateResult.value
    }

    return this.withValidateInputInternal(validateInputFn)
  }

  private withValidateInputInternal<TRunInput, TInput>(
    validateInputFn: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput>(taskOptions: TaskOptions<TRunInput, TOutput>) => Task<TInput, TOutput>
    parentTask: <
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    ) => Task<TInput, TOutput>
  } {
    this.throwIfShutdown()
    return {
      task: <TOutput>(taskOptions: TaskOptions<TRunInput, TOutput>): Task<TInput, TOutput> => {
        this.throwIfShutdown()
        const validatedCommonTaskOptions = validateCommonTaskOptions(taskOptions)
        const taskInternal = TaskInternal.fromTaskOptions(
          this.taskInternalsMap,
          taskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added task ${taskOptions.id}`)
        return {
          id: taskInternal.id,
          retryOptions: validatedCommonTaskOptions.retryOptions,
          sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
          timeoutMs: validatedCommonTaskOptions.timeoutMs,
        }
      },
      parentTask: <
        TRunOutput = unknown,
        TOutput = DefaultParentTaskOutput<TRunOutput>,
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
        const validatedCommonTaskOptions = validateCommonTaskOptions(parentTaskOptions)
        const taskInternal = TaskInternal.fromParentTaskOptions(
          this.taskInternalsMap,
          parentTaskOptions,
          validateInputFn,
        )
        this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
        return {
          id: taskInternal.id,
          retryOptions: validatedCommonTaskOptions.retryOptions,
          sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
          timeoutMs: validatedCommonTaskOptions.timeoutMs,
        }
      },
    }
  }

  /**
   * Create a new task that runs a sequence of tasks sequentially. If any task fails, the entire
   * sequence will be marked as failed.
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
    return this.sequentialTasksInternal(0, tasks)
  }

  private sequentialTasksInternal<T extends ReadonlyArray<Task<unknown, unknown>>>(
    index: number,
    tasks: SequentialTasks<T>,
  ): Task<InferTaskInput<T[0]>, InferTaskOutput<LastTaskElementInArray<T>>> {
    if (tasks.length === 0) {
      throw DurableExecutionError.nonRetryable('No tasks provided')
    }

    const taskId = `st_${generateId(20)}`
    const firstTask = tasks[0]
    if (tasks.length === 1) {
      return this.parentTask({
        id: taskId,
        timeoutMs: 1000,
        runParent: (_, input) => {
          return {
            output: undefined,
            children: [{ task: firstTask, input }],
          }
        },
        finalize: {
          id: `${taskId}_finalize`,
          timeoutMs: 1000,
          run: (_, { children }) => {
            const child = children[0]!
            if (child.status !== 'completed') {
              throw DurableExecutionError.nonRetryable(
                `Task ${firstTask.id} at index ${index} failed: ${child.error.message}`,
              )
            }

            return child.output
          },
        },
      })
    }

    const secondTask = this.sequentialTasksInternal(
      index + 1,
      tasks.slice(1) as SequentialTasks<ReadonlyArray<Task<unknown, unknown>>>,
    )
    return this.parentTask({
      id: taskId,
      timeoutMs: 1000,
      runParent: (_, input) => {
        return {
          output: undefined,
          children: [{ task: firstTask, input }],
        }
      },
      finalize: {
        id: `${taskId}_finalize`,
        timeoutMs: 1000,
        runParent: (_, { children }) => {
          const child = children[0]!
          if (child.status !== 'completed') {
            throw DurableExecutionError.nonRetryable(
              `Task ${firstTask.id} at index ${index} failed with error: ${child.error.message}`,
            )
          }

          return {
            output: undefined,
            children: [{ task: secondTask, input: child.output }],
          }
        },
        finalize: {
          id: `${taskId}_finalize_nested`,
          timeoutMs: 1000,
          run: (_, { children }) => {
            const child = children[0]!
            if (child.status !== 'completed') {
              throw DurableExecutionError.nonRetryable(
                `Task ${secondTask.id} at index ${index} failed with error: ${child.error.message}`,
              )
            }

            return child.output
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
          options?: TaskEnqueueOptions & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insert'>
          },
        ]
      : [
          task: TTask,
          input: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insert'>
          },
        ]
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    this.throwIfShutdown()

    const task = rest[0]
    const input = rest.length > 1 ? rest[1]! : undefined
    const options = rest.length > 2 ? rest[2]! : undefined
    if (!this.taskInternalsMap.has(task.id)) {
      throw new DurableExecutionNotFoundError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it`,
      )
    }

    const executionId = generateTaskExecutionId()
    const now = new Date()
    const validatedEnqueueOptions = validateEnqueueOptions(
      task.id,
      options
        ? {
            retryOptions: options.retryOptions,
            sleepMsBeforeRun: options.sleepMsBeforeRun,
            timeoutMs: options.timeoutMs,
          }
        : undefined,
    )
    const finalEnqueueOptions = overrideTaskEnqueueOptions(task, validatedEnqueueOptions)
    await (options?.taskExecutionsStorageTransaction ?? this.storage).insert([
      createTaskExecutionStorageValue({
        now,
        taskId: task.id,
        executionId,
        retryOptions: finalEnqueueOptions.retryOptions,
        sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
        timeoutMs: finalEnqueueOptions.timeoutMs,
        input: this.serializer.serialize(input, this.maxSerializedInputDataSize),
      }),
    ])

    this.logger.debug(`Enqueued task ${task.id} with execution id ${executionId}`)
    return getTaskHandleInternal(this.storage, this.serializer, this.logger, task.id, executionId)
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
    if (!this.taskInternalsMap.has(task.id)) {
      throw new DurableExecutionNotFoundError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it`,
      )
    }

    const execution = await this.storage.getById(executionId, {})
    if (!execution) {
      throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
    }
    if (execution.taskId !== task.id) {
      throw new DurableExecutionNotFoundError(
        `Task execution ${executionId} not found for task ${task.id} (belongs to ${execution.taskId})`,
      )
    }

    return getTaskHandleInternal(this.storage, this.serializer, this.logger, task.id, executionId)
  }

  /**
   * Start the durable executor background processes. Use {@link DurableExecutor.shutdown} to stop
   * the durable executor.
   */
  startBackgroundProcesses(): void {
    this.throwIfShutdown()
    if (!this.backgroundProcessesPromise) {
      // Fire-and-forget background processes; keep handle for shutdown awaiting.
      this.backgroundProcessesPromise = this.startBackgroundProcessesInternal().catch((error) => {
        this.logger.error('Background processes exited with error', error)
      })
    }
  }

  private async startBackgroundProcessesInternal(): Promise<void> {
    await Promise.all([
      this.processReadyTaskExecutions(),
      this.processOnChildrenFinishedTaskExecutions(),
      this.closeFinishedTaskExecutions(),
      this.cancelNeedPromiseCancellationTaskExecutions(),
      this.retryExpiredTaskExecutions(),
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
      this.cancelShutdownSignal()
    }
    this.logger.info('Durable executor cancelled. Waiting for background processes to stop')

    if (this.backgroundProcessesPromise) {
      await this.backgroundProcessesPromise
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

  protected async runBackgroundProcess(
    processName: string,
    singleBatchProcessFn: () => Promise<boolean>,
    sleepMultiplier?: number,
  ): Promise<void> {
    let consecutiveErrors = 0
    const maxConsecutiveErrors = 10
    let backoffMs = 1000

    let consecutiveEmptyResults = 0
    let backgroundProcessIntraBatchSleepMs =
      sleepMultiplier && sleepMultiplier > 0
        ? this.backgroundProcessIntraBatchSleepMs * sleepMultiplier
        : this.backgroundProcessIntraBatchSleepMs
    while (true) {
      if (this.shutdownSignal.isCancelled()) {
        this.logger.info(`Durable executor cancelled. Stopping ${processName}`)
        return
      }

      try {
        const hasNonEmptyResult = await createCancellablePromiseCustom(
          singleBatchProcessFn(),
          this.shutdownSignal,
        )

        if (hasNonEmptyResult) {
          consecutiveEmptyResults = 0
          backgroundProcessIntraBatchSleepMs = this.backgroundProcessIntraBatchSleepMs
        } else {
          consecutiveEmptyResults++
          const maxSleepMultiplier = 5
          const sleepMultiplier = Math.min(1 + consecutiveEmptyResults / 10, maxSleepMultiplier)
          backgroundProcessIntraBatchSleepMs = Math.floor(
            this.backgroundProcessIntraBatchSleepMs * sleepMultiplier,
          )
          await sleepWithJitter(backgroundProcessIntraBatchSleepMs)
        }

        consecutiveErrors = 0
        backoffMs = 1000
      } catch (error) {
        consecutiveErrors++
        this.logger.error(`Error in ${processName}: consecutive_errors=${consecutiveErrors}`, error)

        const isRetryableError = error instanceof DurableExecutionError ? error.isRetryable : true
        const waitTime = isRetryableError ? Math.min(backoffMs, 5000) : backoffMs
        if (consecutiveErrors >= maxConsecutiveErrors) {
          this.logger.error(
            `Too many consecutive errors (${consecutiveErrors}) in ${processName}. Backing off for ${backoffMs}ms before retrying`,
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
    const batchLimit = Math.min(this.maxTaskExecutionsPerBatch, availableLimit)
    const now = new Date()
    const expiresAt = new Date(now.getTime() + 60 * 1000 + this.expireMs) // 1 minute + expireMs
    const executions = await this.storage.updateByStatusAndStartAtLessThanAndReturn(
      now,
      'ready',
      now,
      { status: 'running', startedAt: now, expiresAt },
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
        await this.storage.updateById(
          now,
          execution.executionId,
          {
            statuses: ['running'],
          },
          {
            status: 'failed',
            error: convertDurableExecutionErrorToStorageValue(
              new DurableExecutionNotFoundError(`Task ${execution.taskId} not found`),
            ),
          },
        )
        return undefined
      }

      const expiresAt = new Date(now.getTime() + execution.timeoutMs + this.expireMs)
      await this.storage.updateById(
        now,
        execution.executionId,
        {
          statuses: ['running'],
        },
        { expiresAt },
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
   * It will update the execution status to `failed`, `timed_out`, `cancelled`,
   * `waiting_for_children`, `waiting_for_finalize`, `completed`, or `ready` depending on the
   * result of the task. If the task completes successfully, it will update the execution status to
   * `waiting_for_children`, `waiting_for_finalize`, or `completed`. If the task fails, it will
   * update the execution status to `failed`, `timed_out`, `cancelled`, or `ready` depending on the
   * error. If the error is retryable and the retry attempts are less than the maximum retry
   * attempts, it will update the execution status to ready. All the errors are saved in storage
   * even if the task is retried. They only get cleared if the execution is completed later.
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
      root: execution.root,
      parent: execution.parent,
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
    const existingExecution = await this.storage.getById(execution.executionId, {})
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
        this.serializer.deserialize(execution.input),
        execution.timeoutMs,
        cancelSignal,
      )
      const runOutput = result.output
      const childrenTasks = result.children

      if (childrenTasks.length > this.maxChildrenPerTaskExecution) {
        throw DurableExecutionError.nonRetryable(
          `Parent task ${taskInternal.id} cannot spawn more than ${this.maxChildrenPerTaskExecution} children tasks. Attempted: ${childrenTasks.length}`,
        )
      }

      const now = new Date()
      if (childrenTasks.length === 0) {
        if (taskInternal.finalize) {
          const finalizeTaskInternal = this.taskInternalsMap.get(taskInternal.finalize.id)
          if (!finalizeTaskInternal) {
            throw new DurableExecutionNotFoundError(
              `Finalize task ${taskInternal.finalize.id} not found`,
            )
          }

          const finalizeTaskInput = {
            output: runOutput,
            children: [],
          }
          const executionId = generateTaskExecutionId()
          await this.storage.updateByIdAndInsertIfUpdated(
            now,
            execution.executionId,
            {
              statuses: ['running'],
            },
            {
              status: 'waiting_for_finalize',
              children: [],
              finalize: {
                taskId: finalizeTaskInternal.id,
                executionId,
              },
            },
            [
              createTaskExecutionStorageValue({
                now,
                root: execution.root ?? {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                },
                parent: {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                  indexInParentChildTaskExecutions: 0,
                  isFinalizeTaskOfParentTask: true,
                },
                taskId: finalizeTaskInternal.id,
                executionId,
                retryOptions: finalizeTaskInternal.retryOptions,
                sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
                timeoutMs: finalizeTaskInternal.timeoutMs,
                input: this.serializer.serialize(
                  finalizeTaskInput,
                  this.maxSerializedInputDataSize,
                ),
              }),
            ],
          )
        } else {
          await this.storage.updateById(
            now,
            execution.executionId,
            {
              statuses: ['running'],
            },
            {
              status: 'completed',
              output: taskInternal.isParentTask
                ? this.serializer.serialize(
                    {
                      output: runOutput,
                      children: [],
                    },
                    this.maxSerializedOutputDataSize,
                  )
                : this.serializer.serialize(runOutput, this.maxSerializedOutputDataSize),
              children: [],
            },
          )
        }
      } else {
        const childrenTaskExecutionsStorageValues: Array<TaskExecutionStorageValue> = []
        const childrenTaskExecutions: Array<TaskExecutionSummary> = []
        for (const [index, child] of childrenTasks.entries()) {
          const childTaskInternal = this.taskInternalsMap.get(child.task.id)
          if (!childTaskInternal) {
            throw new DurableExecutionNotFoundError(`Child task ${child.task.id} not found`)
          }

          const executionId = generateTaskExecutionId()
          const finalEnqueueOptions = overrideTaskEnqueueOptions(childTaskInternal, child.options)
          childrenTaskExecutionsStorageValues.push(
            createTaskExecutionStorageValue({
              now,
              root: execution.root ?? {
                taskId: execution.taskId,
                executionId: execution.executionId,
              },
              parent: {
                taskId: execution.taskId,
                executionId: execution.executionId,
                indexInParentChildTaskExecutions: index,
                isFinalizeTaskOfParentTask: false,
              },
              taskId: child.task.id,
              executionId,
              retryOptions: finalEnqueueOptions.retryOptions,
              sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
              timeoutMs: finalEnqueueOptions.timeoutMs,
              input: this.serializer.serialize(child.input, this.maxSerializedInputDataSize),
            }),
          )
          childrenTaskExecutions.push({
            taskId: child.task.id,
            executionId,
          })
        }

        await this.storage.updateByIdAndInsertIfUpdated(
          now,
          execution.executionId,
          {
            statuses: ['running'],
          },
          {
            status: 'waiting_for_children',
            runOutput: this.serializer.serialize(runOutput, this.maxSerializedOutputDataSize),
            children: childrenTaskExecutions,
            activeChildrenCount: childrenTaskExecutions.length,
          },
          childrenTaskExecutionsStorageValues,
        )
      }
    } catch (error) {
      const durableExecutionError =
        error instanceof DurableExecutionError
          ? error
          : DurableExecutionError.retryable(getErrorMessage(error))

      const now = new Date()
      let update: TaskExecutionStorageUpdateInternal

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
          status: 'ready',
          error: convertDurableExecutionErrorToStorageValue(durableExecutionError),
          retryAttempts: execution.retryAttempts + 1,
          startAt: new Date(now.getTime() + delayMs),
        }
      } else {
        const status =
          error instanceof DurableExecutionTimedOutError
            ? 'timed_out'
            : error instanceof DurableExecutionCancelledError
              ? 'cancelled'
              : 'failed'
        update = {
          status,
          error: convertDurableExecutionErrorToStorageValue(durableExecutionError),
        }
      }

      await this.storage.updateById(
        now,
        execution.executionId,
        {
          statuses: ['running'],
        },
        update,
      )
    }
  }

  private async processOnChildrenFinishedTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('processing on children finished task executions', () =>
      this.processOnChildrenFinishedTaskExecutionsSingleBatch(),
    )
  }

  /**
   * Process on children finished task executions.
   */
  private async processOnChildrenFinishedTaskExecutionsSingleBatch(): Promise<boolean> {
    const now = new Date()
    const expiresAt = new Date(now.getTime() + 60 * 1000 + this.expireMs) // 1 minute + expireMs
    const executions =
      await this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountLessThanAndReturn(
        now,
        'waiting_for_children',
        'idle',
        1,
        {
          onChildrenFinishedProcessingStatus: 'processing',
          onChildrenFinishedProcessingExpiresAt: expiresAt,
        },
        3,
      )

    this.logger.debug(`Processing ${executions.length} on children finished task executions`)
    if (executions.length === 0) {
      return false
    }

    const processExecution = async (execution: TaskExecutionStorageValue): Promise<void> => {
      const taskInternal = this.taskInternalsMap.get(execution.taskId)
      if (!taskInternal) {
        const now = new Date()
        await this.storage.updateById(
          now,
          execution.executionId,
          {
            statuses: ['waiting_for_children'],
          },
          {
            status: 'failed',
            error: convertDurableExecutionErrorToStorageValue(
              new DurableExecutionNotFoundError(`Task ${execution.taskId} not found`),
            ),
          },
        )
        return
      }

      const childrenTaskExecutions = execution.children
        ? ((await this.storage.getByIds(
            execution.children.map((child) => child.executionId),
          )) as Array<TaskExecutionStorageValue>)
        : []
      if (
        execution.children &&
        (childrenTaskExecutions.length !== execution.children.length ||
          childrenTaskExecutions.some((child) => child == null))
      ) {
        const finishedChildrenCount = childrenTaskExecutions.filter((child) => child != null).length
        const now = new Date()
        await this.storage.updateById(
          now,
          execution.executionId,
          {
            statuses: ['waiting_for_children'],
          },
          {
            status: 'failed',
            error: convertDurableExecutionErrorToStorageValue(
              DurableExecutionError.nonRetryable(
                `Some children task executions not found: ${execution.children.length - finishedChildrenCount}/${execution.children.length} not found`,
                true,
              ),
            ),
          },
        )
        return
      }

      const childrenTaskExecutionsForFinalize = childrenTaskExecutions.map(
        (child) =>
          ({
            taskId: child.taskId,
            executionId: child.executionId,
            status: child.status,
            output:
              child.status === 'completed' && child.output
                ? this.serializer.deserialize(child.output)
                : undefined,
            error:
              child.status !== 'completed'
                ? child.error ||
                  convertDurableExecutionErrorToStorageValue(
                    DurableExecutionError.nonRetryable(`Unknown error`),
                  )
                : undefined,
          }) as FinishedChildTaskExecution,
      )
      const finalizeTaskInternal = taskInternal.finalize
      if (!finalizeTaskInternal) {
        const now = new Date()
        await this.storage.updateById(
          now,
          execution.executionId,
          {
            statuses: ['waiting_for_children'],
          },
          {
            status: 'completed',
            output: taskInternal.isParentTask
              ? this.serializer.serialize(
                  {
                    output: execution.runOutput
                      ? this.serializer.deserialize(execution.runOutput)
                      : undefined,
                    children: childrenTaskExecutionsForFinalize,
                  },
                  this.maxSerializedOutputDataSize,
                )
              : this.serializer.deserialize(execution.runOutput!),
          },
        )
        return
      }

      const finalizeTaskInput = {
        output: execution.runOutput ? this.serializer.deserialize(execution.runOutput) : undefined,
        children: childrenTaskExecutionsForFinalize,
      }
      const executionId = generateTaskExecutionId()
      const now = new Date()
      await this.storage.updateByIdAndInsertIfUpdated(
        now,
        execution.executionId,
        {
          statuses: ['waiting_for_children'],
        },
        {
          status: 'waiting_for_finalize',
          finalize: {
            taskId: finalizeTaskInternal.id,
            executionId,
          },
        },
        [
          createTaskExecutionStorageValue({
            now,
            root: execution.root,
            parent: {
              taskId: execution.taskId,
              executionId: execution.executionId,
              indexInParentChildTaskExecutions: 0,
              isFinalizeTaskOfParentTask: true,
            },
            taskId: finalizeTaskInternal.id,
            executionId,
            retryOptions: finalizeTaskInternal.retryOptions,
            sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
            timeoutMs: finalizeTaskInternal.timeoutMs,
            input: this.serializer.serialize(finalizeTaskInput, this.maxSerializedInputDataSize),
          }),
        ],
      )
    }

    await Promise.all(executions.map((execution) => processExecution(execution)))

    await this.storage.updateByIdsAndStatuses(
      now,
      executions.map((execution) => execution.executionId),
      ['completed', 'cancelled'],
      {
        onChildrenFinishedProcessingStatus: 'processed',
        onChildrenFinishedProcessingFinishedAt: now,
      },
    )
    return true
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
    let now = new Date()
    const expiresAt = new Date(now.getTime() + 60 * 1000 + this.expireMs) // 1 minute + expireMs
    const executions = await this.storage.updateByStatusesAndCloseStatusAndReturn(
      now,
      FINISHED_TASK_EXECUTION_STATUSES,
      'idle',
      {
        closeStatus: 'closing',
        closeExpiresAt: expiresAt,
      },
      3,
    )

    this.logger.debug(`Closing ${executions.length} finished task executions`)
    if (executions.length === 0) {
      return false
    }

    const processExecution = async (execution: TaskExecutionStorageValue): Promise<void> => {
      execution.closeStatus = 'closing'
      await Promise.all([
        this.closeFinishedTaskExecutionParent(execution),
        this.closeFinishedTaskExecutionChildren(execution),
      ])
    }

    await Promise.all(executions.map(processExecution))

    now = new Date()
    await this.storage.updateByIdsAndStatuses(
      now,
      executions.map((execution) => execution.executionId),
      FINISHED_TASK_EXECUTION_STATUSES,
      {
        closeStatus: 'closed',
        closedAt: now,
      },
    )
    return true
  }

  /**
   * Close finished task execution parent.
   */
  private async closeFinishedTaskExecutionParent(
    execution: TaskExecutionStorageValue,
  ): Promise<void> {
    if (!execution.parent?.isFinalizeTaskOfParentTask) {
      return
    }

    const now = new Date()
    await (execution.status === 'completed'
      ? this.storage.updateById(
          now,
          execution.parent.executionId,
          {
            statuses: ['waiting_for_finalize'],
          },
          {
            status: 'completed',
            output: execution.output!,
          },
        )
      : this.storage.updateById(
          now,
          execution.parent.executionId,
          {
            statuses: ['waiting_for_finalize'],
          },
          {
            status: 'finalize_failed',
            error:
              execution.error ||
              convertDurableExecutionErrorToStorageValue(
                DurableExecutionError.nonRetryable(
                  `Finalize task ${execution.taskId} failed with an unknown error`,
                ),
              ),
          },
        ))
  }

  /**
   * Close finished task execution children and cancel running children task executions.
   */
  private async closeFinishedTaskExecutionChildren(
    execution: TaskExecutionStorageValue,
  ): Promise<void> {
    if (execution.status === 'completed') {
      return
    }

    const childrenExecutionIds: Array<string> = []
    if (execution.children && execution.children.length > 0) {
      for (const child of execution.children) {
        childrenExecutionIds.push(child.executionId)
      }
    }
    if (execution.finalize) {
      childrenExecutionIds.push(execution.finalize.executionId)
    }
    if (childrenExecutionIds.length === 0) {
      return
    }

    const now = new Date()
    await this.storage.updateByIdsAndStatuses(
      now,
      childrenExecutionIds,
      ACTIVE_TASK_EXECUTION_STATUSES,
      {
        status: 'cancelled',
        error: convertDurableExecutionErrorToStorageValue(
          new DurableExecutionCancelledError(
            `Parent task ${execution.taskId} with execution id ${execution.executionId} failed: ${execution.error?.message || 'Unknown error'}`,
          ),
        ),
        needsPromiseCancellation: true,
      },
    )
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

    const executions = await this.storage.getByNeedsPromiseCancellationAndIds(
      true,
      [...this.runningTaskExecutionsMap.keys()],
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
    await this.storage.updateByNeedsPromiseCancellationAndIds(
      now,
      true,
      executions.map((execution) => execution.executionId),
      {
        needsPromiseCancellation: false,
      },
    )
    return true
  }

  private async retryExpiredTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess(
      'retrying expired task executions',
      async () => {
        await this.retryExpiredTaskExecutionsSingleBatch()
        return false
      },
      5,
    )
  }

  private async retryExpiredTaskExecutionsSingleBatch(): Promise<void> {
    let now = new Date()
    let executions = await this.storage.updateByExpiresAtLessThanAndReturn(
      now,
      now,
      {
        status: 'ready',
        error: convertDurableExecutionErrorToStorageValue(
          DurableExecutionError.nonRetryable('Task execution expired'),
        ),
        startAt: now,
      },
      10,
    )

    this.logger.debug(`Retrying ${executions.length} expired running task executions`)

    now = new Date()
    executions = await this.storage.updateByOnChildrenFinishedProcessingExpiresAtLessThanAndReturn(
      now,
      now,
      {
        onChildrenFinishedProcessingStatus: 'idle',
      },
      10,
    )

    this.logger.debug(
      `Retrying ${executions.length} expired on children finished processing task executions`,
    )

    now = new Date()
    executions = await this.storage.updateByCloseExpiresAtLessThanAndReturn(
      now,
      now,
      {
        closeStatus: 'idle',
      },
      10,
    )

    this.logger.debug(`Retrying ${executions.length} expired close task executions`)
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
    maxTaskExecutionsPerBatch: number
    maxChildrenPerTaskExecution: number
    maxSerializedInputDataSize: number
    maxSerializedOutputDataSize: number
    registeredTasksCount: number
    isShutdown: boolean
  } {
    return {
      expireMs: this.expireMs,
      backgroundProcessIntraBatchSleepMs: this.backgroundProcessIntraBatchSleepMs,
      currConcurrentTaskExecutions: this.runningTaskExecutionsMap.size,
      maxConcurrentTaskExecutions: this.maxConcurrentTaskExecutions,
      maxTaskExecutionsPerBatch: this.maxTaskExecutionsPerBatch,
      maxChildrenPerTaskExecution: this.maxChildrenPerTaskExecution,
      maxSerializedInputDataSize: this.maxSerializedInputDataSize,
      maxSerializedOutputDataSize: this.maxSerializedOutputDataSize,
      registeredTasksCount: this.taskInternalsMap.size,
      isShutdown: this.shutdownSignal.isCancelled(),
    }
  }
}
