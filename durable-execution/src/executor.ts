import type { StandardSchemaV1 } from '@standard-schema/spec'
import z from 'zod'

import { sleepWithJitter } from '@gpahal/std/promises'

import { DurableExecutionCancelledError, DurableExecutionError } from './errors'
import { DurableExecutorCore } from './executor-core'
import type { Logger, LoggerInternal, LogLevel } from './logger'
import { type Serializer } from './serializer'
import type { TaskExecutionsStorage } from './storage'
import {
  type AnyTask,
  type DefaultParentTaskOutput,
  type FinishedTaskExecution,
  type InferTaskInput,
  type InferTaskOutput,
  type LastTaskElementInArray,
  type ParentTaskOptions,
  type SequentialTasks,
  type SleepingTaskOptions,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
  type TaskOptions,
  type WakeupSleepingTaskExecutionOptions,
} from './task'

export const zDurableExecutorOptions = z.object({
  backgroundProcessIntraBatchSleepMs: z
    .number()
    .nullish()
    .transform((val) => val ?? 500),
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
 *   .inputSchema(z.object({ filePath: z.string() }))
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
 *   .inputSchema(z.object({ filePath: z.string(), uploadUrl: z.string() }))
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
 *           new ChildTask(extractFileTitle, { filePath: input.filePath }),
 *           new ChildTask(summarizeFile, { filePath: input.filePath }),
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
  private readonly core: DurableExecutorCore

  readonly id: string
  private readonly logger: LoggerInternal
  private readonly backgroundProcessIntraBatchSleepMs: number

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
   * @param options.expireLeewayMs - The expiration leeway duration after which a task execution is
   *   considered expired. If not provided, defaults to 300_000 (5 minutes).
   * @param options.backgroundProcessIntraBatchSleepMs - The duration to sleep between batches of
   *   background processes. If not provided, defaults to 500 (500ms).
   * @param options.maxConcurrentTaskExecutions - The maximum number of tasks that can run concurrently.
   *   If not provided, defaults to 5000.
   * @param options.maxTaskExecutionsPerBatch - The maximum number of tasks to process in each batch.
   *   If not provided, defaults to 100.
   * @param options.processOnChildrenFinishedTaskExecutionsBatchSize - The maximum number of on
   *   children finished task executions to process in each batch. If not provided, defaults to 100.
   * @param options.markFinishedTaskExecutionsAsCloseStatusReadyBatchSize - The maximum number of
   *   finished task executions to mark as close status ready in each batch. If not provided,
   *   defaults to 100.
   * @param options.closeFinishedTaskExecutionsBatchSize - The maximum number of finished task
   *   executions to close in each batch. If not provided, defaults to 100.
   * @param options.cancelNeedsPromiseCancellationTaskExecutionsBatchSize - The maximum number of
   *   needs promise cancellation task executions to cancel in each batch. If not provided, defaults
   *   to 100.
   * @param options.retryExpiredTaskExecutionsBatchSize - The maximum number of expired task
   *   executions to retry in each batch. If not provided, defaults to 100.
   * @param options.maxChildrenPerTaskExecution - The maximum number of children tasks per parent task.
   *   If not provided, defaults to 1000.
   * @param options.maxSerializedInputDataSize - The maximum size of serialized input data in bytes.
   *   If not provided, defaults to 1MB.
   * @param options.enableStorageBatching - Whether to enable storage batching. If not provided,
   *   defaults to false.
   * @param options.storageBatchingBackgroundProcessIntraBatchSleepMs - The sleep duration between
   *   batches of storage operations. Only applicable if storage batching is enabled. If not
   *   provided, defaults to 10ms.
   * @param options.storageMaxRetryAttempts - The maximum number of times to retry a storage
   *   operation. If not provided, defaults to 1.
   */
  constructor(
    storage: TaskExecutionsStorage,
    {
      backgroundProcessIntraBatchSleepMs,
      ...coreOptions
    }: {
      serializer?: Serializer
      logger?: Logger
      logLevel?: LogLevel
      expireLeewayMs?: number
      backgroundProcessIntraBatchSleepMs?: number
      maxConcurrentTaskExecutions?: number
      maxTaskExecutionsPerBatch?: number
      processOnChildrenFinishedTaskExecutionsBatchSize?: number
      markFinishedTaskExecutionsAsCloseStatusReadyBatchSize?: number
      closeFinishedTaskExecutionsBatchSize?: number
      cancelNeedsPromiseCancellationTaskExecutionsBatchSize?: number
      retryExpiredTaskExecutionsBatchSize?: number
      maxChildrenPerTaskExecution?: number
      maxSerializedInputDataSize?: number
      maxSerializedOutputDataSize?: number
      enableStorageBatching?: boolean
      storageBatchingBackgroundProcessIntraBatchSleepMs?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    const parsedOptions = zDurableExecutorOptions.safeParse({ backgroundProcessIntraBatchSleepMs })
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    this.core = new DurableExecutorCore(storage, coreOptions)
    this.id = this.core.id
    this.logger = this.core.logger
    this.backgroundProcessIntraBatchSleepMs = parsedOptions.data.backgroundProcessIntraBatchSleepMs
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
  ): Task<TInput, TOutput, false> {
    return this.core.task(taskOptions)
  }

  /**
   * Add a sleeping task to the durable executor. See the
   * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) section
   * for more details on creating sleeping tasks.
   *
   * @param taskOptions - The task options. See {@link SleepingTaskOptions} for more details on the
   *   task options.
   * @returns The sleeping task.
   */
  sleepingTask<TOutput = unknown>(
    taskOptions: SleepingTaskOptions<TOutput>,
  ): Task<string, TOutput, true> {
    return this.core.sleepingTask(taskOptions)
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
  ): Task<TInput, TOutput, false> {
    return this.core.parentTask(parentTaskOptions)
  }

  /**
   * Add a validate input function to the durable executor.
   *
   * @param validateInputFn - The validate input function.
   * @returns The validate input function.
   */
  validateInput<TRunInput, TInput>(
    validateInputFn: (input: TInput) => TRunInput | Promise<TRunInput>,
  ): {
    task: <TOutput = unknown>(
      taskOptions: TaskOptions<TRunInput, TOutput>,
    ) => Task<TInput, TOutput, false>
    parentTask: <
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    ) => Task<TInput, TOutput, false>
  } {
    return this.core.validateInput(validateInputFn)
  }

  /**
   * Add an input schema to the durable executor.
   *
   * @param inputSchema - The input schema.
   * @returns The input schema.
   */
  inputSchema<TInputSchema extends StandardSchemaV1>(
    inputSchema: TInputSchema,
  ): {
    task: <TOutput = unknown>(
      taskOptions: TaskOptions<StandardSchemaV1.InferOutput<TInputSchema>, TOutput>,
    ) => Task<StandardSchemaV1.InferInput<TInputSchema>, TOutput, false>
    parentTask: <TRunOutput = unknown, TOutput = DefaultParentTaskOutput<TRunOutput>>(
      parentTaskOptions: ParentTaskOptions<
        StandardSchemaV1.InferOutput<TInputSchema>,
        TRunOutput,
        TOutput
      >,
    ) => Task<StandardSchemaV1.InferInput<TInputSchema>, TOutput, false>
  } {
    return this.core.inputSchema(inputSchema)
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
  sequentialTasks<TSequentialTasks extends ReadonlyArray<AnyTask>>(
    id: string,
    tasks: SequentialTasks<TSequentialTasks>,
  ): Task<
    InferTaskInput<TSequentialTasks[0]>,
    InferTaskOutput<LastTaskElementInArray<TSequentialTasks>>
  > {
    return this.core.sequentialTasks(id, tasks)
  }

  /**
   * Create a new task that runs a task until it returns `{ isDone: true, output: TOutput }`. If the
   * task doesn't return `{ isDone: true, output: TOutput }` within the max attempts, the polling task
   * will return `{ isSuccess: false }`. Any error from the task will be considered a failure of the
   * polling task.
   *
   * @param id - The id of the polling task.
   * @param pollTask - The task to run.
   * @param maxAttempts - The maximum number of attempts to run the task.
   * @param sleepMsBeforeRun - The sleep time before running the task. If a function is provided, it
   *   will be called with the attempt number. The initial attempt is 0, then 1, then 2, etc.
   * @returns The polling task.
   */
  pollingTask<TInput, TOutput>(
    id: string,
    pollTask: Task<TInput, { isDone: false } | { isDone: true; output: TOutput }>,
    maxAttempts: number,
    sleepMsBeforeRun?: number | ((attempt: number) => number),
  ): Task<TInput, { isSuccess: false } | { isSuccess: true; output: TOutput }> {
    return this.core.pollingTask(id, pollTask, maxAttempts, sleepMsBeforeRun)
  }

  /**
   * Enqueue a task for execution.
   *
   * @param rest - The task to enqueue, input, and options.
   * @returns A handle to the task execution.
   */
  async enqueueTask<TTask extends AnyTask>(
    ...rest: undefined extends InferTaskInput<TTask>
      ? [
          task: TTask,
          input?: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions<TTask> & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
          },
        ]
      : [
          task: TTask,
          input: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions<TTask> & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
          },
        ]
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    return await this.core.enqueueTask(...rest)
  }

  /**
   * Get a handle to a task execution.
   *
   * @param task - The task to get the handle for.
   * @param executionId - The id of the execution to get the handle for.
   * @returns The handle to the task execution.
   */
  async getTaskExecutionHandle<TTask extends AnyTask>(
    task: TTask,
    executionId: string,
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    return await this.core.getTaskExecutionHandle(task, executionId)
  }

  /**
   * Wake up a sleeping task execution. The `sleepingTaskUniqueId` is the input passed to the
   * sleeping task when it was enqueued.
   *
   * @param task - The task to wake up.
   * @param sleepingTaskUniqueId - The unique id of the sleeping task to wake up.
   * @param options - The options to wake up the sleeping task execution.
   * @returns The finished task execution.
   */
  async wakeupSleepingTaskExecution<TTask extends Task<unknown, unknown, true>>(
    task: TTask,
    sleepingTaskUniqueId: string,
    options: WakeupSleepingTaskExecutionOptions<InferTaskOutput<TTask>>,
  ): Promise<FinishedTaskExecution<InferTaskOutput<TTask>>> {
    return await this.core.wakeupSleepingTaskExecution(task, sleepingTaskUniqueId, options)
  }

  /**
   * Start the durable executor background processes. Use {@link DurableExecutor.shutdown} to stop
   * the durable executor.
   */
  startBackgroundProcesses(): void {
    this.core.startBackgroundProcesses(() => [
      this.startBackgroundProcessesInternal().catch((error) => {
        this.logger.error('Background processes exited with error', error)
      }),
    ])
  }

  private async startBackgroundProcessesInternal(): Promise<void> {
    await Promise.all([
      this.processReadyTaskExecutions(),
      this.processOnChildrenFinishedTaskExecutions(),
      this.markFinishedTaskExecutionsAsCloseStatusReady(),
      this.closeFinishedTaskExecutions(),
      this.cancelNeedsPromiseCancellationTaskExecutions(),
      this.retryExpiredTaskExecutions(),
    ])
  }

  /**
   * Shutdown the durable executor. Cancels all active executions and stops the background
   * processes and promises.
   *
   * On shutdown, these happen in this order:
   * - Stop enqueuing new tasks
   * - Stop background processes and promises after the current iteration
   * - Wait for active task executions to finish. Task execution context contains a shutdown signal
   *   that can be used to gracefully shutdown the task when executor is shutting down.
   */
  async shutdown(): Promise<void> {
    await this.core.shutdown()
  }

  protected async runBackgroundProcess(
    processName: string,
    singleBatchProcessFn: () => Promise<boolean>,
    sleepMultiplier?: number,
  ): Promise<void> {
    let consecutiveErrors = 0
    const maxConsecutiveErrors = 10

    const originalBackgroundProcessIntraBatchSleepMs =
      sleepMultiplier && sleepMultiplier > 0
        ? this.backgroundProcessIntraBatchSleepMs * sleepMultiplier
        : this.backgroundProcessIntraBatchSleepMs
    let backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs

    const runBackgroundProcessSingleBatch = async (): Promise<boolean> => {
      try {
        const isDone = await singleBatchProcessFn()

        consecutiveErrors = 0
        backgroundProcessIntraBatchSleepMs = isDone
          ? Math.min(
              backgroundProcessIntraBatchSleepMs * 1.125,
              originalBackgroundProcessIntraBatchSleepMs * 2.5,
            )
          : originalBackgroundProcessIntraBatchSleepMs
        return isDone
      } catch (error) {
        if (
          error instanceof DurableExecutionCancelledError &&
          this.core.shutdownSignal.isCancelled()
        ) {
          return false
        }

        consecutiveErrors++
        this.logger.error(`Error in ${processName}: consecutive_errors=${consecutiveErrors}`, error)

        if (consecutiveErrors >= maxConsecutiveErrors) {
          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.25,
            originalBackgroundProcessIntraBatchSleepMs * 5,
          )
        }
        return true
      }
    }

    await sleepWithJitter(this.backgroundProcessIntraBatchSleepMs)
    while (!this.core.shutdownSignal.isCancelled()) {
      const isDone = await runBackgroundProcessSingleBatch()
      await sleepWithJitter(isDone ? backgroundProcessIntraBatchSleepMs : 0)
    }
  }

  private async processReadyTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('processing ready task executions', () =>
      this.core.processReadyTaskExecutionsSingleBatch(),
    )
  }

  private async processOnChildrenFinishedTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('processing on children finished task executions', () =>
      this.core.processOnChildrenFinishedTaskExecutionsSingleBatch(),
    )
  }

  private async markFinishedTaskExecutionsAsCloseStatusReady(): Promise<void> {
    return this.runBackgroundProcess('marking finished task executions as close status ready', () =>
      this.core.markFinishedTaskExecutionsAsCloseStatusReadySingleBatch(),
    )
  }

  private async closeFinishedTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess('closing finished task executions', () =>
      this.core.closeFinishedTaskExecutionsSingleBatch(),
    )
  }

  private async cancelNeedsPromiseCancellationTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess(
      'cancelling needs promise cancellation task executions',
      () => this.core.cancelsNeedPromiseCancellationTaskExecutionsSingleBatch(),
      2,
    )
  }

  private async retryExpiredTaskExecutions(): Promise<void> {
    return this.runBackgroundProcess(
      'retrying expired task executions',
      () => this.core.retryExpiredTaskExecutionsSingleBatch(),
      10,
    )
  }

  /**
   * Get the running task execution ids.
   *
   * @returns The running task execution ids.
   */
  getRunningTaskExecutionIds(): ReadonlySet<string> {
    return this.core.getRunningTaskExecutionIds()
  }

  /**
   * Get executor statistics for monitoring and debugging.
   *
   * @returns The executor statistics.
   */
  getExecutorStats(): {
    expireLeewayMs: number
    currConcurrentTaskExecutions: number
    maxConcurrentTaskExecutions: number
    maxTaskExecutionsPerBatch: number
    maxChildrenPerTaskExecution: number
    maxSerializedInputDataSize: number
    maxSerializedOutputDataSize: number
    registeredTasksCount: number
    isShutdown: boolean
  } {
    return this.core.getExecutorStats()
  }

  /**
   * Get timing stats for monitoring and debugging.
   *
   * @returns The timing stats.
   */
  getStorageTimingStats(): Record<
    string,
    {
      count: number
      meanMs: number
    }
  > {
    return this.core.getStorageTimingStats()
  }

  /**
   * Get per second storage call counts for monitoring and debugging.
   *
   * @returns The per second storage call counts.
   */
  getStoragePerSecondCallCounts(): Map<number, number> {
    return this.core.getStoragePerSecondCallCounts()
  }
}
