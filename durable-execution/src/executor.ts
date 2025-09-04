import { Effect, Exit, Runtime, Scope } from 'effect'

import {
  makeEffectDurableExecutor,
  type AnySchema,
  type EffectDurableExecutor,
  type EffectDurableExecutorOptions,
  type InferSchemaInput,
  type InferSchemaOutput,
} from './effect-executor'
import { convertCauseToDurableExecutionError, DurableExecutionCancelledError } from './errors'
import { SerializerService, type Serializer } from './serializer'
import { TaskExecutionsStorageService, type TaskExecutionsStorage } from './storage'
import {
  type AnyTask,
  type DefaultParentTaskOutput,
  type InferTaskInput,
  type InferTaskOutput,
  type ParentTaskOptions,
  type SequentialTasks,
  type SleepingTaskOptions,
  type Task,
  type TaskEnqueueOptions,
  type TaskOptions,
  type WakeupSleepingTaskExecutionOptions,
} from './task'

/**
 * The options for the durable executor.
 *
 * @category Executor
 */
export type DurableExecutorOptions = EffectDurableExecutorOptions & {
  serializer?: Serializer
}

/**
 * A durable executor. It is used to execute tasks durably, reliably and resiliently.
 *
 * Multiple durable executors can share the same storage. In such a case, all the tasks should be
 * present for all the durable executors. The work is distributed among the durable executors. See
 * the [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
 *
 * If using effect, use the `makeEffectDurableExecutor` function to create the executor with first
 * class support for effect. See
 * [makeEffectDurableExecutor](https://gpahal.github.io/durable-execution/variables/makeEffectDurableExecutor.html)
 * for more details.
 *
 * @example
 * ```ts
 * import { childTask, DurableExecutor } from 'durable-execution'
 * import { Schema } from 'effect'
 *
 * const executor = await DurableExecutor.make(storage)
 *
 * // Create tasks
 * const extractFileTitle = executor
 *   .inputSchema(Schema.Struct({ filePath: Schema.String }))
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
 *     // Example validation function - implement your own validation logic
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
 *   .inputSchema(Schema.Struct({ filePath: Schema.String, uploadUrl: Schema.String }))
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
 *           childTask(extractFileTitle, { filePath: input.filePath }),
 *           childTask(summarizeFile, { filePath: input.filePath }),
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
 *   const uploadFileHandle = await executor.enqueueTask(uploadFile, {
 *     filePath: 'file.txt',
 *     uploadUrl: 'https://example.com/upload',
 *   })
 *   const uploadFileExecution = await uploadFileHandle.getExecution()
 *   const uploadFileFinishedExecution = await uploadFileHandle.waitAndGetFinishedExecution()
 *   await uploadFileHandle.cancel()
 *
 *   console.log(uploadFileExecution)
 * }
 *
 * // Start the durable executor
 * await executor.start()
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
  readonly id: string
  private readonly effectExecutor: EffectDurableExecutor
  private readonly scope: Scope.CloseableScope
  private readonly runtime: Runtime.Runtime<never>

  private constructor(
    effectExecutor: EffectDurableExecutor,
    scope: Scope.CloseableScope,
    runtime: Runtime.Runtime<never>,
  ) {
    this.id = effectExecutor.id
    this.effectExecutor = effectExecutor
    this.scope = scope
    this.runtime = runtime
  }

  /**
   * Make a durable executor.
   *
   * @param storage - The storage to use for the durable executor.
   * @param options - The options for the durable executor.
   *
   * @remarks
   * **Options available:**
   *
   * - serializer - The serializer to use for the durable executor. If not provided, a default
   *   serializer using superjson will be used.
   * - logLevel - The log level to use for the durable executor. If not provided, defaults to
   *   `info`.
   * - expireLeewayMs - The expiration leeway duration after which a task execution is considered
   *   expired. If not provided, defaults to 300_000 (5 minutes).
   * - backgroundProcessIntraBatchSleepMs - The duration to sleep between batches of background
   *   processes. If not provided, defaults to 500 (500ms).
   * - maxConcurrentTaskExecutions - The maximum number of tasks that can run concurrently. If not
   *   provided, defaults to 5000.
   * - maxTaskExecutionsPerBatch - The maximum number of tasks to process in each batch. If not
   *   provided, defaults to 100.
   * - processOnChildrenFinishedTaskExecutionsBatchSize - The maximum number of on children
   *   finished task executions to process in each batch. If not provided, defaults to 100.
   * - markFinishedTaskExecutionsAsCloseStatusReadyBatchSize - The maximum number of finished task
   *   executions to mark as close status ready in each batch. If not provided, defaults to 100.
   * - closeFinishedTaskExecutionsBatchSize - The maximum number of finished task executions to
   *   close in each batch. If not provided, defaults to 100.
   * - cancelNeedsPromiseCancellationTaskExecutionsBatchSize - The maximum number of needs promise
   *   cancellation task executions to cancel in each batch. If not provided, defaults to 100.
   * - retryExpiredTaskExecutionsBatchSize - The maximum number of expired task executions to retry
   *   in each batch. If not provided, defaults to 100.
   * - maxChildrenPerTaskExecution - The maximum number of children tasks per parent task. If not
   *   provided, defaults to 1000.
   * - maxSerializedInputDataSize - The maximum size of serialized input data in bytes. If not
   *   provided, defaults to 1MB.
   * - maxSerializedOutputDataSize - The maximum size of serialized output data in bytes. If not
   *   provided, defaults to 1MB.
   * - enableStorageBatching - Whether to enable storage batching. If not provided, defaults to
   *   false.
   * - enableStorageStats - Whether to enable storage stats. If not provided, defaults to false.
   * - storageBackgroundBatchingProcessIntraBatchSleepMs - The sleep duration between batches of
   *   storage operations. Only applicable if storage batching is enabled. If not provided, defaults
   *   to 10ms.
   * - storageMaxRetryAttempts - The maximum number of times to retry a storage operation. If not
   *   provided, defaults to 1.
   */
  static async make(storage: TaskExecutionsStorage, options: DurableExecutorOptions = {}) {
    const { serializer, ...otherOptions } = options
    const { effectExecutor, scope, runtime } = await Effect.runPromise(
      Effect.gen(function* () {
        const scope = yield* Scope.make()

        let effect = makeEffectDurableExecutor(otherOptions).pipe(
          Effect.provideService(TaskExecutionsStorageService, storage),
        )
        if (serializer) {
          effect = effect.pipe(Effect.provideService(SerializerService, serializer))
        }
        const effectExecutor = yield* effect.pipe(Effect.provideService(Scope.Scope, scope))
        const runtime = yield* Effect.runtime<never>()
        return { effectExecutor, scope, runtime }
      }),
    )

    return new DurableExecutor(effectExecutor, scope, runtime)
  }

  private runSync<T>(effect: Effect.Effect<T, unknown, never>) {
    const exit = Runtime.runSyncExit(this.runtime, effect)
    if (Exit.isFailure(exit)) {
      throw convertCauseToDurableExecutionError(exit.cause)
    }
    return exit.value
  }

  private async runPromise<T>(effect: Effect.Effect<T, unknown, never>) {
    const exit = await Runtime.runPromiseExit(this.runtime, effect)
    if (Exit.isFailure(exit)) {
      throw convertCauseToDurableExecutionError(exit.cause)
    }
    return exit.value
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
  task<TInput = undefined, TOutput = unknown>(taskOptions: TaskOptions<TInput, TOutput>) {
    return this.runSync(this.effectExecutor.task(taskOptions))
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
  sleepingTask<TOutput = unknown>(taskOptions: SleepingTaskOptions<TOutput>) {
    return this.runSync(this.effectExecutor.sleepingTask<TOutput>(taskOptions))
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
  ): Task<TInput, TOutput, 'parentTask'> {
    return this.runSync(this.effectExecutor.parentTask(parentTaskOptions))
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
  ) {
    return this.runSync(this.effectExecutor.sequentialTasks(id, tasks))
  }

  /**
   * Create a new task that runs a task until it returns `{ isDone: true, output: TOutput }`. If
   * the task doesn't return `{ isDone: true, output: TOutput }` within the max attempts, the
   * looping task will return `{ isSuccess: false }`. Any error from the task will be considered a
   * failure of the looping task.
   *
   * @param id - The id of the looping task.
   * @param iterationTask - The iteration task to run.
   * @param maxAttempts - The maximum number of attempts to run the task.
   * @param sleepMsBeforeRun - The sleep time before running the task. If a function is provided, it
   *   will be called with the attempt number. The initial attempt is 0, then 1, then 2, etc.
   * @returns The looping task.
   */
  loopingTask<TInput = undefined, TOutput = unknown>(
    id: string,
    iterationTask: Task<TInput, { isDone: false } | { isDone: true; output: TOutput }>,
    maxAttempts: number,
    sleepMsBeforeRun?: number | ((attempt: number) => number),
  ) {
    return this.runSync(
      this.effectExecutor.loopingTask(id, iterationTask, maxAttempts, sleepMsBeforeRun),
    )
  }

  private convertEffectValidateInputOutputToPromiseValidateInputOutput<TRunInput, TInput>(
    effectValidateInputOutput: ReturnType<
      typeof this.effectExecutor.validateInput<TRunInput, TInput>
    >,
  ) {
    return {
      task: <TOutput = unknown>(taskOptions: TaskOptions<TRunInput, TOutput>) =>
        this.runSync(effectValidateInputOutput.task(taskOptions)),
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
      ) => this.runSync(effectValidateInputOutput.parentTask(parentTaskOptions)),
      sequentialTasks: <TSequentialTasks extends ReadonlyArray<AnyTask>>(
        id: string,
        tasks: SequentialTasks<TSequentialTasks>,
      ) => this.runSync(effectValidateInputOutput.sequentialTasks(id, tasks)),
      loopingTask: <TOutput = unknown>(
        id: string,
        iterationTask: Task<TRunInput, { isDone: false } | { isDone: true; output: TOutput }>,
        maxAttempts: number,
        sleepMsBeforeRun?: number | ((attempt: number) => number),
      ) =>
        this.runSync(
          effectValidateInputOutput.loopingTask(id, iterationTask, maxAttempts, sleepMsBeforeRun),
        ),
    }
  }

  /**
   * Add a validate input function to the durable executor.
   *
   * @param validateInputFn - The validate input function.
   * @returns The validate input function.
   */
  validateInput<TRunInput, TInput>(
    validateInputFn: (input: TInput) => TRunInput | Promise<TRunInput>,
  ) {
    return this.convertEffectValidateInputOutputToPromiseValidateInputOutput<TRunInput, TInput>(
      this.effectExecutor.validateInput(validateInputFn),
    )
  }

  /**
   * Add an input schema to the durable executor.
   *
   * @param inputSchema - The input schema.
   * @returns The input schema.
   */
  inputSchema<TInputSchema extends AnySchema>(inputSchema: TInputSchema) {
    return this.convertEffectValidateInputOutputToPromiseValidateInputOutput<
      InferSchemaOutput<TInputSchema>,
      InferSchemaInput<TInputSchema>
    >(this.effectExecutor.inputSchema(inputSchema))
  }

  private convertEffectTaskExecutionHandleToPromiseTaskExecutionHandle<TOutput>(
    effectTaskExecutionHandle: Effect.Effect.Success<
      ReturnType<typeof this.effectExecutor.getTaskExecutionHandle<Task<unknown, TOutput>>>
    >,
  ) {
    return {
      taskId: effectTaskExecutionHandle.taskId,
      executionId: effectTaskExecutionHandle.executionId,
      getExecution: async () => {
        return this.runPromise(effectTaskExecutionHandle.getExecution)
      },
      waitAndGetFinishedExecution: async (options?: {
        pollingIntervalMs?: number
        signal?: AbortSignal
      }) => {
        const effect = effectTaskExecutionHandle.waitAndGetFinishedExecution({
          pollingIntervalMs: options?.pollingIntervalMs,
        })
        if (!options?.signal) {
          return await this.runPromise(effect)
        }

        const signal = options.signal
        const effectMapped = effect.pipe(
          Effect.map((value) => ({ _tag: 'result' as const, value })),
        )
        const abortEffect = Effect.async<void, never, never>((resume) => {
          if (signal.aborted) {
            resume(Effect.void)
            return undefined
          }
          const onAbort = () => resume(Effect.void)
          signal.addEventListener('abort', onAbort, { once: true })
          return Effect.sync(() => {
            signal.removeEventListener('abort', onAbort)
          })
        }).pipe(Effect.as({ _tag: 'aborted' as const }))
        return await this.runPromise(
          Effect.race(effectMapped, abortEffect).pipe(
            Effect.flatMap((res) =>
              res._tag === 'aborted'
                ? Effect.fail(new DurableExecutionCancelledError())
                : Effect.succeed(res.value),
            ),
          ),
        )
      },
      cancel: async () => {
        return await this.runPromise(effectTaskExecutionHandle.cancel)
      },
    }
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
  ) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const effectTaskExecutionHandle = (await this.runPromise(
      this.effectExecutor.enqueueTask(
        ...(rest as Parameters<typeof this.effectExecutor.enqueueTask<TTask>>),
      ),
    )) as Effect.Effect.Success<
      ReturnType<typeof this.effectExecutor.getTaskExecutionHandle<TTask>>
    >
    return this.convertEffectTaskExecutionHandleToPromiseTaskExecutionHandle<
      InferTaskOutput<TTask>
    >(effectTaskExecutionHandle)
  }

  /**
   * Get a handle to a task execution.
   *
   * @param task - The task to get the handle for.
   * @param executionId - The id of the execution to get the handle for.
   * @returns The handle to the task execution.
   */
  async getTaskExecutionHandle<TTask extends AnyTask>(task: TTask, executionId: string) {
    const effectTaskExecutionHandle = await this.runPromise(
      this.effectExecutor.getTaskExecutionHandle(task, executionId),
    )
    return this.convertEffectTaskExecutionHandleToPromiseTaskExecutionHandle<
      InferTaskOutput<TTask>
    >(effectTaskExecutionHandle)
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
  async wakeupSleepingTaskExecution<TTask extends Task<unknown, unknown, 'sleepingTask'>>(
    task: TTask,
    sleepingTaskUniqueId: string,
    options: WakeupSleepingTaskExecutionOptions<InferTaskOutput<TTask>>,
  ) {
    return this.runPromise(
      this.effectExecutor.wakeupSleepingTaskExecution(task, sleepingTaskUniqueId, options),
    )
  }

  /**
   * Start the durable executor. Use {@link DurableExecutor.shutdown} to stop the durable executor.
   */
  async start() {
    return await this.runPromise(this.effectExecutor.start)
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
  async shutdown() {
    return await Effect.runPromise(Scope.close(this.scope, Exit.void))
  }

  /**
   * Get the running task executions count.
   *
   * @returns The running task executions count.
   */
  getRunningTaskExecutionsCount(): number {
    return this.runSync(this.effectExecutor.getRunningTaskExecutionsCount)
  }

  /**
   * Get the running task execution ids.
   *
   * @returns The running task execution ids.
   */
  getRunningTaskExecutionIds(): ReadonlySet<string> {
    return this.runSync(this.effectExecutor.getRunningTaskExecutionIds)
  }

  /**
   * Get storage metrics for monitoring and debugging.
   *
   * @returns The storage metrics.
   */
  async getStorageMetrics() {
    return await this.runPromise(this.effectExecutor.getStorageMetrics)
  }
}
