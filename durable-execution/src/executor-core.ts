import type { StandardSchemaV1 } from '@standard-schema/spec'
import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
import { getErrorMessage } from '@gpahal/std/errors'
import { isFunction } from '@gpahal/std/functions'
import { sleep } from '@gpahal/std/promises'
import { isString } from '@gpahal/std/strings'

import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
  type DurableExecutionErrorStorageValue,
} from './errors'
import {
  createConsoleLogger,
  LoggerInternal,
  zLogger,
  zLogLevel,
  type Logger,
  type LogLevel,
} from './logger'
import { SerializerInternal, zSerializer, type Serializer } from './serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  createTaskExecutionStorageValue,
  type TaskExecutionsStorage,
  type TaskExecutionStorageValue,
} from './storage'
import {
  TaskExecutionsStorageInternal,
  zStorageMaxRetryAttempts,
  type TaskExecutionStorageUpdateInternal,
} from './storage-internal'
import {
  ChildTask,
  FINISHED_TASK_EXECUTION_STATUSES,
  type AnyTask,
  type DefaultParentTaskOutput,
  type FinishedChildTaskExecution,
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
  type TaskExecutionSummary,
  type TaskOptions,
  type TaskRunContext,
  type WakeupSleepingTaskExecutionOptions,
} from './task'
import {
  generateTaskExecutionId,
  overrideTaskEnqueueOptions,
  TaskInternal,
  validateCommonTaskOptions,
  validateEnqueueOptions,
} from './task-internal'
import { createCancellablePromiseCustom, generateId, summarizeStandardSchemaIssues } from './utils'

export const zDurableExecutorCoreOptions = z.object({
  serializer: zSerializer.nullish(),
  logger: zLogger.nullish(),
  logLevel: zLogLevel.nullish(),
  expireLeewayMs: z
    .number()
    .nullish()
    .transform((val) => val ?? 300_000),
  maxConcurrentTaskExecutions: z
    .number()
    .nullish()
    .transform((val) => val ?? 5000),
  maxTaskExecutionsPerBatch: z
    .number()
    .nullish()
    .transform((val) => val ?? 100),
  processOnChildrenFinishedTaskExecutionsBatchSize: z
    .number()
    .int()
    .min(1)
    .max(100)
    .nullish()
    .transform((val) => val ?? 100),
  markFinishedTaskExecutionsAsCloseStatusReadyBatchSize: z
    .number()
    .int()
    .min(1)
    .max(200)
    .nullish()
    .transform((val) => val ?? 100),
  closeFinishedTaskExecutionsBatchSize: z
    .number()
    .int()
    .min(1)
    .max(100)
    .nullish()
    .transform((val) => val ?? 100),
  cancelNeedsPromiseCancellationTaskExecutionsBatchSize: z
    .number()
    .int()
    .min(1)
    .max(200)
    .nullish()
    .transform((val) => val ?? 100),
  retryExpiredTaskExecutionsBatchSize: z
    .number()
    .int()
    .min(1)
    .max(200)
    .nullish()
    .transform((val) => val ?? 100),
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
  enableStorageBatching: z
    .boolean()
    .nullish()
    .transform((val) => val ?? false),
  storageBatchingBackgroundProcessIntraBatchSleepMs: z
    .number()
    .int()
    .min(1)
    .max(100)
    .nullish()
    .transform((val) => val ?? 10),
  storageMaxRetryAttempts: zStorageMaxRetryAttempts,
})

/**
 * A durable executor core. It is used to execute tasks durably, reliably and resiliently but
 * doesn't provide any background processes. This is used for implementing durable executors with
 * custom background processes. Use {@link DurableExecutor} to create a durable executor with
 * background processes.
 *
 * @category Executor
 */
export class DurableExecutorCore {
  readonly id: string
  readonly logger: LoggerInternal
  private readonly storage: TaskExecutionsStorageInternal
  private readonly serializer: SerializerInternal
  private readonly expireLeewayMs: number
  private readonly maxConcurrentTaskExecutions: number
  private readonly maxTaskExecutionsPerBatch: number
  private readonly processOnChildrenFinishedTaskExecutionsBatchSize: number
  private readonly markFinishedTaskExecutionsAsCloseStatusReadyBatchSize: number
  private readonly closeFinishedTaskExecutionsBatchSize: number
  private readonly cancelNeedsPromiseCancellationTaskExecutionsBatchSize: number
  private readonly retryExpiredTaskExecutionsBatchSize: number
  private readonly maxChildrenPerTaskExecution: number
  private readonly maxSerializedInputDataSize: number
  private readonly maxSerializedOutputDataSize: number
  readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private readonly backgroundProcessesPromises: Set<Promise<void>>
  private readonly backgroundPromises: Set<Promise<void>>
  readonly taskInternalsMap: Map<string, TaskInternal>
  readonly runningTaskExecutionsMap: Map<
    string,
    {
      promise: Promise<void>
      cancel: () => void
    }
  >

  /**
   * Create a durable executor core.
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
   * @param options.maxConcurrentTaskExecutions - The maximum number of tasks that can run
   *   concurrently. If not provided, defaults to 5000.
   * @param options.maxTaskExecutionsPerBatch - The maximum number of tasks to process in each
   *   batch. If not provided, defaults to 100.
   * @param options.processOnChildrenFinishedTaskExecutionsBatchSize - The maximum number of on
   *   children finished task executions to process in each batch. If not provided, defaults to
   *   100.
   * @param options.markFinishedTaskExecutionsAsCloseStatusReadyBatchSize - The maximum number of
   *   finished task executions to mark as close status ready in each batch. If not provided,
   *   defaults to 100.
   * @param options.closeFinishedTaskExecutionsBatchSize - The maximum number of finished task
   *   executions to close in each batch. If not provided, defaults to 100.
   * @param options.cancelNeedsPromiseCancellationTaskExecutionsBatchSize - The maximum number of
   *   needs promise cancellation task executions to cancel in each batch. If not provided,
   *   defaults to 100.
   * @param options.retryExpiredTaskExecutionsBatchSize - The maximum number of expired task
   *   executions to retry in each batch. If not provided, defaults to 100.
   * @param options.maxChildrenPerTaskExecution - The maximum number of children tasks per parent
   *   task. If not provided, defaults to 1000.
   * @param options.maxSerializedInputDataSize - The maximum size of serialized input data in
   *   bytes. If not provided, defaults to 1MB.
   * @param options.maxSerializedOutputDataSize - The maximum size of serialized output data in
   *   bytes. If not provided, defaults to 1MB.
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
    options: {
      serializer?: Serializer
      logger?: Logger
      logLevel?: LogLevel
      expireLeewayMs?: number
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
    const parsedOptions = zDurableExecutorCoreOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    const {
      serializer,
      logger,
      logLevel,
      expireLeewayMs,
      maxConcurrentTaskExecutions,
      maxTaskExecutionsPerBatch,
      processOnChildrenFinishedTaskExecutionsBatchSize,
      markFinishedTaskExecutionsAsCloseStatusReadyBatchSize,
      closeFinishedTaskExecutionsBatchSize,
      cancelNeedsPromiseCancellationTaskExecutionsBatchSize,
      retryExpiredTaskExecutionsBatchSize,
      maxChildrenPerTaskExecution,
      maxSerializedInputDataSize,
      maxSerializedOutputDataSize,
      enableStorageBatching,
      storageBatchingBackgroundProcessIntraBatchSleepMs,
      storageMaxRetryAttempts,
    } = parsedOptions.data

    this.id = `de_${generateId(8)}`
    this.serializer = new SerializerInternal(serializer)
    this.logger = new LoggerInternal(
      logger ?? createConsoleLogger(`DurableExecutor:${this.id}`),
      logLevel,
    )
    this.expireLeewayMs = expireLeewayMs
    this.maxConcurrentTaskExecutions = maxConcurrentTaskExecutions
    this.maxTaskExecutionsPerBatch = maxTaskExecutionsPerBatch
    this.processOnChildrenFinishedTaskExecutionsBatchSize =
      processOnChildrenFinishedTaskExecutionsBatchSize
    this.markFinishedTaskExecutionsAsCloseStatusReadyBatchSize =
      markFinishedTaskExecutionsAsCloseStatusReadyBatchSize
    this.closeFinishedTaskExecutionsBatchSize = closeFinishedTaskExecutionsBatchSize
    this.cancelNeedsPromiseCancellationTaskExecutionsBatchSize =
      cancelNeedsPromiseCancellationTaskExecutionsBatchSize
    this.retryExpiredTaskExecutionsBatchSize = retryExpiredTaskExecutionsBatchSize
    this.maxChildrenPerTaskExecution = maxChildrenPerTaskExecution
    this.maxSerializedInputDataSize = maxSerializedInputDataSize
    this.maxSerializedOutputDataSize = maxSerializedOutputDataSize
    this.storage = new TaskExecutionsStorageInternal(
      this.logger,
      storage,
      enableStorageBatching,
      storageBatchingBackgroundProcessIntraBatchSleepMs,
      storageMaxRetryAttempts,
    )

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.backgroundProcessesPromises = new Set()
    this.backgroundPromises = new Set()
    this.taskInternalsMap = new Map()
    this.runningTaskExecutionsMap = new Map()
  }

  /**
   * Throw an error if the durable executor core is shutdown.
   */
  throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw DurableExecutionError.nonRetryable('Durable executor shutdown')
    }
  }

  /**
   * Add a background process promise to the durable executor core.
   *
   * @param promise - The promise to add.
   */
  addBackgroundProcessesPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundProcessesPromises.add(
      wrappedPromise().finally(() => this.backgroundPromises.delete(promise)),
    )
  }

  /**
   * Add a background promise to the durable executor core.
   *
   * @param promise - The promise to add.
   */
  addBackgroundPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundPromises.add(
      wrappedPromise().finally(() => this.backgroundPromises.delete(promise)),
    )
  }

  /**
   * Get the storage for the durable executor core.
   *
   * @returns The storage for the durable executor core.
   */
  getStorage(): TaskExecutionsStorageInternal {
    return this.storage
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
    this.throwIfShutdown()

    const validatedCommonTaskOptions = validateCommonTaskOptions(taskOptions)
    const taskInternal = TaskInternal.fromTaskOptions(this.taskInternalsMap, taskOptions)
    this.logger.debug(`Added task ${taskOptions.id}`)
    return {
      id: taskInternal.id,
      isSleepingTask: false,
      retryOptions: validatedCommonTaskOptions.retryOptions,
      sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
      timeoutMs: validatedCommonTaskOptions.timeoutMs,
      areChildrenSequential: false,
    }
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
    this.throwIfShutdown()

    const validatedCommonTaskOptions = validateCommonTaskOptions(taskOptions)
    const taskInternal = TaskInternal.fromSleepingTaskOptions(this.taskInternalsMap, taskOptions)
    this.logger.debug(`Added sleeping task ${taskOptions.id}`)
    return {
      id: taskInternal.id,
      isSleepingTask: true,
      retryOptions: validatedCommonTaskOptions.retryOptions,
      sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
      timeoutMs: validatedCommonTaskOptions.timeoutMs,
      areChildrenSequential: false,
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
  ): Task<TInput, TOutput, false> {
    return this.parentTaskInternal(parentTaskOptions, false)
  }

  private parentTaskInternal<
    TInput = undefined,
    TRunOutput = unknown,
    TOutput = DefaultParentTaskOutput<TRunOutput>,
    TFinalizeTaskRunOutput = unknown,
  >(
    parentTaskOptions: ParentTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    areChildrenSequential: boolean,
  ): Task<TInput, TOutput, false> {
    this.throwIfShutdown()

    const validatedCommonTaskOptions = validateCommonTaskOptions(parentTaskOptions)
    const taskInternal = TaskInternal.fromParentTaskOptions(
      this.taskInternalsMap,
      parentTaskOptions,
      areChildrenSequential,
    )
    this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
    return {
      id: taskInternal.id,
      isSleepingTask: false,
      retryOptions: validatedCommonTaskOptions.retryOptions,
      sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
      timeoutMs: validatedCommonTaskOptions.timeoutMs,
      areChildrenSequential,
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
    task: <TOutput>(taskOptions: TaskOptions<TRunInput, TOutput>) => Task<TInput, TOutput, false>
    parentTask: <
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
      TFinalizeTaskRunOutput = unknown,
    >(
      parentTaskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    ) => Task<TInput, TOutput, false>
  } {
    this.throwIfShutdown()

    return {
      task: <TOutput>(
        taskOptions: TaskOptions<TRunInput, TOutput>,
      ): Task<TInput, TOutput, false> => {
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
          isSleepingTask: false,
          retryOptions: validatedCommonTaskOptions.retryOptions,
          sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
          timeoutMs: validatedCommonTaskOptions.timeoutMs,
          areChildrenSequential: false,
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
      ): Task<TInput, TOutput, false> => {
        this.throwIfShutdown()

        const validatedCommonTaskOptions = validateCommonTaskOptions(parentTaskOptions)
        const taskInternal = TaskInternal.fromParentTaskOptions(
          this.taskInternalsMap,
          parentTaskOptions,
          false,
          validateInputFn,
        )
        this.logger.debug(`Added parent task ${parentTaskOptions.id}`)
        return {
          id: taskInternal.id,
          isSleepingTask: false,
          retryOptions: validatedCommonTaskOptions.retryOptions,
          sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
          timeoutMs: validatedCommonTaskOptions.timeoutMs,
          areChildrenSequential: false,
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
  sequentialTasks<TSequentialTasks extends ReadonlyArray<AnyTask>>(
    id: string,
    tasks: SequentialTasks<TSequentialTasks>,
  ): Task<
    InferTaskInput<TSequentialTasks[0]>,
    InferTaskOutput<LastTaskElementInArray<TSequentialTasks>>
  > {
    if (tasks.length === 0) {
      throw DurableExecutionError.nonRetryable('No tasks provided')
    }

    const firstTask = tasks[0]
    const otherTasks = tasks.slice(1)
    return this.parentTaskInternal(
      {
        id,
        timeoutMs: 30_000,
        runParent: (_, input) => {
          return {
            output: undefined,
            children: [
              new ChildTask(firstTask, input),
              ...otherTasks.map((task) => new ChildTask(task, undefined)),
            ],
          }
        },
        finalize: ({ children }) => {
          if (children.length === 0) {
            return undefined
          }

          const lastChild = children.at(-1)!
          if (lastChild.status !== 'completed') {
            throw DurableExecutionError.nonRetryable(
              `Task at index ${children.length - 1} failed: ${lastChild.error.message}`,
            )
          }

          return lastChild.output
        },
      },
      true,
    )
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
    if (maxAttempts <= 0) {
      throw DurableExecutionError.nonRetryable('Max attempts must be greater than 0')
    }

    const pollingTaskInner: Task<
      { input: TInput; attempt: number },
      { isSuccess: false } | { isSuccess: true; output: TOutput }
    > = this.parentTask<
      { input: TInput; attempt: number },
      { input: TInput; attempt: number },
      { isSuccess: false } | { isSuccess: true; output: TOutput },
      { isSuccess: false } | { isSuccess: true; output: TOutput }
    >({
      id: `${id}_inner`,
      timeoutMs: 30_000,
      runParent: (_, input) => {
        return {
          output: input,
          children: [
            new ChildTask(pollTask, input.input, {
              sleepMsBeforeRun: (sleepMsBeforeRun && isFunction(sleepMsBeforeRun)
                ? sleepMsBeforeRun(input.attempt)
                : sleepMsBeforeRun != null
                  ? sleepMsBeforeRun
                  : pollTask.sleepMsBeforeRun) as number | undefined,
            }),
          ],
        }
      },
      finalize: {
        id: `${id}_inner_finalize`,
        timeoutMs: 30_000,
        runParent: (_, { output: input, children }) => {
          const child = children[0]! as FinishedChildTaskExecution<
            { isDone: false } | { isDone: true; output: TOutput }
          >
          if (child.status !== 'completed') {
            throw DurableExecutionError.nonRetryable(
              `Poll task ${pollTask.id} failed: ${child.error.message}`,
            )
          }

          const childOutput = child.output
          if (childOutput.isDone) {
            return { output: { isSuccess: true, output: childOutput.output } }
          }
          if (input.attempt + 1 >= maxAttempts) {
            return { output: { isSuccess: false } }
          }

          return {
            output: { isSuccess: false },
            children: [
              new ChildTask(pollingTaskInner, { input: input.input, attempt: input.attempt + 1 }),
            ],
          }
        },
        finalize: ({ output, children }) => {
          if (output.isSuccess) {
            return output
          }
          if (children.length === 0) {
            return { isSuccess: false }
          }

          const child = children[0]! as FinishedChildTaskExecution<
            { isSuccess: false } | { isSuccess: true; output: TOutput }
          >
          if (child.status !== 'completed') {
            throw DurableExecutionError.nonRetryable(child.error.message)
          }

          return child.output
        },
      },
    })

    const pollingTask = this.parentTask<
      TInput,
      undefined,
      { isSuccess: false } | { isSuccess: true; output: TOutput }
    >({
      id,
      timeoutMs: 30_000,
      runParent: (_, input) => {
        return {
          output: undefined,
          children: [new ChildTask(pollingTaskInner, { input, attempt: 0 })],
        }
      },
      finalize: ({ children }) => {
        const child = children[0]! as FinishedChildTaskExecution<
          { isSuccess: false } | { isSuccess: true; output: TOutput }
        >
        if (child.status !== 'completed') {
          throw DurableExecutionError.nonRetryable(child.error.message)
        }

        return child.output
      },
    })
    return pollingTask
  }

  private getSleepingTaskUniqueId(task: AnyTask, input: unknown): string {
    if (input == null) {
      throw DurableExecutionError.nonRetryable(
        `A unique id string is required to enqueue a sleeping task ${task.id}. This unique id is used to wake up the sleeping task later`,
      )
    }
    if (!isString(input)) {
      throw DurableExecutionError.nonRetryable(
        `Input must be a unique id string for sleeping task ${task.id}. This unique id is used to wake up the sleeping task later`,
      )
    }
    if (input.length === 0) {
      throw DurableExecutionError.nonRetryable(
        `A non-empty unique id string is required for sleeping task ${task.id}. This unique id is used to wake up the sleeping task later`,
      )
    }
    if (input.length > 255) {
      throw DurableExecutionError.nonRetryable(
        `The unique id string must be shorter than 256 characters for sleeping task ${task.id}`,
      )
    }
    return input
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
            skipTaskPresenceCheck?: boolean
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
          },
        ]
      : [
          task: TTask,
          input: InferTaskInput<TTask>,
          options?: TaskEnqueueOptions<TTask> & {
            skipTaskPresenceCheck?: boolean
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
          },
        ]
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTask>>> {
    this.throwIfShutdown()

    const task = rest[0]
    const input = rest.length > 1 ? rest[1]! : undefined
    const options = rest.length > 2 ? rest[2]! : undefined
    if (!options?.skipTaskPresenceCheck && !this.taskInternalsMap.has(task.id)) {
      throw new DurableExecutionNotFoundError(
        `Task ${task.id} not found. Use DurableExecutor.task() to add it before enqueuing it`,
      )
    }

    const sleepingTaskUniqueId = task.isSleepingTask
      ? this.getSleepingTaskUniqueId(task, input)
      : undefined
    const executionId = generateTaskExecutionId()
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
    const now = Date.now()
    await (options?.taskExecutionsStorageTransaction ?? this.storage).insertMany([
      createTaskExecutionStorageValue({
        now,
        taskId: task.id,
        executionId,
        isSleepingTask: task.isSleepingTask,
        sleepingTaskUniqueId,
        retryOptions: finalEnqueueOptions.retryOptions,
        sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
        timeoutMs: finalEnqueueOptions.timeoutMs,
        areChildrenSequential: task.areChildrenSequential,
        input: this.serializer.serialize(
          task.isSleepingTask ? undefined : input,
          this.maxSerializedInputDataSize,
        ),
      }),
    ])

    this.logger.debug(`Enqueued task ${task.id} with execution id ${executionId}`)
    return this.getTaskExecutionHandleInternal(task.id, executionId)
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
        `Task execution ${executionId} belongs to task ${execution.taskId}`,
      )
    }

    return this.getTaskExecutionHandleInternal(task.id, executionId)
  }

  private getTaskExecutionHandleInternal<TOutput = unknown>(
    taskId: string,
    executionId: string,
  ): TaskExecutionHandle<TOutput> {
    return {
      getTaskId: () => taskId,
      getExecutionId: () => executionId,
      getExecution: async () => {
        const execution = await this.storage.getById(executionId, {})
        if (!execution) {
          throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
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
          pollingIntervalMs && pollingIntervalMs > 0 ? pollingIntervalMs : 2500
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

          const execution = await this.storage.getById(executionId, {})
          if (!execution) {
            throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
          }

          if (FINISHED_TASK_EXECUTION_STATUSES.includes(execution.status)) {
            return convertTaskExecutionStorageValueToTaskExecution(
              execution,
              this.serializer,
            ) as FinishedTaskExecution<TOutput>
          } else {
            this.logger.debug(
              `Waiting for task ${executionId} to be finished. Status: ${execution.status}`,
            )
          }
        }
      },
      cancel: async () => {
        const now = Date.now()
        await this.storage.updateById(
          now,
          executionId,
          {
            isFinished: false,
          },
          {
            status: 'cancelled',
            error: convertDurableExecutionErrorToStorageValue(new DurableExecutionCancelledError()),
            needsPromiseCancellation: true,
          },
        )
        this.logger.debug(`Cancelled task execution ${executionId}`)
      },
    }
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
    this.throwIfShutdown()

    if (!task.isSleepingTask) {
      throw DurableExecutionError.nonRetryable(`Task ${task.id} is not a sleeping task`)
    }

    const execution = await this.storage.getBySleepingTaskUniqueId(sleepingTaskUniqueId)
    if (!execution) {
      throw new DurableExecutionNotFoundError(
        `Sleeping task execution ${sleepingTaskUniqueId} not found`,
      )
    }
    if (execution.taskId !== task.id) {
      throw new DurableExecutionNotFoundError(
        `Sleeping task execution ${sleepingTaskUniqueId} belongs to task ${execution.taskId}`,
      )
    }
    if (execution.isFinished) {
      return convertTaskExecutionStorageValueToTaskExecution(
        execution,
        this.serializer,
      ) as FinishedTaskExecution<InferTaskOutput<TTask>>
    }

    const update: TaskExecutionStorageUpdateInternal = {
      status: options.status,
    }
    if (options.status === 'completed') {
      update.output = this.serializer.serialize(options.output)
    } else if (options.status === 'failed') {
      update.error = convertDurableExecutionErrorToStorageValue(
        DurableExecutionError.nonRetryable(getErrorMessage(options.error)),
      )
    } else {
      throw DurableExecutionError.nonRetryable(
        // @ts-expect-error - This is safe
        `Invalid status for task execution ${executionId}: ${options.status}`,
      )
    }

    const now = Date.now()
    await this.storage.updateById(
      now,
      execution.executionId,
      {
        isSleepingTask: true,
        isFinished: false,
      },
      update,
    )
    this.logger.debug(`Woken up sleeping task execution ${execution.executionId}`)

    const finishedExecution = await this.storage.getById(execution.executionId, {})
    if (!finishedExecution) {
      throw new DurableExecutionNotFoundError(
        `Sleeping task execution ${execution.executionId} not found`,
      )
    }
    if (!finishedExecution.isFinished) {
      throw DurableExecutionError.nonRetryable(
        `Task execution ${execution.executionId} is not a sleeping task execution`,
      )
    }
    return convertTaskExecutionStorageValueToTaskExecution(
      finishedExecution,
      this.serializer,
    ) as FinishedTaskExecution<InferTaskOutput<TTask>>
  }

  /**
   * Start the durable executor core background processes. Use {@link DurableExecutorCore.shutdown}
   * to stop the durable executor.
   */
  startBackgroundProcesses(getBackgroundProcessesPromises: () => Array<Promise<void>>): void {
    this.throwIfShutdown()

    this.logger.info('Starting background processes')
    for (const promise of getBackgroundProcessesPromises()) {
      this.addBackgroundProcessesPromise(promise)
    }
    this.storage.startBackgroundProcesses()
    this.logger.info('Started background processes')
  }

  /**
   * Shutdown the durable executor core. Cancels all active executions and stops the background
   * processes and promises.
   *
   * On shutdown, these happen in this order:
   * - Stop enqueuing new tasks
   * - Stop background processes and promises after the current iteration
   * - Wait for active task executions to finish. Task execution context contains a shutdown signal
   *   that can be used to gracefully shutdown the task when executor is shutting down.
   */
  async shutdown(): Promise<void> {
    const startTime = Date.now()
    this.logger.info('Shutting down durable executor')
    if (!this.shutdownSignal.isCancelled()) {
      this.cancelShutdownSignal()
    }
    this.logger.debug('Durable executor cancelled')

    if (this.backgroundPromises.size > 0) {
      this.logger.debug('Stopping background processes and promises')
      await Promise.all(this.backgroundProcessesPromises)
      this.backgroundProcessesPromises.clear()
      await Promise.all(this.backgroundPromises)
      this.backgroundPromises.clear()
    }
    this.logger.debug('Background processes and promises stopped')

    this.logger.debug('Stopping active task executions')
    const runningExecutions = [...this.runningTaskExecutionsMap.entries()]
    for (const [executionId, runningExecution] of runningExecutions) {
      try {
        await runningExecution.promise
        this.runningTaskExecutionsMap.delete(executionId)
      } catch (error) {
        this.logger.error(`Error in waiting for running task execution ${executionId}`, error)
      }
    }
    this.runningTaskExecutionsMap.clear()
    this.logger.debug('Active task executions stopped')

    await this.storage.shutdown()
    const durationMs = Date.now() - startTime
    this.logger.info(`Durable executor shut down in ${(durationMs / 1000).toFixed(2)}s`)
  }

  /**
   * Process task executions that are ready to run based on status being ready and startAt
   * being in the past. Implements backpressure by respecting maxConcurrentTaskExecutions limit.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async processReadyTaskExecutionsSingleBatch(): Promise<boolean> {
    const currConcurrentTaskExecutions = this.runningTaskExecutionsMap.size
    if (currConcurrentTaskExecutions >= this.maxConcurrentTaskExecutions) {
      this.logger.debug(
        `At max concurrent task execution limit (${currConcurrentTaskExecutions}/${this.maxConcurrentTaskExecutions}), skipping processing ready task executions`,
      )
      return true
    }

    const availableLimit = this.maxConcurrentTaskExecutions - currConcurrentTaskExecutions
    const batchLimit = Math.min(this.maxTaskExecutionsPerBatch, availableLimit)
    const now = Date.now()
    const expiresAt = now + this.expireLeewayMs
    const executions = await this.storage.updateByStatusAndStartAtLessThanAndReturn(
      now,
      'ready',
      now,
      {
        executorId: this.id,
        status: 'running',
        startedAt: now,
      },
      expiresAt,
      batchLimit,
    )

    this.logger.debug(`Processing ${executions.length} ready task executions`)
    if (executions.length === 0) {
      return true
    }

    for (const execution of executions) {
      this.runTaskExecutionWithCancelSignal(execution)
    }

    return executions.length === 0
  }

  /**
   * Run a task execution with a cancel signal. It is expected to be in running state.
   *
   * It will add the execution to the running executions map and make sure it is removed from the
   * running executions map if the task execution completes. If the process crashes, the execution
   * will be retried later on expiration.
   *
   * @param execution - The task execution to run.
   */
  private runTaskExecutionWithCancelSignal(execution: TaskExecutionStorageValue): void {
    const executionId = execution.executionId
    if (this.runningTaskExecutionsMap.has(executionId)) {
      return
    }

    const [cancelSignal, cancel] = createCancelSignal()
    const promise = this.runTaskExecutionWithContext(execution, cancelSignal)
      .catch((error) => {
        this.logger.error(`Error in running task execution ${executionId}`, error)
      })
      .finally(() => {
        try {
          cancel()
        } catch (error) {
          this.logger.error(`Error in cancelling task execution ${executionId}`, error)
        }
        this.runningTaskExecutionsMap.delete(executionId)
      })
    this.runningTaskExecutionsMap.set(executionId, {
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
   * @param execution - The task execution to run.
   * @param cancelSignal - The cancel signal.
   */
  private async runTaskExecutionWithContext(
    execution: TaskExecutionStorageValue,
    cancelSignal: CancelSignal,
  ): Promise<void> {
    const taskRunCtx: TaskRunContext = {
      root: execution.root,
      parent: execution.parent,
      taskId: execution.taskId,
      executionId: execution.executionId,
      cancelSignal,
      shutdownSignal: this.shutdownSignal,
      attempt: execution.retryAttempts,
      prevError: execution.error,
    }

    try {
      const taskInternal = this.taskInternalsMap.get(execution.taskId)
      if (!taskInternal) {
        throw new DurableExecutionNotFoundError(`Task ${execution.taskId} not found`)
      }

      const result = await taskInternal.runParentWithTimeoutAndCancellation(
        taskRunCtx,
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

      const now = Date.now()
      if (childrenTasks.length === 0) {
        if (taskInternal.finalize) {
          if (isFunction(taskInternal.finalize)) {
            let finalizeOutput: unknown
            let finalizeError: DurableExecutionErrorStorageValue | undefined
            try {
              finalizeOutput = await taskInternal.finalize({
                output: runOutput,
                children: [],
              })
            } catch (error) {
              finalizeError = convertDurableExecutionErrorToStorageValue(
                DurableExecutionError.nonRetryable(getErrorMessage(error)),
              )
            }

            await (finalizeError
              ? this.storage.updateById(
                  now,
                  execution.executionId,
                  {
                    status: 'running',
                  },
                  {
                    status: 'finalize_failed',
                    error: finalizeError,
                    children: [],
                  },
                  execution,
                )
              : this.storage.updateById(
                  now,
                  execution.executionId,
                  {
                    status: 'running',
                  },
                  {
                    status: 'completed',
                    output: this.serializer.serialize(
                      finalizeOutput,
                      this.maxSerializedOutputDataSize,
                    ),
                    children: [],
                  },
                  execution,
                ))
          } else {
            const finalizeTaskInternal = taskInternal.finalize as TaskInternal
            if (finalizeTaskInternal.taskType === 'sleepingTask') {
              throw DurableExecutionError.nonRetryable(
                `Finalize task ${finalizeTaskInternal.id} cannot be a sleeping task`,
              )
            }

            const finalizeTaskInput = {
              output: runOutput,
              children: [],
            }
            const executionId = generateTaskExecutionId()
            await this.storage.updateByIdAndInsertChildrenIfUpdated(
              now,
              execution.executionId,
              {
                status: 'running',
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
                    indexInParentChildren: 0,
                    isOnlyChildOfParent: false,
                    isFinalizeOfParent: true,
                  },
                  taskId: finalizeTaskInternal.id,
                  executionId,
                  isSleepingTask: false,
                  retryOptions: finalizeTaskInternal.retryOptions,
                  sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
                  timeoutMs: finalizeTaskInternal.timeoutMs,
                  areChildrenSequential: finalizeTaskInternal.areChildrenSequential,
                  input: this.serializer.serialize(
                    finalizeTaskInput,
                    this.maxSerializedInputDataSize,
                  ),
                }),
              ],
              execution,
            )
          }
        } else {
          const output =
            taskInternal.taskType === 'parentTask'
              ? this.serializer.serialize(
                  {
                    output: runOutput,
                    children: [],
                  },
                  this.maxSerializedOutputDataSize,
                )
              : this.serializer.serialize(runOutput, this.maxSerializedOutputDataSize)
          await this.storage.updateById(
            now,
            execution.executionId,
            {
              status: 'running',
            },
            {
              status: 'completed',
              output,
              children: [],
            },
            execution,
          )
        }
      } else {
        const childrenStorageValues: Array<TaskExecutionStorageValue> = []
        const children: Array<TaskExecutionSummary> = []
        for (const [index, child] of childrenTasks.entries()) {
          const childTaskInternal = this.taskInternalsMap.get(child.task.id)
          if (!childTaskInternal) {
            throw new DurableExecutionNotFoundError(`Child task ${child.task.id} not found`)
          }

          if (!taskInternal.areChildrenSequential || index === 0) {
            const isChildSleepingTask = childTaskInternal.taskType === 'sleepingTask'
            const childSleepingTaskUniqueId = isChildSleepingTask
              ? this.getSleepingTaskUniqueId(child.task, child.input)
              : undefined
            const executionId = generateTaskExecutionId()
            const finalEnqueueOptions = overrideTaskEnqueueOptions(childTaskInternal, child.options)
            childrenStorageValues.push(
              createTaskExecutionStorageValue({
                now,
                root: execution.root ?? {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                },
                parent: {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                  indexInParentChildren: index,
                  isOnlyChildOfParent: childrenTasks.length === 1,
                  isFinalizeOfParent: false,
                },
                taskId: child.task.id,
                executionId,
                isSleepingTask: isChildSleepingTask,
                sleepingTaskUniqueId: childSleepingTaskUniqueId,
                retryOptions: finalEnqueueOptions.retryOptions,
                sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
                timeoutMs: finalEnqueueOptions.timeoutMs,
                areChildrenSequential: childTaskInternal.areChildrenSequential,
                input: this.serializer.serialize(
                  isChildSleepingTask ? undefined : child.input,
                  this.maxSerializedInputDataSize,
                ),
              }),
            )
            children.push({
              taskId: child.task.id,
              executionId,
            })
          } else {
            children.push({
              taskId: child.task.id,
              executionId: 'dummy',
            })
          }
        }

        await this.storage.updateByIdAndInsertChildrenIfUpdated(
          now,
          execution.executionId,
          {
            status: 'running',
          },
          {
            status: 'waiting_for_children',
            runOutput: this.serializer.serialize(runOutput, this.maxSerializedOutputDataSize),
            children: children,
            activeChildrenCount: childrenStorageValues.length,
          },
          childrenStorageValues,
          execution,
        )
      }
    } catch (error) {
      const durableExecutionError =
        error instanceof DurableExecutionError
          ? error
          : DurableExecutionError.retryable(getErrorMessage(error))

      const now = Date.now()
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
          startAt: now + delayMs,
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
          status: 'running',
        },
        update,
        execution,
      )
    }
  }

  /**
   * Process on children finished task executions.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async processOnChildrenFinishedTaskExecutionsSingleBatch(): Promise<boolean> {
    const now = Date.now()
    const expiresAt = now + 2 * 60 * 1000 + this.expireLeewayMs // 2 minutes + expireLeewayMs
    const executions =
      await this.storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
        now,
        'waiting_for_children',
        'idle',
        {
          onChildrenFinishedProcessingStatus: 'processing',
          onChildrenFinishedProcessingExpiresAt: expiresAt,
        },
        this.processOnChildrenFinishedTaskExecutionsBatchSize,
      )

    this.logger.debug(`Processing ${executions.length} on children finished task executions`)
    if (executions.length === 0) {
      return true
    }

    const markExecutionAsFailed = async (
      execution: TaskExecutionStorageValue,
      error: DurableExecutionError,
      status: 'failed' | 'finalize_failed' = 'failed',
    ): Promise<void> => {
      const now = Date.now()
      await this.storage.updateById(
        now,
        execution.executionId,
        {
          status: 'waiting_for_children',
        },
        {
          status,
          error: convertDurableExecutionErrorToStorageValue(error),
          onChildrenFinishedProcessingStatus: 'processed',
          onChildrenFinishedProcessingFinishedAt: now,
        },
        execution,
      )
    }

    const processExecution = async (execution: TaskExecutionStorageValue): Promise<void> => {
      const taskInternal = this.taskInternalsMap.get(execution.taskId)
      if (!taskInternal) {
        await markExecutionAsFailed(
          execution,
          new DurableExecutionNotFoundError(`Task ${execution.taskId} not found`),
        )
        return
      }

      if (taskInternal.areChildrenSequential) {
        const firstDummyChildIdx = (execution.children || []).findIndex(
          (child) => child.executionId === 'dummy',
        )
        if (firstDummyChildIdx > 0) {
          const firstDummyChild = execution.children![firstDummyChildIdx]!
          const childTaskInternal = this.taskInternalsMap.get(firstDummyChild.taskId)
          if (!childTaskInternal) {
            await markExecutionAsFailed(
              execution,
              new DurableExecutionNotFoundError(`Child task ${firstDummyChild.taskId} not found`),
            )
            return
          }

          const prevChildTaskExecution = await this.storage.getById(
            execution.children![firstDummyChildIdx - 1]!.executionId,
            {},
          )
          if (!prevChildTaskExecution) {
            await markExecutionAsFailed(
              execution,
              new DurableExecutionNotFoundError(
                `Task at index ${firstDummyChildIdx - 1} not found`,
              ),
            )
            return
          }
          if (prevChildTaskExecution.status !== 'completed') {
            await markExecutionAsFailed(
              execution,
              DurableExecutionError.nonRetryable(
                `Task at index ${firstDummyChildIdx - 1} failed: ${prevChildTaskExecution.error?.message || 'Unknown error'}`,
              ),
              'finalize_failed',
            )
            return
          }

          const executionId = generateTaskExecutionId()
          firstDummyChild.executionId = executionId
          const now = Date.now()
          await this.storage.updateByIdAndInsertChildrenIfUpdated(
            now,
            execution.executionId,
            {
              status: 'waiting_for_children',
            },
            {
              children: execution.children!,
              activeChildrenCount: 1,
              onChildrenFinishedProcessingStatus: 'idle',
            },
            [
              createTaskExecutionStorageValue({
                now,
                root: execution.root,
                parent: {
                  taskId: execution.taskId,
                  executionId: execution.executionId,
                  indexInParentChildren: firstDummyChildIdx,
                  isOnlyChildOfParent: false,
                  isFinalizeOfParent: false,
                },
                taskId: firstDummyChild.taskId,
                executionId,
                isSleepingTask: childTaskInternal.taskType === 'sleepingTask',
                retryOptions: childTaskInternal.retryOptions,
                sleepMsBeforeRun: childTaskInternal.sleepMsBeforeRun,
                timeoutMs: childTaskInternal.timeoutMs,
                areChildrenSequential: childTaskInternal.areChildrenSequential,
                input: prevChildTaskExecution.output!,
              }),
            ],
            execution,
          )
          return
        }
      }

      let finalizeFn: ((input: DefaultParentTaskOutput) => Promise<unknown>) | undefined
      let finalizeTaskInternal: TaskInternal | undefined
      if (taskInternal.finalize) {
        if (isFunction(taskInternal.finalize)) {
          finalizeFn = taskInternal.finalize as (input: DefaultParentTaskOutput) => Promise<unknown>
        } else {
          finalizeTaskInternal = taskInternal.finalize as TaskInternal
          if (finalizeTaskInternal?.taskType === 'sleepingTask') {
            await markExecutionAsFailed(
              execution,
              new DurableExecutionNotFoundError(
                `Finalize task ${finalizeTaskInternal.id} cannot be a sleeping task`,
              ),
            )
            return
          }
        }
      }

      let childrenTaskExecutionsForFinalize: Array<FinishedChildTaskExecution> = []
      if (execution.children) {
        let childrenTaskExecutions = await this.storage.getByParentExecutionId(
          execution.executionId,
        )
        const childrenTaskExecutionsMap = new Map(
          childrenTaskExecutions.map((child) => [child.executionId, child]),
        )
        childrenTaskExecutions = execution.children
          .map((child) => childrenTaskExecutionsMap.get(child.executionId))
          .filter(Boolean) as Array<TaskExecutionStorageValue>
        if (childrenTaskExecutions.length !== execution.children.length) {
          await markExecutionAsFailed(
            execution,
            new DurableExecutionNotFoundError(
              `Some children task executions not found: ${execution.children.length - childrenTaskExecutions.length}/${execution.children.length} not found`,
            ),
          )
          return
        }

        childrenTaskExecutionsForFinalize = childrenTaskExecutions.map(
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
      }

      if (!finalizeFn && !finalizeTaskInternal) {
        const now = Date.now()
        await this.storage.updateById(
          now,
          execution.executionId,
          {
            status: 'waiting_for_children',
          },
          {
            status: 'completed',
            output:
              taskInternal.taskType === 'parentTask'
                ? this.serializer.serialize(
                    {
                      output: execution.runOutput
                        ? this.serializer.deserialize(execution.runOutput)
                        : undefined,
                      children: childrenTaskExecutionsForFinalize,
                    },
                    this.maxSerializedOutputDataSize,
                  )
                : execution.runOutput,
            onChildrenFinishedProcessingStatus: 'processed',
            onChildrenFinishedProcessingFinishedAt: now,
          },
          execution,
        )
        return
      }

      const finalizeTaskInput = {
        output: execution.runOutput ? this.serializer.deserialize(execution.runOutput) : undefined,
        children: childrenTaskExecutionsForFinalize,
      }
      if (finalizeFn) {
        let finalizeOutput: unknown
        let finalizeError: DurableExecutionErrorStorageValue | undefined
        try {
          finalizeOutput = await finalizeFn(finalizeTaskInput)
        } catch (error) {
          finalizeError = convertDurableExecutionErrorToStorageValue(
            DurableExecutionError.nonRetryable(getErrorMessage(error)),
          )
        }

        const now = Date.now()
        await (finalizeError
          ? this.storage.updateById(
              now,
              execution.executionId,
              {
                status: 'waiting_for_children',
              },
              {
                status: 'finalize_failed',
                error: finalizeError,
                onChildrenFinishedProcessingStatus: 'processed',
                onChildrenFinishedProcessingFinishedAt: now,
              },
              execution,
            )
          : this.storage.updateById(
              now,
              execution.executionId,
              {
                status: 'waiting_for_children',
              },
              {
                status: 'completed',
                output: this.serializer.serialize(finalizeOutput, this.maxSerializedOutputDataSize),
                onChildrenFinishedProcessingStatus: 'processed',
                onChildrenFinishedProcessingFinishedAt: now,
              },
              execution,
            ))
        return
      }

      if (!finalizeTaskInternal) {
        return
      }

      const executionId = generateTaskExecutionId()
      const now = Date.now()
      await this.storage.updateByIdAndInsertChildrenIfUpdated(
        now,
        execution.executionId,
        {
          status: 'waiting_for_children',
        },
        {
          status: 'waiting_for_finalize',
          finalize: {
            taskId: finalizeTaskInternal.id,
            executionId,
          },
          onChildrenFinishedProcessingStatus: 'processed',
          onChildrenFinishedProcessingFinishedAt: now,
        },
        [
          createTaskExecutionStorageValue({
            now,
            root: execution.root,
            parent: {
              taskId: execution.taskId,
              executionId: execution.executionId,
              indexInParentChildren: 0,
              isOnlyChildOfParent: false,
              isFinalizeOfParent: true,
            },
            taskId: finalizeTaskInternal.id,
            executionId,
            isSleepingTask: false,
            retryOptions: finalizeTaskInternal.retryOptions,
            sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
            timeoutMs: finalizeTaskInternal.timeoutMs,
            areChildrenSequential: finalizeTaskInternal.areChildrenSequential,
            input: this.serializer.serialize(finalizeTaskInput, this.maxSerializedInputDataSize),
          }),
        ],
        execution,
      )
    }

    for (const execution of executions) {
      this.addBackgroundPromise(processExecution(execution))
    }
    return executions.length === 0
  }

  /**
   * Mark finished task executions as close status ready.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async markFinishedTaskExecutionsAsCloseStatusReadySingleBatch(): Promise<boolean> {
    const now = Date.now()
    const updatedCount =
      await this.storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
        now,
        true,
        'idle',
        {
          closeStatus: 'ready',
        },
        this.markFinishedTaskExecutionsAsCloseStatusReadyBatchSize,
      )

    this.logger.debug(`Marking ${updatedCount} finished task executions as close status ready`)
    return updatedCount === 0
  }

  /**
   * Close finished task executions.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async closeFinishedTaskExecutionsSingleBatch(): Promise<boolean> {
    const now = Date.now()
    const expiresAt = now + 2 * 60 * 1000 + this.expireLeewayMs // 2 minutes + expireLeewayMs
    const executions = await this.storage.updateByCloseStatusAndReturn(
      now,
      'ready',
      {
        closeStatus: 'closing',
        closeExpiresAt: expiresAt,
      },
      this.closeFinishedTaskExecutionsBatchSize,
    )

    this.logger.debug(`Closing ${executions.length} finished task executions`)
    if (executions.length === 0) {
      return true
    }

    const processExecution = async (execution: TaskExecutionStorageValue): Promise<void> => {
      execution.closeStatus = 'closing'
      let now = Date.now()
      await Promise.all([
        this.closeFinishedTaskExecutionParent(execution),
        this.closeFinishedTaskExecutionChildren(execution),
      ])

      now = Date.now()
      await this.storage.updateById(
        now,
        execution.executionId,
        {
          isFinished: true,
        },
        {
          closeStatus: 'closed',
          closedAt: now,
        },
        execution,
      )
    }

    for (const execution of executions) {
      this.addBackgroundPromise(processExecution(execution))
    }
    return executions.length === 0
  }

  /**
   * Close finished task execution parent if it is a finalize of a parent task.
   */
  private async closeFinishedTaskExecutionParent(
    execution: TaskExecutionStorageValue,
  ): Promise<void> {
    if (!execution.parent?.isFinalizeOfParent) {
      return
    }

    const now = Date.now()
    await (execution.status === 'completed'
      ? this.storage.updateById(
          now,
          execution.parent.executionId,
          {
            status: 'waiting_for_finalize',
          },
          {
            status: 'completed',
            output: execution.output ?? undefined,
          },
        )
      : this.storage.updateById(
          now,
          execution.parent.executionId,
          {
            status: 'waiting_for_finalize',
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
   * Close finished task execution children and cancel running children task executions if the parent
   * task is finished but not completed and has children.
   */
  private async closeFinishedTaskExecutionChildren(
    execution: TaskExecutionStorageValue,
  ): Promise<void> {
    if (
      execution.status === 'completed' ||
      ((!execution.children || execution.children.length === 0) && !execution.finalize)
    ) {
      return
    }

    const now = Date.now()
    await this.storage.updateByParentExecutionIdAndIsFinished(now, execution.executionId, false, {
      status: 'cancelled',
      error: convertDurableExecutionErrorToStorageValue(
        new DurableExecutionCancelledError(
          `Parent task ${execution.taskId} with execution id ${execution.executionId} failed: ${execution.error?.message || 'Unknown error'}`,
        ),
      ),
      needsPromiseCancellation: true,
    })
  }

  /**
   * Cancel task executions that need promise cancellation.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async cancelsNeedPromiseCancellationTaskExecutionsSingleBatch(): Promise<boolean> {
    if (this.runningTaskExecutionsMap.size === 0) {
      return true
    }

    const now = Date.now()
    const executions = await this.storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
      now,
      this.id,
      true,
      {
        needsPromiseCancellation: false,
      },
      this.cancelNeedsPromiseCancellationTaskExecutionsBatchSize,
    )

    this.logger.debug(
      `Cancelling ${executions.length} task executions that need promise cancellation`,
    )
    if (executions.length === 0) {
      return true
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
    return executions.length === 0
  }

  /**
   * Retry expired task executions.
   *
   * @returns `true` if the batch doesn't return a full batch, `false` otherwise.
   */
  async retryExpiredTaskExecutionsSingleBatch(): Promise<boolean> {
    let now = Date.now()
    const updatedCount1 = await this.storage.updateByIsSleepingTaskAndExpiresAtLessThan(
      now,
      false,
      now,
      {
        status: 'ready',
        error: convertDurableExecutionErrorToStorageValue(
          DurableExecutionError.nonRetryable('Task execution expired'),
        ),
        startAt: now,
      },
      this.retryExpiredTaskExecutionsBatchSize,
    )

    this.logger.debug(`Retrying ${updatedCount1} expired running task executions`)

    now = Date.now()
    const updatedCount2 = await this.storage.updateByIsSleepingTaskAndExpiresAtLessThan(
      now,
      true,
      now,
      {
        status: 'timed_out',
        error: convertDurableExecutionErrorToStorageValue(new DurableExecutionTimedOutError()),
        startAt: now,
      },
      this.retryExpiredTaskExecutionsBatchSize,
    )

    this.logger.debug(`Timing out ${updatedCount2} expired sleeping task executions`)

    const updatedCount3 = await this.storage.updateByIsSleepingTaskAndExpiresAtLessThan(
      now,
      true,
      now,
      {
        status: 'timed_out',
        error: convertDurableExecutionErrorToStorageValue(new DurableExecutionTimedOutError()),
        startAt: now,
      },
      this.retryExpiredTaskExecutionsBatchSize,
    )

    this.logger.debug(
      `Retrying ${updatedCount3} expired on children finished processing task executions`,
    )

    const updatedCount4 = await this.storage.updateByCloseExpiresAtLessThan(
      now,
      now,
      {
        closeStatus: 'ready',
      },
      this.retryExpiredTaskExecutionsBatchSize,
    )

    this.logger.debug(`Retrying ${updatedCount4} expired close task executions`)

    return (
      updatedCount1 + updatedCount2 + updatedCount3 + updatedCount4 <
      this.retryExpiredTaskExecutionsBatchSize * 0.5
    )
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
    return {
      expireLeewayMs: this.expireLeewayMs,
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
    return Object.fromEntries(
      [...this.storage.timingStats.entries()].map(([key, value]) => [
        key,
        { count: value.count, meanMs: value.meanMs },
      ]),
    )
  }

  /**
   * Get per second storage call counts for monitoring and debugging.
   *
   * @returns The per second storage call counts.
   */
  getStoragePerSecondCallCounts(): Map<number, number> {
    return new Map(this.storage.perSecondStorageCallCounts)
  }
}
