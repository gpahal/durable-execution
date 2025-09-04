import type { StandardSchemaV1 } from '@standard-schema/spec'
import { Clock, Duration, Effect, Either, Fiber, Logger, LogLevel, Ref, Schema } from 'effect'

import { getErrorMessage } from '@gpahal/std/errors'
import { isFunction } from '@gpahal/std/functions'
import { isString } from '@gpahal/std/strings'

import { makeBackgroundProcessor } from './background-processor'
import {
  convertDurableExecutionErrorToStorageValue,
  convertErrorToDurableExecutionError,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from './errors'
import { makeFiberPool } from './fiber-pool'
import { makeSerializerInternal } from './serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  createTaskExecutionStorageValue,
  type TaskExecutionsStorage,
  type TaskExecutionStorageValue,
} from './storage'
import {
  makeTaskExecutionsStorageInternal,
  type TaskExecutionStorageUpdateInternal,
} from './storage-internal'
import {
  childTask,
  FINISHED_TASK_EXECUTION_STATUSES,
  type AnyTask,
  type ChildTask,
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
  type TaskExecutionSummary,
  type TaskOptions,
  type TaskRunContext,
  type WakeupSleepingTaskExecutionOptions,
} from './task'
import {
  addTaskInternal,
  convertParentTaskOptionsToOptionsInternal,
  convertSleepingTaskOptionsToOptionsInternal,
  convertTaskOptionsToOptionsInternal,
  generateTaskExecutionId,
  overrideTaskEnqueueOptions,
  runParentWithTimeout,
  validateEnqueueOptions,
  type TaskInternal,
} from './task-internal'
import {
  convertMaybePromiseOrEffectToEffect,
  convertMaybePromiseToEffect,
  convertPromiseToEffect,
  generateId,
  summarizeStandardSchemaIssues,
} from './utils'

/**
 * Any schema that can be used to validate inputs and outputs of tasks.
 *
 * @category Executor
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnySchema = StandardSchemaV1 | Schema.Schema<any, any, never>

function isStandardSchema<TInput, TOutput>(
  schema: AnySchema,
): schema is StandardSchemaV1<TInput, TOutput> {
  return '~standard' in schema
}

function isEffectSchema<A, I>(schema: AnySchema): schema is Schema.Schema<A, I, never> {
  return 'ast' in schema
}

/**
 * Infer the input type from a schema.
 *
 * @category Executor
 */
export type InferSchemaInput<TSchema extends AnySchema> =
  TSchema extends StandardSchemaV1<infer Input>
    ? Input
    : // eslint-disable-next-line @typescript-eslint/no-explicit-any
      TSchema extends Schema.Schema<any, infer Input, never>
      ? Input
      : never

/**
 * Infer the output type from a schema.
 *
 * @category Executor
 */
export type InferSchemaOutput<TSchema extends AnySchema> =
  TSchema extends StandardSchemaV1<infer Output>
    ? Output
    : // eslint-disable-next-line @typescript-eslint/no-explicit-any
      TSchema extends Schema.Schema<infer Output, any, never>
      ? Output
      : never

/**
 * Schema for the options for the effect durable executor.
 *
 * @category Executor
 */
export const EffectDurableExecutorOptionsSchema = Schema.Struct({
  logLevel: Schema.Literal('debug', 'info', 'error').pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 'info' as const),
  ),
  expireLeewayMs: Schema.Number.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 300_000),
  ),
  backgroundProcessIntraBatchSleepMs: Schema.Number.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 500),
  ),
  maxConcurrentTaskExecutions: Schema.Number.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 5000),
  ),
  maxTaskExecutionsPerBatch: Schema.Number.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  processOnChildrenFinishedTaskExecutionsBatchSize: Schema.Int.pipe(
    Schema.between(1, 200),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  markFinishedTaskExecutionsAsCloseStatusReadyBatchSize: Schema.Int.pipe(
    Schema.between(1, 200),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  closeFinishedTaskExecutionsBatchSize: Schema.Int.pipe(
    Schema.between(1, 200),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  cancelNeedsPromiseCancellationTaskExecutionsBatchSize: Schema.Int.pipe(
    Schema.between(1, 200),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  retryExpiredTaskExecutionsBatchSize: Schema.Int.pipe(
    Schema.between(1, 200),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 100),
  ),
  maxChildrenPerTaskExecution: Schema.Int.pipe(
    Schema.between(1, 1000),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 1000),
  ),
  maxSerializedInputDataSize: Schema.Int.pipe(
    Schema.between(1024, 10 * 1024 * 1024), // 1KB to 10MB
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 1024 * 1024), // 1MB
  ),
  maxSerializedOutputDataSize: Schema.Int.pipe(
    Schema.between(1024, 10 * 1024 * 1024), // 1KB to 10MB
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 1024 * 1024), // 1MB
  ),
  enableStorageBatching: Schema.Boolean.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => false),
  ),
  enableStorageStats: Schema.Boolean.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => false),
  ),
  storageBackgroundBatchingProcessIntraBatchSleepMs: Schema.Number.pipe(
    Schema.between(1, 100),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 10),
  ),
  storageMaxRetryAttempts: Schema.Number.pipe(
    Schema.between(0, 10),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 1),
  ),
})
const decodeEffectDurableExecutorOptions = Schema.decodeUnknown(EffectDurableExecutorOptionsSchema)

/**
 * The options for the effect durable executor.
 *
 * @category Executor
 */
export type EffectDurableExecutorOptions = Schema.Schema.Encoded<
  typeof EffectDurableExecutorOptionsSchema
>

type EffectDurableExecutorState = {
  isStarted: boolean
  isShutdown: boolean
}

/**
 * Make an effect durable executor. It is used to execute tasks durably, reliably and resiliently.
 *
 *
 * Multiple durable executors can share the same storage. In such a case, all the tasks should be
 * present for all the durable executors. The work is distributed among the durable executors. See
 * the [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
 *
 * It provides the same functionality as {@link DurableExecutor} but operates within the effect
 * ecosystem, returning effect values instead of promises.
 *
 * ## Key Differences from DurableExecutor
 *
 * - **Effect integration**: All methods return effect values for composability
 * - **Resource management**: Automatic cleanup via effect's scoped resource management
 * - **Error handling**: Uses effect's structured error handling with Cause
 * - **Concurrency**: Built on effect's fiber-based concurrency model
 *
 * @see {@link DurableExecutor} for more details on the methods available.
 *
 * @see {@link DurableExecutor.make} for more details on the options available.
 *
 * @example
 * ```ts
 * import { makeEffectDurableExecutor } from 'durable-execution'
 * import { Schema } from 'effect'
 *
 * const main = Effect.fn(() => Effect.gen(function* () {
 *   const executor = yield* makeEffectDurableExecutor(storage)
 *
 *   // Create tasks
 *   const extractFileTitle = yield* executor
 *     .inputSchema(Schema.Struct({ filePath: Schema.String }))
 *     .task({
 *       id: 'extractFileTitle',
 *       timeoutMs: 30_000, // 30 seconds
 *       run: async (ctx, input) => {
 *         // ... extract the file title
 *         return {
 *           title: 'File Title',
 *         }
 *       },
 *     })
 *
 *   const summarizeFile = yield* executor
 *     .validateInput(async (input: { filePath: string }) => {
 *       // Example validation function - implement your own validation logic
 *       if (!isValidFilePath(input.filePath)) {
 *         throw new Error('Invalid file path')
 *       }
 *       return {
 *         filePath: input.filePath,
 *       }
 *     })
 *     .task({
 *       id: 'summarizeFile',
 *       timeoutMs: 30_000, // 30 seconds
 *       run: async (ctx, input) => {
 *         // ... summarize the file
 *         return {
 *           summary: 'File summary',
 *         }
 *       },
 *     })
 *
 *   const uploadFile = yield* executor
 *     .inputSchema(Schema.Struct({ filePath: Schema.String, uploadUrl: Schema.String }))
 *     .parentTask({
 *       id: 'uploadFile',
 *       timeoutMs: 60_000, // 1 minute
 *       runParent: async (ctx, input) => {
 *         // ... upload file to the given uploadUrl
 *         // Extract the file title and summarize the file in parallel
 *         return {
 *           output: {
 *             filePath: input.filePath,
 *             uploadUrl: input.uploadUrl,
 *             fileSize: 100,
 *           },
 *           children: [
 *             childTask(extractFileTitle, { filePath: input.filePath }),
 *             childTask(summarizeFile, { filePath: input.filePath }),
 *           ],
 *         }
 *       },
 *       finalize: {
 *         id: 'uploadFileFinalize',
 *         timeoutMs: 60_000, // 1 minute
 *         run: async (ctx, { output, children }) => {
 *           // ... combine the output of the run function and children tasks
 *           return {
 *            filePath: output.filePath,
 *             uploadUrl: output.uploadUrl,
 *             fileSize: 100,
 *             title: 'File Title',
 *             summary: 'File summary',
 *           }
 *         }
 *       },
 *     })
 *
 *   // Start the durable executor
 *   yield* executor.start()
 *
 *   // Enqueue task and manage its execution lifecycle
 *   const uploadFileHandle = yield* executor.enqueueTask(uploadFile, {
 *     filePath: 'file.txt',
 *     uploadUrl: 'https://example.com/upload',
 *   })
 *   const uploadFileExecution = yield* uploadFileHandle.getExecution()
 *   const uploadFileFinishedExecution = yield* uploadFileHandle.waitAndGetFinishedExecution()
 *   yield* uploadFileHandle.cancel()
 *
 *   yield* Effect.log(uploadFileExecution)
 * }).pipe(Effect.scoped)
 * ```
 *
 * @category Executor
 */
export const makeEffectDurableExecutor = Effect.fn(function* (
  options: EffectDurableExecutorOptions = {},
) {
  const {
    logLevel,
    expireLeewayMs,
    backgroundProcessIntraBatchSleepMs,
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
    enableStorageStats,
    storageBackgroundBatchingProcessIntraBatchSleepMs,
    storageMaxRetryAttempts,
  } = yield* decodeEffectDurableExecutorOptions(options).pipe(
    Effect.mapError((error) =>
      DurableExecutionError.nonRetryable(`Invalid options: ${error.message}`),
    ),
  )

  const effect = Effect.gen(function* () {
    const executorId = `de_${generateId(8)}`

    const storage = yield* makeTaskExecutionsStorageInternal({
      executorId,
      enableBatching: enableStorageBatching,
      enableStats: enableStorageStats,
      backgroundBatchingIntraBatchSleepMs: storageBackgroundBatchingProcessIntraBatchSleepMs,
      maxRetryAttempts: storageMaxRetryAttempts,
    })
    const serializer = yield* makeSerializerInternal

    const stateRef = yield* Ref.make<EffectDurableExecutorState>({
      isStarted: false,
      isShutdown: false,
    })
    const shutdownController = new AbortController()

    const taskInternalsMap = new Map<string, TaskInternal>()
    const runningTaskExecutionsMap = new Map<string, Fiber.RuntimeFiber<void, never> | undefined>()

    const fiberPool = yield* makeFiberPool({
      executorId,
      processName: 'EffectDurableExecutor',
    })

    const errorIfShutdown = Ref.get(stateRef).pipe(
      Effect.andThen((state) =>
        state.isShutdown
          ? Effect.fail(DurableExecutionError.nonRetryable('Durable executor shutdown'))
          : Effect.void,
      ),
    )

    const serializeInput = Effect.fnUntraced(function* (input: unknown) {
      return yield* serializer.serialize(input, maxSerializedInputDataSize)
    })

    const serializeOutput = Effect.fnUntraced(function* (output: unknown) {
      return yield* serializer.serialize(output, maxSerializedOutputDataSize)
    })

    const taskInternal = Effect.fnUntraced(function* <
      TRunInput = undefined,
      TInput = TRunInput,
      TOutput = unknown,
    >(
      taskOptions: TaskOptions<TRunInput, TOutput>,
      validateInputFn?: (
        id: string,
        input: TInput,
      ) => Effect.Effect<TRunInput, DurableExecutionError>,
    ) {
      yield* errorIfShutdown

      const taskOptionsInternal = convertTaskOptionsToOptionsInternal(taskOptions, validateInputFn)
      const taskInternal = yield* addTaskInternal(taskInternalsMap, taskOptionsInternal)
      yield* Effect.logDebug('Added task').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('taskId', taskInternal.id),
        Effect.annotateLogs('taskType', taskInternal.taskType),
      )
      return {
        taskType: 'task',
        id: taskInternal.id,
        retryOptions: taskInternal.retryOptions,
        sleepMsBeforeRun: taskInternal.sleepMsBeforeRun,
        timeoutMs: taskInternal.timeoutMs,
      } as Task<TInput, TOutput, 'task'>
    })

    const task = Effect.fn(function* <TInput = undefined, TOutput = unknown>(
      taskOptions: TaskOptions<TInput, TOutput>,
    ) {
      return yield* taskInternal(taskOptions)
    })

    const sleepingTaskInternal = Effect.fnUntraced(function* <TInput = string, TOutput = unknown>(
      taskOptions: SleepingTaskOptions<TOutput>,
      validateInputFn?: (id: string, input: TInput) => Effect.Effect<string, DurableExecutionError>,
    ) {
      yield* errorIfShutdown

      const taskOptionsInternal = convertSleepingTaskOptionsToOptionsInternal(
        taskOptions,
        validateInputFn,
      )
      const taskInternal = yield* addTaskInternal(taskInternalsMap, taskOptionsInternal)
      yield* Effect.logDebug('Added task').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('taskId', taskInternal.id),
        Effect.annotateLogs('taskType', taskInternal.taskType),
      )
      return {
        taskType: 'sleepingTask',
        id: taskInternal.id,
        retryOptions: taskInternal.retryOptions,
        sleepMsBeforeRun: taskInternal.sleepMsBeforeRun,
        timeoutMs: taskInternal.timeoutMs,
      } as Task<TInput, TOutput, 'sleepingTask'>
    })

    const sleepingTask = Effect.fn(function* <TOutput = unknown>(
      taskOptions: SleepingTaskOptions<TOutput>,
    ) {
      return yield* sleepingTaskInternal<string, TOutput>(taskOptions)
    })

    const parentTaskInternal = Effect.fnUntraced(function* <
      TRunInput = undefined,
      TInput = TRunInput,
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
      TFinalizeTaskRunOutput = unknown,
      TTaskType extends 'parentTask' | 'sequentialTasks' = 'parentTask',
    >(
      taskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
      validateInputFn?: (
        id: string,
        input: TInput,
      ) => Effect.Effect<TRunInput, DurableExecutionError>,
      taskType?: TTaskType,
    ) {
      yield* errorIfShutdown

      const taskOptionsInternal = convertParentTaskOptionsToOptionsInternal(
        taskOptions,
        validateInputFn,
      )
      if (taskType && taskType !== 'parentTask') {
        taskOptionsInternal.taskType = taskType
      }
      const taskInternal = yield* addTaskInternal(taskInternalsMap, taskOptionsInternal)
      yield* Effect.logDebug('Added task').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('taskId', taskInternal.id),
        Effect.annotateLogs('taskType', taskInternal.taskType),
      )
      return {
        taskType: taskType ?? 'parentTask',
        id: taskInternal.id,
        retryOptions: taskInternal.retryOptions,
        sleepMsBeforeRun: taskInternal.sleepMsBeforeRun,
        timeoutMs: taskInternal.timeoutMs,
      } as Task<TInput, TOutput, TTaskType>
    })

    const parentTask = Effect.fn(function* <
      TInput = undefined,
      TRunOutput = unknown,
      TOutput = DefaultParentTaskOutput<TRunOutput>,
      TFinalizeTaskRunOutput = unknown,
    >(taskOptions: ParentTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>) {
      return yield* parentTaskInternal(taskOptions)
    })

    const sequentialTasksInternal = Effect.fnUntraced(function* <
      TSequentialTasks extends ReadonlyArray<AnyTask>,
      TInput = InferTaskInput<TSequentialTasks[0]>,
    >(
      id: string,
      tasks: SequentialTasks<TSequentialTasks>,
      validateInputFn?: (
        id: string,
        input: TInput,
      ) => Effect.Effect<InferTaskInput<TSequentialTasks[0]>, DurableExecutionError>,
    ) {
      type TRunInput = InferTaskInput<TSequentialTasks[0]>
      type TRunOutput = unknown
      type TOutput = InferTaskOutput<LastTaskElementInArray<TSequentialTasks>>
      type TFinalizeTaskRunOutput = unknown

      if (tasks.length === 0) {
        return yield* Effect.fail(DurableExecutionError.nonRetryable('No tasks provided'))
      }

      const firstTask = tasks[0]
      const otherTasks = tasks.slice(1)
      const task = yield* parentTaskInternal<
        TRunInput,
        TInput,
        TRunOutput,
        TOutput,
        TFinalizeTaskRunOutput,
        'sequentialTasks'
      >(
        {
          id,
          timeoutMs: 30_000,
          runParent: (_, input) => {
            return {
              output: undefined,
              children: [childTask(firstTask, input), ...otherTasks.map((task) => childTask(task))],
            }
          },
        },
        validateInputFn,
        'sequentialTasks',
      )

      return task
    })

    const sequentialTasks = Effect.fn(function* <TSequentialTasks extends ReadonlyArray<AnyTask>>(
      id: string,
      tasks: SequentialTasks<TSequentialTasks>,
    ) {
      return yield* sequentialTasksInternal(id, tasks)
    })

    const loopingTaskInternal = Effect.fnUntraced(function* <
      TRunInput = undefined,
      TInput = TRunInput,
      TOutput = unknown,
    >(
      id: string,
      iterationTask: Task<TRunInput, { isDone: false } | { isDone: true; output: TOutput }>,
      maxAttempts: number,
      sleepMsBeforeRun?: number | ((attempt: number) => number),
      validateInputFn?: (
        id: string,
        input: TInput,
      ) => Effect.Effect<TRunInput, DurableExecutionError>,
    ) {
      if (maxAttempts <= 0) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable('Max attempts must be greater than 0'),
        )
      }

      const loopingTaskInner: Task<
        { input: TRunInput; attempt: number },
        { isSuccess: false } | { isSuccess: true; output: TOutput }
      > = yield* parentTask<
        { input: TRunInput; attempt: number },
        { input: TRunInput; attempt: number },
        { isSuccess: false } | { isSuccess: true; output: TOutput },
        { isSuccess: false } | { isSuccess: true; output: TOutput }
      >({
        id: `${id}_inner`,
        timeoutMs: 30_000,
        runParent: (_, input) =>
          Effect.sync(() => {
            return {
              output: input,
              children: [
                childTask(iterationTask, input.input, {
                  sleepMsBeforeRun: (sleepMsBeforeRun && isFunction(sleepMsBeforeRun)
                    ? sleepMsBeforeRun(input.attempt)
                    : sleepMsBeforeRun != null
                      ? sleepMsBeforeRun
                      : iterationTask.sleepMsBeforeRun) as number | undefined,
                }),
              ],
            }
          }),
        finalize: {
          id: `${id}_inner_finalize`,
          timeoutMs: 30_000,
          runParent: (_, { output: input, children }) =>
            Effect.gen(function* () {
              const child = children[0]! as FinishedChildTaskExecution<
                { isDone: false } | { isDone: true; output: TOutput }
              >
              if (child.status !== 'completed') {
                return yield* Effect.fail(
                  DurableExecutionError.nonRetryable(
                    `Iteration task failed [taskId=${iterationTask.id}]: ${child.error.message}`,
                  ),
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
                  childTask(loopingTaskInner, { input: input.input, attempt: input.attempt + 1 }),
                ],
              }
            }),
          finalize: ({ output, children }) =>
            Effect.gen(function* () {
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
                return yield* Effect.fail(DurableExecutionError.nonRetryable(child.error.message))
              }

              return child.output
            }),
        },
      })

      const loopingTask = yield* parentTaskInternal<
        TRunInput,
        TInput,
        undefined,
        { isSuccess: false } | { isSuccess: true; output: TOutput }
      >(
        {
          id,
          timeoutMs: 30_000,
          runParent: (_, input) =>
            Effect.sync(() => {
              return {
                output: undefined,
                children: [childTask(loopingTaskInner, { input, attempt: 0 })],
              }
            }),
          finalize: ({ children }) =>
            Effect.gen(function* () {
              const child = children[0]! as FinishedChildTaskExecution<
                { isSuccess: false } | { isSuccess: true; output: TOutput }
              >
              if (child.status !== 'completed') {
                return yield* Effect.fail(DurableExecutionError.nonRetryable(child.error.message))
              }

              return child.output
            }),
        },
        validateInputFn,
      )
      return loopingTask
    })

    const loopingTask = Effect.fn(function* <TInput = undefined, TOutput = unknown>(
      id: string,
      iterationTask: Task<TInput, { isDone: false } | { isDone: true; output: TOutput }>,
      maxAttempts: number,
      sleepMsBeforeRun?: number | ((attempt: number) => number),
    ) {
      return yield* loopingTaskInternal(id, iterationTask, maxAttempts, sleepMsBeforeRun)
    })

    const validateInputInternal = <TRunInput, TInput>(
      validateInputFn: (
        id: string,
        input: TInput,
      ) => Effect.Effect<TRunInput, DurableExecutionError>,
    ) => {
      return {
        task: <TOutput = unknown>(taskOptions: TaskOptions<TRunInput, TOutput>) =>
          taskInternal(taskOptions, validateInputFn),
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
        ) => parentTaskInternal(parentTaskOptions, validateInputFn),
        sequentialTasks: (<TSequentialTasks extends ReadonlyArray<AnyTask>>(
          id: string,
          tasks: SequentialTasks<TSequentialTasks>,
        ) => {
          return sequentialTasksInternal(
            id,
            tasks,
            validateInputFn as (
              id: string,
              input: TInput,
            ) => Effect.Effect<InferTaskInput<TSequentialTasks[0]>, DurableExecutionError>,
          )
        }) as <TSequentialTasks extends ReadonlyArray<AnyTask>>(
          id: string,
          tasks: SequentialTasks<TSequentialTasks>,
        ) => InferTaskInput<TSequentialTasks[0]> extends TRunInput
          ? Effect.Effect<
              Task<TInput, InferTaskOutput<LastTaskElementInArray<TSequentialTasks>>>,
              DurableExecutionError,
              never
            >
          : never,
        loopingTask: <TOutput = unknown>(
          id: string,
          iterationTask: Task<TRunInput, { isDone: false } | { isDone: true; output: TOutput }>,
          maxAttempts: number,
          sleepMsBeforeRun?: number | ((attempt: number) => number),
        ) => {
          return loopingTaskInternal(
            id,
            iterationTask,
            maxAttempts,
            sleepMsBeforeRun,
            validateInputFn,
          )
        },
      }
    }

    const validateInput = <TRunInput, TInput>(
      validateInputFn: (
        input: TInput,
      ) => TRunInput | Promise<TRunInput> | Effect.Effect<TRunInput, unknown>,
    ) => {
      const finalValidateInputfn = Effect.fnUntraced(function* (id: string, input: TInput) {
        return yield* convertMaybePromiseOrEffectToEffect(() => validateInputFn(input)).pipe(
          Effect.mapError((error) =>
            convertErrorToDurableExecutionError(error, {
              isRetryable: false,
              prefix: `Invalid input to task ${id}`,
            }),
          ),
        )
      })

      return validateInputInternal(finalValidateInputfn)
    }

    const inputSchema = <TInputSchema extends AnySchema>(inputSchema: TInputSchema) => {
      type TRunInput = InferSchemaOutput<TInputSchema>
      type TInput = InferSchemaInput<TInputSchema>

      const finalValidateInputfn = Effect.fnUntraced(function* (id: string, input: TInput) {
        if (isStandardSchema(inputSchema)) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-return
          return yield* convertPromiseToEffect(async () => {
            const validateResult = await inputSchema['~standard'].validate(input)
            if (validateResult.issues != null) {
              throw DurableExecutionError.nonRetryable(
                summarizeStandardSchemaIssues(validateResult.issues),
              )
            }
            // eslint-disable-next-line @typescript-eslint/no-unsafe-return
            return validateResult.value as TRunInput
          }).pipe(
            Effect.mapError((error) =>
              convertErrorToDurableExecutionError(error, {
                isRetryable: false,
                prefix: `Invalid input to task ${id}`,
              }),
            ),
          )
        } else if (isEffectSchema(inputSchema)) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-return
          return (yield* Schema.decodeUnknown(inputSchema)(input).pipe(
            Effect.mapError((error) =>
              convertErrorToDurableExecutionError(error, {
                isRetryable: false,
                prefix: `Invalid input to task ${id}`,
              }),
            ),
          )) as TRunInput
        } else {
          return yield* Effect.fail(DurableExecutionError.nonRetryable('Invalid schema'))
        }
      })

      return validateInputInternal(finalValidateInputfn)
    }

    const getSleepingTaskUniqueIdInternal = Effect.fnUntraced(function* (
      taskId: string,
      input: unknown,
    ) {
      if (input == null) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `A unique id string is required to enqueue a sleeping task [taskId=${taskId}] [input=${input}]`,
          ),
        )
      }
      if (!isString(input)) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `Input must be a unique id string for sleeping task [taskId=${taskId}] [input=${typeof input}]`,
          ),
        )
      }
      if (input.length === 0) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `A non-empty unique id string is required for sleeping task [taskId=${taskId}]`,
          ),
        )
      }
      if (input.length > 255) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `The unique id string must be shorter than 256 characters for sleeping task [taskId=${taskId}] [inputLength=${input.length}]`,
          ),
        )
      }
      return input
    })

    const getTaskExecutionHandleInternal = <TOutput>(taskId: string, executionId: string) => {
      return {
        taskId,
        executionId,
        getExecution: Effect.gen(function* () {
          const execution = yield* storage.getById({ executionId })
          if (!execution) {
            return yield* Effect.fail(
              new DurableExecutionNotFoundError(
                `Task execution not found [executionId=${executionId}]`,
              ),
            )
          }
          return yield* convertTaskExecutionStorageValueToTaskExecution<TOutput>(
            execution,
            serializer,
          )
        }),
        waitAndGetFinishedExecution: Effect.fn(
          ({
            pollingIntervalMs,
          }: {
            pollingIntervalMs?: number
          } = {}) => {
            const resolvedPollingInterval = Duration.millis(
              Math.max(50, pollingIntervalMs && pollingIntervalMs > 0 ? pollingIntervalMs : 2500),
            )
            return Effect.gen(function* () {
              const execution = yield* storage.getById({ executionId })
              if (!execution) {
                return yield* Effect.fail(
                  new DurableExecutionNotFoundError(
                    `Task execution not found [executionId=${executionId}]`,
                  ),
                )
              }

              if (FINISHED_TASK_EXECUTION_STATUSES.includes(execution.status)) {
                return (yield* convertTaskExecutionStorageValueToTaskExecution(
                  execution,
                  serializer,
                )) as FinishedTaskExecution<TOutput>
              }

              yield* Effect.logDebug('Waiting for task to be finished').pipe(
                Effect.annotateLogs('service', 'effect-executor'),
                Effect.annotateLogs('executorId', executorId),
                Effect.annotateLogs('executionId', executionId),
                Effect.annotateLogs('status', execution.status),
              )
              yield* Effect.sleep(resolvedPollingInterval)
              return undefined
            }).pipe(
              Effect.repeat({
                until: (output): output is FinishedTaskExecution<TOutput> => output != null,
              }),
            )
          },
        ),
        cancel: Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis
          yield* storage.updateById(now, {
            executionId,
            filters: { isFinished: false },
            update: {
              status: 'cancelled',
              error: convertDurableExecutionErrorToStorageValue(
                new DurableExecutionCancelledError(),
              ),
              needsPromiseCancellation: true,
            },
          })
          yield* Effect.logDebug('Cancelled task execution').pipe(
            Effect.annotateLogs('service', 'effect-executor'),
            Effect.annotateLogs('executorId', executorId),
            Effect.annotateLogs('executionId', executionId),
          )
        }),
      }
    }

    const enqueueTask = Effect.fnUntraced(function* <TTask extends AnyTask>(
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
      yield* errorIfShutdown

      const task = rest[0]
      const input = rest.length > 1 ? rest[1]! : undefined
      const options = rest.length > 2 ? rest[2]! : undefined
      if (!taskInternalsMap.has(task.id)) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(`Task not found [taskId=${task.id}]`),
        )
      }

      const sleepingTaskUniqueId =
        task.taskType === 'sleepingTask'
          ? yield* getSleepingTaskUniqueIdInternal(task.id, input)
          : undefined
      const executionId = generateTaskExecutionId()
      const validatedEnqueueOptions = yield* validateEnqueueOptions(task.id, options)
      const finalEnqueueOptions = overrideTaskEnqueueOptions(task, validatedEnqueueOptions)
      const now = yield* Clock.currentTimeMillis
      let insertMany = storage.insertMany
      if (options?.taskExecutionsStorageTransaction) {
        insertMany = (requests) =>
          convertMaybePromiseToEffect(() =>
            options.taskExecutionsStorageTransaction!.insertMany(requests),
          )
      }

      yield* insertMany([
        createTaskExecutionStorageValue({
          now,
          taskId: task.id,
          executionId,
          sleepingTaskUniqueId,
          retryOptions: finalEnqueueOptions.retryOptions,
          sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
          timeoutMs: finalEnqueueOptions.timeoutMs,
          areChildrenSequential: task.taskType === 'sequentialTasks',
          input: yield* serializeInput(task.taskType === 'sleepingTask' ? undefined : input),
        }),
      ])

      yield* Effect.logDebug('Enqueued task').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('taskId', task.id),
        Effect.annotateLogs('taskType', task.taskType),
        Effect.annotateLogs('executionId', executionId),
      )
      return getTaskExecutionHandleInternal<InferTaskOutput<TTask>>(task.id, executionId)
    })

    const getTaskExecutionHandle = Effect.fn(function* <TTask extends AnyTask>(
      task: TTask,
      executionId: string,
    ) {
      yield* errorIfShutdown

      if (!taskInternalsMap.has(task.id)) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(`Task not found [taskId=${task.id}]`),
        )
      }

      const execution = yield* storage.getById({ executionId })
      if (!execution) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(
            `Task execution not found [executionId=${executionId}]`,
          ),
        )
      }
      if (execution.taskId !== task.id) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(
            `Task execution belongs to another task [providedTaskId=${task.id}] [taskId=${execution.taskId}] [executionId=${executionId}]`,
          ),
        )
      }

      return getTaskExecutionHandleInternal<InferTaskOutput<TTask>>(task.id, executionId)
    })

    const wakeupSleepingTaskExecution = Effect.fn(function* <
      TTask extends Task<unknown, unknown, 'sleepingTask'>,
    >(
      task: TTask,
      sleepingTaskUniqueId: string,
      options: WakeupSleepingTaskExecutionOptions<InferTaskOutput<TTask>>,
    ) {
      yield* errorIfShutdown

      type TOutput = InferTaskOutput<TTask>

      if (task.taskType !== 'sleepingTask') {
        return yield* Effect.fail(
          new DurableExecutionError(`Task is not a sleeping task [taskId=${task.id}]`),
        )
      }

      const execution = yield* storage.getBySleepingTaskUniqueId({ sleepingTaskUniqueId })
      if (!execution) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(
            `Sleeping task execution not found [sleepingTaskUniqueId=${sleepingTaskUniqueId}]`,
          ),
        )
      }
      if (execution.taskId !== task.id) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(
            `Sleeping task execution belongs to another task [providedTaskId=${task.id}] [sleepingTaskId=${execution.taskId}] [sleepingTaskUniqueId=${sleepingTaskUniqueId}]`,
          ),
        )
      }
      if (execution.isFinished) {
        return (yield* convertTaskExecutionStorageValueToTaskExecution(
          execution,
          serializer,
        )) as FinishedTaskExecution<TOutput>
      }

      const update: TaskExecutionStorageUpdateInternal = {
        status: options.status,
      }
      if (options.status === 'completed') {
        update.output = yield* serializeOutput(options.output)
      } else if (options.status === 'failed') {
        update.error = convertDurableExecutionErrorToStorageValue(
          DurableExecutionError.nonRetryable(getErrorMessage(options.error)),
        )
      } else {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            // @ts-expect-error - This is safe
            `Invalid status for sleeping task execution [executionId=${execution.executionId}] [status=${options.status}]`,
          ),
        )
      }

      const now = yield* Clock.currentTimeMillis
      yield* storage.updateById(now, {
        executionId: execution.executionId,
        filters: { isSleepingTask: true, isFinished: false },
        update,
      })
      yield* Effect.logDebug('Woken up sleeping task').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('taskId', task.id),
        Effect.annotateLogs('taskType', task.taskType),
        Effect.annotateLogs('executionId', execution.executionId),
      )

      const finishedExecution = yield* storage.getById({ executionId: execution.executionId })
      if (!finishedExecution) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(
            `Sleeping task execution not found [executionId=${execution.executionId}]`,
          ),
        )
      }
      if (!finishedExecution.isFinished) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `Task execution is not a sleeping task execution [executionId=${execution.executionId}]`,
          ),
        )
      }
      return (yield* convertTaskExecutionStorageValueToTaskExecution(
        finishedExecution,
        serializer,
      )) as FinishedTaskExecution<TOutput>
    })

    const processReadyTaskExecutionsIteration = Effect.gen(function* () {
      const currConcurrentTaskExecutions = runningTaskExecutionsMap.size
      if (currConcurrentTaskExecutions >= maxConcurrentTaskExecutions) {
        yield* Effect.logDebug(
          'At max concurrent task execution limit, skipping processing ready task executions',
        ).pipe(
          Effect.annotateLogs('service', 'effect-executor'),
          Effect.annotateLogs('executorId', executorId),
          Effect.annotateLogs('currConcurrentTaskExecutions', currConcurrentTaskExecutions),
          Effect.annotateLogs('maxConcurrentTaskExecutions', maxConcurrentTaskExecutions),
        )
        return { hasMore: false }
      }

      const availableLimit = maxConcurrentTaskExecutions - currConcurrentTaskExecutions
      const batchLimit = Math.min(maxTaskExecutionsPerBatch, availableLimit)
      const now = yield* Clock.currentTimeMillis
      const expiresAt = now + expireLeewayMs
      const executions = yield* storage.updateByStatusAndStartAtLessThanAndReturn(now, {
        status: 'ready',
        startAtLessThan: now,
        update: {
          executorId,
          status: 'running',
        },
        updateExpiresAtWithStartedAt: expiresAt,
        limit: batchLimit,
      })

      yield* Effect.logDebug('Processing ready task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('executionsCount', executions.length),
        Effect.annotateLogs('currConcurrentTaskExecutions', currConcurrentTaskExecutions),
        Effect.annotateLogs('maxConcurrentTaskExecutions', maxConcurrentTaskExecutions),
        Effect.annotateLogs('batchLimit', batchLimit),
      )
      if (executions.length === 0) {
        return { hasMore: false }
      }

      yield* Effect.forEach(executions, forkTaskExecution, {
        concurrency: 'unbounded',
        discard: true,
      })

      return { hasMore: executions.length > 0 }
    })

    const forkTaskExecution = Effect.fn(function* (execution: TaskExecutionStorageValue) {
      const executionId = execution.executionId
      if (runningTaskExecutionsMap.has(executionId)) {
        return
      }

      runningTaskExecutionsMap.set(executionId, undefined)
      yield* fiberPool
        .fork(
          runTaskExecution(execution).pipe(
            Effect.catchAll((error) =>
              Effect.gen(function* () {
                const durableExecutionError = convertErrorToDurableExecutionError(error, {
                  isRetryable: false,
                  prefix: `Error in running task execution [executionId=${executionId}]`,
                })
                const now = yield* Clock.currentTimeMillis
                yield* storage
                  .updateById(
                    now,
                    {
                      executionId: execution.executionId,
                      filters: { status: 'running' },
                      update: {
                        status: 'failed',
                        error: convertDurableExecutionErrorToStorageValue(durableExecutionError),
                      },
                    },
                    execution,
                  )
                  .pipe(
                    Effect.tapError((error) =>
                      Effect.logError('Error in updating task execution').pipe(
                        Effect.annotateLogs('service', 'effect-executor'),
                        Effect.annotateLogs('executorId', executorId),
                        Effect.annotateLogs('executionId', executionId),
                        Effect.annotateLogs('error', error),
                      ),
                    ),
                    Effect.ignore,
                  )
                yield* Effect.logError('Error in running task execution').pipe(
                  Effect.annotateLogs('service', 'effect-executor'),
                  Effect.annotateLogs('executorId', executorId),
                  Effect.annotateLogs('executionId', executionId),
                  Effect.annotateLogs('error', error),
                )
              }),
            ),
            Effect.ensuring(Effect.sync(() => runningTaskExecutionsMap.delete(executionId))),
          ),
        )
        .pipe(
          Effect.tap((fiber) => {
            if (runningTaskExecutionsMap.has(executionId)) {
              runningTaskExecutionsMap.set(executionId, fiber)
            }
          }),
          Effect.tapError(() =>
            Effect.sync(() => {
              runningTaskExecutionsMap.delete(executionId)
            }),
          ),
          Effect.ignore,
        )
    })

    const runTaskExecution = Effect.fn(function* (execution: TaskExecutionStorageValue) {
      const taskRunCtx: Omit<TaskRunContext, 'abortSignal'> = {
        root: execution.root,
        parent: execution.parent,
        taskId: execution.taskId,
        executionId: execution.executionId,
        shutdownSignal: shutdownController.signal,
        attempt: execution.retryAttempts,
        prevError: execution.error,
      }

      const taskInternal = taskInternalsMap.get(execution.taskId)
      if (!taskInternal) {
        return yield* Effect.fail(
          new DurableExecutionNotFoundError(`Task not found [taskId=${execution.taskId}]`),
        )
      }

      const result = yield* runParentWithTimeout(
        taskInternal,
        taskRunCtx,
        yield* serializer.deserialize(execution.input),
        execution.timeoutMs,
      ).pipe(Effect.either)
      return yield* Either.isLeft(result)
        ? onRunTaskExecutionWithContextError(execution, result.left)
        : onRunTaskExecutionWithContextResult(execution, taskInternal, result.right).pipe(
            Effect.tapError((error) => onRunTaskExecutionWithContextError(execution, error)),
          )
    })

    const onRunTaskExecutionWithContextResult = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
      taskInternal: TaskInternal,
      result: {
        output: unknown
        children: ReadonlyArray<ChildTask>
      },
    ) {
      const runOutput = result.output
      const childrenTasks = result.children

      if (childrenTasks.length > maxChildrenPerTaskExecution) {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `Parent task tried to spawn more than max children tasks [taskId=${taskInternal.id}] [attempted=${childrenTasks.length}] [max=${maxChildrenPerTaskExecution}]`,
          ),
        )
      }

      const now = yield* Clock.currentTimeMillis
      if (childrenTasks.length === 0) {
        if (taskInternal.finalize) {
          if (isFunction(taskInternal.finalize)) {
            const finalizeFn = taskInternal.finalize as (
              input: DefaultParentTaskOutput,
            ) => Effect.Effect<unknown, DurableExecutionError>
            const finalizeOutputEither = yield* finalizeFn({
              output: runOutput,
              children: [],
            }).pipe(
              Effect.mapError((error) =>
                convertDurableExecutionErrorToStorageValue(
                  DurableExecutionError.nonRetryable(getErrorMessage(error)),
                ),
              ),
              Effect.either,
            )

            yield* Either.isLeft(finalizeOutputEither)
              ? storage.updateById(
                  now,
                  {
                    executionId: execution.executionId,
                    filters: { status: 'running' },
                    update: {
                      status: 'finalize_failed',
                      error: finalizeOutputEither.left,
                      waitingForFinalizeStartedAt: now,
                      children: [],
                    },
                  },
                  execution,
                )
              : storage.updateById(
                  now,
                  {
                    executionId: execution.executionId,
                    filters: { status: 'running' },
                    update: {
                      status: 'completed',
                      output: yield* serializeOutput(finalizeOutputEither.right),
                      waitingForFinalizeStartedAt: now,
                      children: [],
                    },
                  },
                  execution,
                )
          } else {
            const finalizeTaskInternal = taskInternal.finalize as TaskInternal
            if (finalizeTaskInternal.taskType === 'sleepingTask') {
              return yield* Effect.fail(
                DurableExecutionError.nonRetryable(
                  `Finalize task cannot be a sleeping task [taskId=${finalizeTaskInternal.id}]`,
                ),
              )
            }

            const finalizeTaskInput = {
              output: runOutput,
              children: [],
            }
            const executionId = generateTaskExecutionId()
            yield* storage.updateByIdAndInsertChildrenIfUpdated(
              now,
              {
                executionId: execution.executionId,
                filters: { status: 'running' },
                update: {
                  status: 'waiting_for_finalize',
                  waitingForChildrenStartedAt: now,
                  children: [],
                  finalize: {
                    taskId: finalizeTaskInternal.id,
                    executionId,
                  },
                },
                childrenTaskExecutionsToInsertIfAnyUpdated: [
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
                    retryOptions: finalizeTaskInternal.retryOptions,
                    sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
                    timeoutMs: finalizeTaskInternal.timeoutMs,
                    areChildrenSequential: finalizeTaskInternal.taskType === 'sequentialTasks',
                    input: yield* serializeInput(finalizeTaskInput),
                  }),
                ],
              },
              execution,
            )
          }
        } else {
          const output =
            taskInternal.taskType === 'parentTask' || taskInternal.taskType === 'sequentialTasks'
              ? yield* serializeOutput({
                  output: runOutput,
                  children: [],
                })
              : yield* serializeOutput(runOutput)
          yield* storage.updateById(
            now,
            {
              executionId: execution.executionId,
              filters: { status: 'running' },
              update: {
                status: 'completed',
                output,
                children: [],
              },
            },
            execution,
          )
        }
      } else {
        const childrenStorageValues: Array<TaskExecutionStorageValue> = []
        const children: Array<TaskExecutionSummary> = []

        for (const [index, child] of childrenTasks.entries()) {
          const childTaskInternal = taskInternalsMap.get(child.task.id)
          if (!childTaskInternal) {
            yield* Effect.logWarning('Child task not found').pipe(
              Effect.annotateLogs('service', 'effect-executor'),
              Effect.annotateLogs('executorId', executorId),
              Effect.annotateLogs('parentTaskId', execution.taskId),
              Effect.annotateLogs('parentExecutionId', execution.executionId),
              Effect.annotateLogs('childTaskId', child.task.id),
              Effect.annotateLogs('childIndex', index),
            )
            return yield* Effect.fail(
              new DurableExecutionNotFoundError(`Child task not found [taskId=${child.task.id}]`),
            )
          }

          if (taskInternal.taskType !== 'sequentialTasks' || index === 0) {
            const isChildSleepingTask = childTaskInternal.taskType === 'sleepingTask'
            const childSleepingTaskUniqueId = isChildSleepingTask
              ? yield* getSleepingTaskUniqueIdInternal(child.task.id, child.input)
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
                sleepingTaskUniqueId: childSleepingTaskUniqueId,
                retryOptions: finalEnqueueOptions.retryOptions,
                sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
                timeoutMs: finalEnqueueOptions.timeoutMs,
                areChildrenSequential: childTaskInternal.taskType === 'sequentialTasks',
                input: yield* serializeInput(isChildSleepingTask ? undefined : child.input),
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

        yield* storage.updateByIdAndInsertChildrenIfUpdated(
          now,
          {
            executionId: execution.executionId,
            filters: { status: 'running' },
            update: {
              status: 'waiting_for_children',
              runOutput: yield* serializeOutput(runOutput),
              children: children,
              activeChildrenCount: childrenStorageValues.length,
            },
            childrenTaskExecutionsToInsertIfAnyUpdated: childrenStorageValues,
          },
          execution,
        )
      }
    })

    const onRunTaskExecutionWithContextError = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
      error: unknown,
    ) {
      const durableExecutionError = convertErrorToDurableExecutionError(error)

      const now = yield* Clock.currentTimeMillis
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
        if (execution.retryAttempts > 0 && delayMs > 0) {
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

      yield* storage.updateById(
        now,
        { executionId: execution.executionId, filters: { status: 'running' }, update },
        execution,
      )
    })

    const processOnChildrenFinishedTaskExecutionsIteration = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const expiresAt = now + 2 * 60 * 1000 + expireLeewayMs // 2 minutes + expireLeewayMs
      const executions =
        yield* storage.updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
          now,
          {
            status: 'waiting_for_children',
            onChildrenFinishedProcessingStatus: 'idle',
            update: {
              onChildrenFinishedProcessingStatus: 'processing',
              onChildrenFinishedProcessingExpiresAt: expiresAt,
            },
            limit: processOnChildrenFinishedTaskExecutionsBatchSize,
          },
        )

      yield* Effect.logDebug('Processing on children finished task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('executionsCount', executions.length),
        Effect.annotateLogs(
          'processOnChildrenFinishedTaskExecutionsBatchSize',
          processOnChildrenFinishedTaskExecutionsBatchSize,
        ),
      )
      if (executions.length === 0) {
        return { hasMore: false }
      }

      const markExecutionAsFailed = Effect.fn(function* (
        execution: TaskExecutionStorageValue,
        error: DurableExecutionError,
        status: 'failed' | 'finalize_failed' = 'failed',
      ) {
        const now = yield* Clock.currentTimeMillis
        yield* storage.updateById(
          now,
          {
            executionId: execution.executionId,
            filters: { status: 'waiting_for_children' },
            update: {
              status,
              error: convertDurableExecutionErrorToStorageValue(error),
              onChildrenFinishedProcessingStatus: 'processed',
            },
          },
          execution,
        )
      })

      yield* fiberPool.fork(
        Effect.forEach(
          executions,
          (execution) =>
            processOnChildrenFinishedTaskExecutionsSingleExecution(
              execution,
              markExecutionAsFailed,
            ),
          {
            concurrency: 'unbounded',
            discard: true,
          },
        ),
      )

      return { hasMore: executions.length > 0 }
    })

    const processOnChildrenFinishedTaskExecutionsSingleExecution = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
      markExecutionAsFailed: (
        execution: TaskExecutionStorageValue,
        error: DurableExecutionError,
        status?: 'failed' | 'finalize_failed',
      ) => Effect.Effect<void, DurableExecutionError, never>,
    ) {
      const taskInternal = taskInternalsMap.get(execution.taskId)
      if (!taskInternal) {
        yield* Effect.logWarning(
          'Parent task not found during on-children-finished processing',
        ).pipe(
          Effect.annotateLogs('service', 'effect-executor'),
          Effect.annotateLogs('executorId', executorId),
          Effect.annotateLogs('taskId', execution.taskId),
          Effect.annotateLogs('executionId', execution.executionId),
        )
        return yield* markExecutionAsFailed(
          execution,
          new DurableExecutionNotFoundError(`Task not found [taskId=${execution.taskId}]`),
        )
      }

      if (taskInternal.taskType === 'sequentialTasks') {
        return yield* processOnChildrenFinishedTaskExecutionsSingleSequentialExecution(
          execution,
          markExecutionAsFailed,
        )
      }

      let finalizeFn:
        | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError>)
        | undefined
      let finalizeTaskInternal: TaskInternal | undefined
      if (taskInternal.finalize) {
        if (isFunction(taskInternal.finalize)) {
          finalizeFn = taskInternal.finalize as (
            input: DefaultParentTaskOutput,
          ) => Effect.Effect<unknown, DurableExecutionError>
        } else {
          finalizeTaskInternal = taskInternal.finalize as TaskInternal
          if (finalizeTaskInternal?.taskType === 'sleepingTask') {
            return yield* markExecutionAsFailed(
              execution,
              new DurableExecutionNotFoundError(
                `Finalize task cannot be a sleeping task [parentTaskId=${execution.taskId}] [finalizeTaskId=${finalizeTaskInternal.id}]`,
              ),
            )
          }
        }
      }

      let childrenTaskExecutionsForFinalize: Array<FinishedChildTaskExecution> = []
      if (execution.children) {
        let childrenTaskExecutions = yield* storage.getByParentExecutionId({
          parentExecutionId: execution.executionId,
        })
        const childrenTaskExecutionsMap = new Map(
          childrenTaskExecutions.map((child) => [child.executionId, child]),
        )
        childrenTaskExecutions = execution.children
          .map((child) => childrenTaskExecutionsMap.get(child.executionId))
          .filter(Boolean) as Array<TaskExecutionStorageValue>
        if (childrenTaskExecutions.length !== execution.children.length) {
          return yield* markExecutionAsFailed(
            execution,
            new DurableExecutionNotFoundError(
              `Some children task executions not found [parentTaskId=${execution.taskId}] [notFound=${execution.children.length - childrenTaskExecutions.length}] [total=${execution.children.length}]`,
            ),
          )
        }

        childrenTaskExecutionsForFinalize = yield* Effect.forEach(
          childrenTaskExecutions,
          // eslint-disable-next-line unicorn/no-array-method-this-argument
          (child) =>
            Effect.gen(function* () {
              return {
                taskId: child.taskId,
                executionId: child.executionId,
                status: child.status,
                output:
                  child.status === 'completed' && child.output
                    ? yield* serializer.deserialize(child.output)
                    : undefined,
                error:
                  child.status !== 'completed'
                    ? child.error ||
                      convertDurableExecutionErrorToStorageValue(
                        DurableExecutionError.nonRetryable(
                          `Unknown child task execution error [childTaskId=${child.taskId}]`,
                        ),
                      )
                    : undefined,
              } as FinishedChildTaskExecution
            }),
        )
      }

      if (!finalizeFn && !finalizeTaskInternal) {
        const now = yield* Clock.currentTimeMillis
        yield* storage.updateById(
          now,
          {
            executionId: execution.executionId,
            filters: { status: 'waiting_for_children' },
            update: {
              status: 'completed',
              output:
                taskInternal.taskType === 'parentTask'
                  ? yield* serializeOutput({
                      output: execution.runOutput
                        ? yield* serializer.deserialize(execution.runOutput)
                        : undefined,
                      children: childrenTaskExecutionsForFinalize,
                    })
                  : execution.runOutput,
              onChildrenFinishedProcessingStatus: 'processed',
            },
          },
          execution,
        )
        return
      }

      const finalizeTaskInput = {
        output: execution.runOutput
          ? yield* serializer.deserialize(execution.runOutput)
          : undefined,
        children: childrenTaskExecutionsForFinalize,
      }
      if (finalizeFn) {
        const finalizeOutputEither = yield* finalizeFn(finalizeTaskInput).pipe(
          Effect.mapError((error) =>
            convertDurableExecutionErrorToStorageValue(
              DurableExecutionError.nonRetryable(getErrorMessage(error)),
            ),
          ),
          Effect.either,
        )

        const now = yield* Clock.currentTimeMillis
        yield* Either.isLeft(finalizeOutputEither)
          ? storage.updateById(
              now,
              {
                executionId: execution.executionId,
                filters: { status: 'waiting_for_children' },
                update: {
                  status: 'finalize_failed',
                  error: finalizeOutputEither.left,
                  waitingForFinalizeStartedAt: now,
                  onChildrenFinishedProcessingStatus: 'processed',
                },
              },
              execution,
            )
          : storage.updateById(
              now,
              {
                executionId: execution.executionId,
                filters: { status: 'waiting_for_children' },
                update: {
                  status: 'completed',
                  output: yield* serializeOutput(finalizeOutputEither.right),
                  waitingForFinalizeStartedAt: now,
                  onChildrenFinishedProcessingStatus: 'processed',
                },
              },
              execution,
            )
        return
      }

      if (!finalizeTaskInternal) {
        return
      }

      const executionId = generateTaskExecutionId()
      const now = yield* Clock.currentTimeMillis
      yield* storage.updateByIdAndInsertChildrenIfUpdated(
        now,
        {
          executionId: execution.executionId,
          filters: { status: 'waiting_for_children' },
          update: {
            status: 'waiting_for_finalize',
            finalize: {
              taskId: finalizeTaskInternal.id,
              executionId,
            },
            onChildrenFinishedProcessingStatus: 'processed',
          },
          childrenTaskExecutionsToInsertIfAnyUpdated: [
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
              retryOptions: finalizeTaskInternal.retryOptions,
              sleepMsBeforeRun: finalizeTaskInternal.sleepMsBeforeRun,
              timeoutMs: finalizeTaskInternal.timeoutMs,
              areChildrenSequential: finalizeTaskInternal.taskType === 'sequentialTasks',
              input: yield* serializeInput(finalizeTaskInput),
            }),
          ],
        },
        execution,
      )
    })

    const processOnChildrenFinishedTaskExecutionsSingleSequentialExecution = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
      markExecutionAsFailed: (
        execution: TaskExecutionStorageValue,
        error: DurableExecutionError,
        status?: 'failed' | 'finalize_failed',
      ) => Effect.Effect<void, DurableExecutionError, never>,
    ) {
      const firstDummyChildIdx = (execution.children || []).findIndex(
        (child) => child.executionId === 'dummy',
      )
      if (firstDummyChildIdx === 0) {
        return yield* markExecutionAsFailed(
          execution,
          DurableExecutionError.nonRetryable(
            `Sequential tasks is in an invalid state [taskId=${execution.taskId}]`,
          ),
        )
      }

      if (firstDummyChildIdx > 0) {
        const firstDummyChild = execution.children![firstDummyChildIdx]!
        const childTaskInternal = taskInternalsMap.get(firstDummyChild.taskId)
        if (!childTaskInternal) {
          yield* Effect.logWarning('Child task not found during sequential processing').pipe(
            Effect.annotateLogs('service', 'effect-executor'),
            Effect.annotateLogs('executorId', executorId),
            Effect.annotateLogs('parentTaskId', execution.taskId),
            Effect.annotateLogs('executionId', execution.executionId),
            Effect.annotateLogs('childTaskId', firstDummyChild.taskId),
            Effect.annotateLogs('childIndex', firstDummyChildIdx),
          )
          return yield* markExecutionAsFailed(
            execution,
            new DurableExecutionNotFoundError(
              `Child task not found [taskId=${firstDummyChild.taskId}]`,
            ),
          )
        }

        const prevChildTaskExecution = yield* storage.getById({
          executionId: execution.children![firstDummyChildIdx - 1]!.executionId,
        })
        if (!prevChildTaskExecution) {
          yield* Effect.logWarning(
            'Previous child task execution not found during sequential execution processing',
          ).pipe(
            Effect.annotateLogs('service', 'effect-executor'),
            Effect.annotateLogs('executorId', executorId),
            Effect.annotateLogs('parentTaskId', execution.taskId),
            Effect.annotateLogs('executionId', execution.executionId),
            Effect.annotateLogs('missingChildIndex', firstDummyChildIdx - 1),
          )
          return yield* markExecutionAsFailed(
            execution,
            new DurableExecutionNotFoundError(
              `Sequential task not found [parentTaskId=${execution.taskId}] [index=${firstDummyChildIdx - 1}]`,
            ),
          )
        }
        if (prevChildTaskExecution.status !== 'completed') {
          return yield* markExecutionAsFailed(
            execution,
            DurableExecutionError.nonRetryable(
              `Sequential task failed [parentTaskId=${execution.taskId}] [index=${firstDummyChildIdx - 1}]: ${prevChildTaskExecution.error?.message || 'Unknown error'}`,
            ),
            'finalize_failed',
          )
        }

        const executionId = generateTaskExecutionId()
        firstDummyChild.executionId = executionId
        const isChildSleepingTask = childTaskInternal.taskType === 'sleepingTask'
        const childSleepingTaskUniqueId = isChildSleepingTask
          ? yield* getSleepingTaskUniqueIdInternal(
              childTaskInternal.id,
              yield* serializer.deserialize(prevChildTaskExecution.output!),
            )
          : undefined
        const now = yield* Clock.currentTimeMillis
        yield* storage.updateByIdAndInsertChildrenIfUpdated(
          now,
          {
            executionId: execution.executionId,
            filters: { status: 'waiting_for_children' },
            update: {
              children: execution.children!,
              activeChildrenCount: 1,
              onChildrenFinishedProcessingStatus: 'idle',
            },
            childrenTaskExecutionsToInsertIfAnyUpdated: [
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
                sleepingTaskUniqueId: childSleepingTaskUniqueId,
                retryOptions: childTaskInternal.retryOptions,
                sleepMsBeforeRun: childTaskInternal.sleepMsBeforeRun,
                timeoutMs: childTaskInternal.timeoutMs,
                areChildrenSequential: childTaskInternal.taskType === 'sequentialTasks',
                input: isChildSleepingTask
                  ? yield* serializeInput(undefined)
                  : prevChildTaskExecution.output!,
              }),
            ],
          },
          execution,
        )
      } else {
        const prevChildTaskExecution = yield* storage.getById({
          executionId: execution.children![execution.children!.length - 1]!.executionId,
        })
        if (!prevChildTaskExecution) {
          yield* Effect.logWarning(
            'Previous child task execution not found for final sequential execution result',
          ).pipe(
            Effect.annotateLogs('service', 'effect-executor'),
            Effect.annotateLogs('executorId', executorId),
            Effect.annotateLogs('parentTaskId', execution.taskId),
            Effect.annotateLogs('executionId', execution.executionId),
            Effect.annotateLogs('missingChildIndex', execution.children!.length - 1),
          )
          return yield* markExecutionAsFailed(
            execution,
            new DurableExecutionNotFoundError(
              `Sequential task not found [parentTaskId=${execution.taskId}] [index=${execution.children!.length - 1}]`,
            ),
          )
        }
        if (prevChildTaskExecution.status === 'completed') {
          const now = yield* Clock.currentTimeMillis
          yield* storage.updateById(
            now,
            {
              executionId: execution.executionId,
              filters: { status: 'waiting_for_children' },
              update: {
                status: 'completed',
                output: prevChildTaskExecution.output,
                onChildrenFinishedProcessingStatus: 'processed',
              },
            },
            execution,
          )
        } else {
          return yield* markExecutionAsFailed(
            execution,
            DurableExecutionError.nonRetryable(
              `Sequential task failed [parentTaskId=${execution.taskId}] [index=${execution.children!.length - 1}]: ${prevChildTaskExecution.error?.message || 'Unknown error'}`,
            ),
            'finalize_failed',
          )
        }
      }
    })

    const markFinishedTaskExecutionsAsCloseStatusReadyIteration = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const updatedCount =
        yield* storage.updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(now, {
          isFinished: true,
          closeStatus: 'idle',
          update: {
            closeStatus: 'ready',
          },
          limit: markFinishedTaskExecutionsAsCloseStatusReadyBatchSize,
        })

      yield* Effect.logDebug('Marking finished task executions as close status ready').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('updatedCount', updatedCount),
        Effect.annotateLogs(
          'markFinishedTaskExecutionsAsCloseStatusReadyBatchSize',
          markFinishedTaskExecutionsAsCloseStatusReadyBatchSize,
        ),
      )
      return { hasMore: updatedCount > 0 }
    })

    const closeFinishedTaskExecutionsIteration = Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const expiresAt = now + 2 * 60 * 1000 + expireLeewayMs // 2 minutes + expireLeewayMs
      const executions = yield* storage.updateByCloseStatusAndReturn(now, {
        closeStatus: 'ready',
        update: {
          closeStatus: 'closing',
          closeExpiresAt: expiresAt,
        },
        limit: closeFinishedTaskExecutionsBatchSize,
      })

      yield* Effect.logDebug('Closing finished task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('executionsCount', executions.length),
        Effect.annotateLogs(
          'closeFinishedTaskExecutionsBatchSize',
          closeFinishedTaskExecutionsBatchSize,
        ),
      )
      if (executions.length === 0) {
        return { hasMore: false }
      }

      yield* fiberPool.fork(
        Effect.forEach(executions, closeFinishedTaskExecutionsSingleExecution, {
          concurrency: 'unbounded',
          discard: true,
        }),
      )

      return { hasMore: executions.length > 0 }
    })

    const closeFinishedTaskExecutionsSingleExecution = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
    ) {
      execution.closeStatus = 'closing'
      let now = yield* Clock.currentTimeMillis
      yield* Effect.all([
        closeFinishedTaskExecutionParent(execution),
        closeFinishedTaskExecutionChildren(execution),
      ])

      now = yield* Clock.currentTimeMillis
      yield* storage.updateById(
        now,
        {
          executionId: execution.executionId,
          filters: { isFinished: true },
          update: {
            closeStatus: 'closed',
          },
        },
        execution,
      )
    })

    const closeFinishedTaskExecutionParent = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
    ) {
      if (!execution.parent?.isFinalizeOfParent) {
        return
      }

      const now = yield* Clock.currentTimeMillis
      yield* execution.status === 'completed'
        ? storage.updateById(now, {
            executionId: execution.parent.executionId,
            filters: { status: 'waiting_for_finalize' },
            update: {
              status: 'completed',
              output: execution.output ?? undefined,
            },
          })
        : storage.updateById(now, {
            executionId: execution.parent.executionId,
            filters: {
              status: 'waiting_for_finalize',
            },
            update: {
              status: 'finalize_failed',
              error:
                execution.error ||
                convertDurableExecutionErrorToStorageValue(
                  DurableExecutionError.nonRetryable(
                    `Finalize task failed with an unknown error [parentTaskId=${execution.parent.taskId}] [finalizeTaskId=${execution.taskId}] [finalizeExecutionId=${execution.executionId}]`,
                  ),
                ),
            },
          })
    })

    const closeFinishedTaskExecutionChildren = Effect.fn(function* (
      execution: TaskExecutionStorageValue,
    ) {
      if (
        execution.status === 'completed' ||
        ((!execution.children || execution.children.length === 0) && !execution.finalize)
      ) {
        return
      }

      const now = yield* Clock.currentTimeMillis
      yield* storage.updateByParentExecutionIdAndIsFinished(now, {
        parentExecutionId: execution.executionId,
        isFinished: false,
        update: {
          status: 'cancelled',
          error: convertDurableExecutionErrorToStorageValue(
            new DurableExecutionCancelledError(
              `Parent task failed [parentTaskId=${execution.taskId}] [parentExecutionId=${execution.executionId}]: ${execution.error?.message || 'Unknown error'}`,
            ),
          ),
          needsPromiseCancellation: true,
        },
      })
    })

    const cancelsNeedPromiseCancellationTaskExecutionsIteration = Effect.gen(function* () {
      if (runningTaskExecutionsMap.size === 0) {
        return { hasMore: false }
      }

      const now = yield* Clock.currentTimeMillis
      const executions = yield* storage.updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
        now,
        {
          executorId,
          needsPromiseCancellation: true,
          update: {
            needsPromiseCancellation: false,
          },
          limit: cancelNeedsPromiseCancellationTaskExecutionsBatchSize,
        },
      )

      yield* Effect.logDebug('Cancelling task executions that need promise cancellation').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('executionsCount', executions.length),
        Effect.annotateLogs(
          'cancelNeedsPromiseCancellationTaskExecutionsBatchSize',
          cancelNeedsPromiseCancellationTaskExecutionsBatchSize,
        ),
      )
      if (executions.length === 0) {
        return { hasMore: false }
      }

      const processExecution = Effect.fn(function* (execution: TaskExecutionStorageValue) {
        const fiber = runningTaskExecutionsMap.get(execution.executionId)
        if (fiber) {
          try {
            yield* Fiber.interrupt(fiber)
          } catch (error) {
            yield* Effect.logError('Error in cancelling task execution').pipe(
              Effect.annotateLogs('service', 'effect-executor'),
              Effect.annotateLogs('executorId', executorId),
              Effect.annotateLogs('executionId', execution.executionId),
              Effect.annotateLogs('error', error),
            )
          }
          runningTaskExecutionsMap.delete(execution.executionId)
        }
      })

      yield* Effect.forEach(executions, processExecution, {
        concurrency: 'unbounded',
        discard: true,
      })
      return { hasMore: executions.length > 0 }
    })

    const retryExpiredTaskExecutionsIteration = Effect.gen(function* () {
      let now = yield* Clock.currentTimeMillis
      const updatedCount1 = yield* storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(
        now,
        {
          status: 'running',
          isSleepingTask: false,
          expiresAtLessThan: now,
          update: {
            status: 'ready',
            error: convertDurableExecutionErrorToStorageValue(
              DurableExecutionError.nonRetryable('Task execution expired'),
            ),
            startAt: now,
          },
          limit: retryExpiredTaskExecutionsBatchSize,
        },
      )

      yield* Effect.logDebug('Retrying expired running task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('updatedCount', updatedCount1),
        Effect.annotateLogs(
          'retryExpiredTaskExecutionsBatchSize',
          retryExpiredTaskExecutionsBatchSize,
        ),
      )

      now = yield* Clock.currentTimeMillis
      const updatedCount2 = yield* storage.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan(
        now,
        {
          status: 'running',
          isSleepingTask: true,
          expiresAtLessThan: now,
          update: {
            status: 'timed_out',
            error: convertDurableExecutionErrorToStorageValue(new DurableExecutionTimedOutError()),
            startAt: now,
          },
          limit: retryExpiredTaskExecutionsBatchSize,
        },
      )

      yield* Effect.logDebug('Timing out expired sleeping task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('updatedCount', updatedCount2),
        Effect.annotateLogs(
          'retryExpiredTaskExecutionsBatchSize',
          retryExpiredTaskExecutionsBatchSize,
        ),
      )

      const updatedCount3 = yield* storage.updateByOnChildrenFinishedProcessingExpiresAtLessThan(
        now,
        {
          onChildrenFinishedProcessingExpiresAtLessThan: now,
          update: {
            onChildrenFinishedProcessingStatus: 'idle',
          },
          limit: retryExpiredTaskExecutionsBatchSize,
        },
      )

      yield* Effect.logDebug(
        'Retrying expired on children finished processing task executions',
      ).pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('updatedCount', updatedCount3),
        Effect.annotateLogs(
          'retryExpiredTaskExecutionsBatchSize',
          retryExpiredTaskExecutionsBatchSize,
        ),
      )

      const updatedCount4 = yield* storage.updateByCloseExpiresAtLessThan(now, {
        closeExpiresAtLessThan: now,
        update: {
          closeStatus: 'ready',
        },
        limit: retryExpiredTaskExecutionsBatchSize,
      })

      yield* Effect.logDebug('Retrying expired close task executions').pipe(
        Effect.annotateLogs('service', 'effect-executor'),
        Effect.annotateLogs('executorId', executorId),
        Effect.annotateLogs('updatedCount', updatedCount4),
        Effect.annotateLogs(
          'retryExpiredTaskExecutionsBatchSize',
          retryExpiredTaskExecutionsBatchSize,
        ),
      )

      return {
        hasMore:
          updatedCount1 + updatedCount2 + updatedCount3 + updatedCount4 >=
          retryExpiredTaskExecutionsBatchSize * 0.25,
      }
    })

    const processReadyTaskExecutions = yield* makeBackgroundProcessor({
      executorId,
      processName: 'processReadyTaskExecutions',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: processReadyTaskExecutionsIteration,
    })

    const processOnChildrenFinishedTaskExecutions = yield* makeBackgroundProcessor({
      executorId,
      processName: 'processOnChildrenFinishedTaskExecutions',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: processOnChildrenFinishedTaskExecutionsIteration,
    })

    const markFinishedTaskExecutionsAsCloseStatusReady = yield* makeBackgroundProcessor({
      executorId,
      processName: 'markFinishedTaskExecutionsAsCloseStatusReady',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: markFinishedTaskExecutionsAsCloseStatusReadyIteration,
    })

    const closeFinishedTaskExecutions = yield* makeBackgroundProcessor({
      executorId,
      processName: 'closeFinishedTaskExecutions',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: closeFinishedTaskExecutionsIteration,
    })

    const cancelsNeedPromiseCancellationTaskExecutions = yield* makeBackgroundProcessor({
      executorId,
      processName: 'cancelsNeedPromiseCancellationTaskExecutions',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: cancelsNeedPromiseCancellationTaskExecutionsIteration,
    })

    const retryExpiredTaskExecutions = yield* makeBackgroundProcessor({
      executorId,
      processName: 'retryExpiredTaskExecutions',
      intraIterationSleepMs: backgroundProcessIntraBatchSleepMs,
      iterationEffect: retryExpiredTaskExecutionsIteration,
    })

    const backgroundProcessors = [
      processReadyTaskExecutions,
      processOnChildrenFinishedTaskExecutions,
      markFinishedTaskExecutionsAsCloseStatusReady,
      closeFinishedTaskExecutions,
      cancelsNeedPromiseCancellationTaskExecutions,
      retryExpiredTaskExecutions,
    ] as const

    const isRunning = Effect.gen(function* () {
      return yield* Ref.get(stateRef).pipe(
        Effect.map((state) => state.isStarted && !state.isShutdown),
      )
    })

    const start = Effect.gen(function* () {
      const isStarted = yield* Ref.modify(stateRef, (state) => {
        return state.isStarted || state.isShutdown
          ? [false, state]
          : [true, { ...state, isStarted: true }]
      })
      if (!isStarted) {
        return
      }

      yield* storage.start
      yield* Effect.forEach(
        backgroundProcessors,
        (backgroundProcessor) => backgroundProcessor.start,
        {
          concurrency: 'unbounded',
          discard: true,
        },
      )
    })

    const shutdown = Effect.gen(function* () {
      const isAlreadyShutdown = yield* Ref.modify(stateRef, (state) =>
        state.isShutdown ? [true, state] : [false, { ...state, isShutdown: true }],
      )
      if (isAlreadyShutdown) {
        return
      }

      yield* Effect.sync(() => shutdownController.abort())
      yield* Effect.forEach(
        backgroundProcessors,
        (backgroundProcessor) => backgroundProcessor.shutdown,
        {
          concurrency: 'unbounded',
          discard: true,
        },
      )
      yield* storage.shutdown
      yield* fiberPool.shutdown
    })

    return {
      id: executorId,
      task,
      sleepingTask,
      parentTask,
      sequentialTasks,
      loopingTask,
      validateInput,
      inputSchema,
      enqueueTask,
      getTaskExecutionHandle,
      wakeupSleepingTaskExecution,
      isRunning,
      start,
      shutdown,
      getStorageMetrics: storage.getMetrics,
      getRunningTaskExecutionsCount: Effect.sync(() => runningTaskExecutionsMap.size),
      getRunningTaskExecutionIds: Effect.sync(() => new Set(runningTaskExecutionsMap.keys())),
    }
  })

  return yield* Effect.acquireRelease(
    effect.pipe(
      Logger.withMinimumLogLevel(
        logLevel === 'debug'
          ? LogLevel.Debug
          : logLevel === 'error'
            ? LogLevel.Error
            : LogLevel.Info,
      ),
    ),
    (effectDurableExecutor) => effectDurableExecutor.shutdown,
  )
})

/**
 * Type representing an Effect-based durable executor instance.
 *
 * @see {@link makeEffectDurableExecutor} for more details.
 *
 * @category Executor
 */
export type EffectDurableExecutor = Effect.Effect.Success<
  ReturnType<typeof makeEffectDurableExecutor>
>
