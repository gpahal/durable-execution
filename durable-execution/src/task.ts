import type { Effect } from 'effect'

import type { DurableExecutionErrorStorageValue } from './errors'

/**
 * The type of task.
 *
 * @category Task
 */
export type TaskType = 'task' | 'sleepingTask' | 'parentTask' | 'sequentialTasks'

/**
 * Represents a durable task that can be executed with automatic retry, timeout and failure handling
 * capabilities.
 *
 * Tasks are the fundamental unit of work in the durable execution system. They encapsulate business
 * logic that needs to run reliably despite failures.
 *
 * ## Key Properties
 *
 * - **id**: Unique identifier for the task type
 * - **retryOptions**: Configuration for automatic retry behavior
 * - **sleepMsBeforeRun**: Optional delay before execution starts
 * - **timeoutMs**: Maximum execution time before timeout
 *
 * @example
 * ```ts
 * const emailTask: Task<{to: string, subject: string}, {messageId: string}> = {
 *   id: 'sendEmail',
 *   retryOptions: { maxAttempts: 3, baseDelayMs: 1000 },
 *   sleepMsBeforeRun: 0,
 *   timeoutMs: 30000
 * }
 * ```
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) for
 * more patterns and use cases.
 *
 * @category Task
 */
export type Task<
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  TInput,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  TOutput,
  TTaskType extends TaskType = TaskType,
> = {
  readonly taskType: TTaskType
  readonly id: string
  readonly retryOptions: TaskRetryOptions
  readonly sleepMsBeforeRun: number
  readonly timeoutMs: number
}

/**
 * Type alias for a task with unknown input and output.
 *
 * @category Task
 */
export type AnyTask = Task<unknown, unknown>

/**
 * Type-safe record of available tasks.
 *
 * @category Task
 */
export type AnyTasks = Record<string, AnyTask>

/**
 * TypeScript utility type to extract the input type from a Task.
 *
 * @example
 * ```ts
 * type EmailInput = InferTaskInput<typeof emailTask>
 * // Result: { to: string, subject: string }
 * ```
 *
 * @category Task
 */
export type InferTaskInput<TTask extends AnyTask> = TTask extends Task<infer I, unknown> ? I : never

/**
 * TypeScript utility type to extract the output type from a Task.
 *
 * @example
 * ```ts
 * type EmailOutput = InferTaskOutput<typeof emailTask>
 * // Result: { messageId: string }
 * ```
 *
 * @category Task
 */
export type InferTaskOutput<TTask extends AnyTask> =
  TTask extends Task<unknown, infer O> ? O : never

/**
 * TypeScript utility type to extract the task type from a Task.
 *
 * @example
 * ```ts
 * type EmailTaskType = InferTaskType<typeof emailTask>
 * // Result: 'task'
 * ```
 *
 * @category Task
 */
export type InferTaskType<TTask extends AnyTask> =
  TTask extends Task<unknown, unknown, infer T> ? T : never

/**
 * Common options for a task. These options are used by both {@link TaskOptions} and
 * {@link ParentTaskOptions}.
 *
 * @category Task
 */
export type CommonTaskOptions = {
  /**
   * A unique identifier for the task. Can only contain alphanumeric characters and underscores.
   * The identifier must be unique among all the tasks in the same executor.
   */
  id: string
  /**
   * The options for retrying the task.
   */
  retryOptions?: TaskRetryOptions
  /**
   * The delay before running the task run function. If the value is undefined, it will be treated
   * as 0.
   */
  sleepMsBeforeRun?: number
  /**
   * The timeout for the task run function.
   */
  timeoutMs: number
}

/**
 * Configuration for automatic task retry behavior with exponential backoff.
 *
 * ## Retry Delay Calculation
 *
 * The delay between retries follows an exponential backoff pattern:
 * ```
 * delay = min(baseDelayMs * (delayMultiplier ^ attemptNumber), maxDelayMs)
 * ```
 *
 * @example
 * ```ts
 * // Retry up to 5 times with exponential backoff
 * const retryOptions: TaskRetryOptions = {
 *   maxAttempts: 5,
 *   baseDelayMs: 1000,    // Start with 1 second
 *   delayMultiplier: 2,   // Double each time
 *   maxDelayMs: 30000     // Cap at 30 seconds
 * }
 * // Results in delays: 1s, 2s, 4s, 8s, 16s (capped)
 *
 * // Constant delay (no backoff)
 * const constantRetry: TaskRetryOptions = {
 *   maxAttempts: 3,
 *   baseDelayMs: 5000,    // Always wait 5 seconds
 *   delayMultiplier: 1,   // No increase
 * }
 * // Results in delays: 5s, 5s, 5s
 *
 * // Immediate retry (no delay)
 * const immediateRetry: TaskRetryOptions = {
 *   maxAttempts: 2,
 *   baseDelayMs: 0,       // No delay between retries
 * }
 * // Results in delays: 0s, 0s
 * ```
 *
 * @category Task
 */
export type TaskRetryOptions = {
  /**
   * The maximum number of times the task can be retried.
   */
  maxAttempts: number
  /**
   * The base delay before each retry in milliseconds. Defaults to 0 (immediate retry). When set to
   * 0, retries happen immediately without delay.
   */
  baseDelayMs?: number
  /**
   * The multiplier for the delay before each retry. Defaults to 1 (constant delay). Values > 1
   * create exponential backoff, values < 1 create decreasing delays.
   */
  delayMultiplier?: number
  /**
   * The maximum delay before each retry in milliseconds. When specified, delays are capped at this
   * value regardless of the exponential calculation.
   */
  maxDelayMs?: number
}

/**
 * Options for a task that can be run using a durable executor. A task is resilient to task
 * failures, process failures, network connectivity issues, and other transient errors. The task
 * should be idempotent as it may be run multiple times if there is a process failure or if the
 * task is retried.
 *
 * When enqueued with an executor, a task execution handle is returned. It supports getting the
 * execution status, waiting for the task to complete, and cancelling the task.
 *
 * The output of the `run` function is the output of the task.
 *
 * The input and output are serialized and deserialized using the serializer passed to the durable
 * executor.
 *
 * Make sure the id is unique among all the tasks in the same durable executor. If two tasks are
 * registered with the same id, an error will be thrown.
 *
 * The tasks can be added to an executor using the {@link DurableExecutor.task} method. If the
 * input to a task needs to be validated, it can be done using the
 * {@link DurableExecutor.validateInput} or {@link DurableExecutor.inputSchema} methods.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
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
 * ```
 *
 * @category Task
 */
export type TaskOptions<TInput = undefined, TOutput = unknown> = CommonTaskOptions & {
  /**
   * The task run logic. It returns the output.
   *
   * Behavior on throwing errors:
   * - If the task throws an error, it will be marked as failed
   * - If the task throws a `{@link DurableExecutionTimedOutError}`, it will be marked as timed out
   * - If the task throws a `{@link DurableExecutionCancelledError}`, it will be marked as cancelled
   * - Failed and timed out tasks might be retried based on the task's retry configuration
   * - Cancelled tasks will not be retried
   *
   * @param ctx - The context object to the task.
   * @param input - The input to the task.
   * @returns The output of the task.
   */
  run: (
    ctx: TaskRunContext,
    input: TInput,
  ) => TOutput | Promise<TOutput> | Effect.Effect<TOutput, unknown, never>
}

/**
 * Options for a task that is asleep until it is marked completed or failed or timed out or
 * cancelled. It is similar to {@link TaskOptions} but it does not have a `run` function. It
 * just sleeps until it is marked completed or failed or timed out or cancelled.
 *
 * It is not very useful by itself but when combined with {@link ParentTaskOptions}, it can be used
 * to create a task that is a parent task that runs a child task that is asleep until it is marked
 * completed by a webhook or some other event.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
 * const waitForWebhook = executor
 *   .sleepingTask<{ webhookId: string }>({
 *     id: 'waitForWebhook',
 *     timeoutMs: 24 * 60 * 60 * 1000, // 24 hours
 *   })
 *
 * // Enqueue the sleeping task
 * const handle = await executor.enqueueTask(waitForWebhook, 'uniqueId')
 *
 * // Wakeup using the unique id and executor (or execution client)
 * await executor.wakeupSleepingTaskExecution(waitForWebhook, 'uniqueId', {
 *   status: 'completed',
 *   output: {
 *     webhookId: '123',
 *   },
 * })
 *
 * // Or, wakeup in a webhook or event handler asynchronously using the unique id and executor
 * // (or execution client)
 * await executor.wakeupSleepingTaskExecution(waitForWebhook, 'uniqueId', {
 *   status: 'completed',
 *   output: {
 *     webhookId: '123',
 *   },
 * })
 * ```
 *
 * @template TOutput - The type of the output when the sleeping task is woken up
 *
 * @category Task
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type SleepingTaskOptions<TOutput = unknown> = Pick<CommonTaskOptions, 'id' | 'timeoutMs'>

/**
 * Options for a parent task that can be run using a durable executor. It is similar to
 * {@link TaskOptions} but it returns children tasks to be run in parallel after the run
 * function completes, along with the output of the parent task.
 *
 * The `runParent` function is similar to the `run` function in {@link TaskOptions}, but the
 * output is of the form `{ output: TRunOutput, children: ReadonlyArray<ChildTask> }` where the
 * children are the tasks to be run in parallel after the run function completes.
 *
 * The `finalize` function or task is run after the runParent function and all the children tasks
 * finish. It is useful for combining the output of the runParent function and children tasks. It
 * is called even if the children tasks fail. Its input has the following properties:
 *
 * - `output`: The output of the runParent function
 * - `children`: The finished children task executions (includes both successful and failed
 *   children)
 *
 * **Important**: The `finalize` function or task receives outputs from ALL children, including
 * those that have failed. This behaves similar to `Promise.allSettled()` - you get the results
 * regardless of individual child success or failure. This allows you to implement custom error
 * handling logic.
 *
 * If `finalize` is provided, the output of the whole task is the output of the `finalize` function
 * or task. If it is not provided, the output of the whole task is of the form
 * `{ output: TRunOutput, children: ReadonlyArray<FinishedChildTaskExecution> }`.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
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
 * ```
 *
 * @category Task
 */
export type ParentTaskOptions<
  TInput = undefined,
  TRunOutput = unknown,
  TOutput = DefaultParentTaskOutput<TRunOutput>,
  TFinalizeTaskRunOutput = unknown,
> = CommonTaskOptions & {
  /**
   * The task run logic. It is similar to the `run` function in {@link TaskOptions} but it
   * returns the output and children tasks to be run in parallel after the run function completes.
   *
   * @param ctx - The context object to the task.
   * @param input - The input of the task.
   * @returns The output of the task and children tasks to be run in parallel after the run
   * function completes.
   */
  runParent: (
    ctx: TaskRunContext,
    input: TInput,
  ) =>
    | {
        output: TRunOutput
        children?: ReadonlyArray<ChildTask>
      }
    | Promise<{
        output: TRunOutput
        children?: ReadonlyArray<ChildTask>
      }>
    | Effect.Effect<
        {
          output: TRunOutput
          children?: ReadonlyArray<ChildTask>
        },
        unknown,
        never
      >
  /**
   * Function or task to run after the runParent function and children tasks finish. This is useful
   * for combining the output of the run function and children tasks. It is called even if the
   * children tasks fail.
   *
   * If it is a function, it is called without any durability guarantees and retries. It should
   * not be a long running function.
   */
  finalize?:
    | ((
        input: DefaultParentTaskOutput<TRunOutput>,
      ) => TOutput | Promise<TOutput> | Effect.Effect<TOutput, unknown, never>)
    | FinalizeTaskOptions<TRunOutput, TOutput, TFinalizeTaskRunOutput>
}

/**
 * Options for the `finalize` property in {@link ParentTaskOptions}. It is similar to
 * {@link TaskOptions} or {@link ParentTaskOptions} but the input is of the form:
 *
 * ```ts
 * {
 *   output: TRunOutput,
 *   children: ReadonlyArray<FinishedChildTaskExecution>
 * }
 * ```
 *
 * **Critical**: The `finalize` function/task receives outputs from all children, including those
 * that have failed. This behaves similar to `Promise.allSettled()` - you get the results
 * regardless of individual child success or failure. This allows you to implement custom error
 * handling logic, such as failing the parent only if critical children fail, or providing partial
 * results. As a caveat, always check the status of child executions in the finalize function/task.
 *
 * No validation is done on the input and the output of the parent task is the output of the
 * `finalize` task.
 *
 * @template TRunOutput - The type of the parent task's run output
 * @template TOutput - The type of the finalize task's output (becomes the parent task's output)
 * @template TFinalizeTaskRunOutput - If finalize is a parent task, the type of its run output
 *
 * @category Task
 */
export type FinalizeTaskOptions<
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
> =
  | TaskOptions<DefaultParentTaskOutput<TRunOutput>, TOutput>
  | ParentTaskOptions<DefaultParentTaskOutput<TRunOutput>, TFinalizeTaskRunOutput, TOutput>

/**
 * The default output type for a parent task when no `finalize` task is provided.
 *
 * @category Task
 */
export type DefaultParentTaskOutput<TRunOutput = unknown> = {
  output: TRunOutput
  children: ReadonlyArray<FinishedChildTaskExecution>
}

/**
 * Type predicate to check if a {@link FinalizeTaskOptions} is a {@link TaskOptions}.
 *
 * This utility function helps determine whether the finalize configuration is a simple task (with
 * a `run` function) or a parent task (with a `runParent` function).
 *
 * @template TRunOutput - The type of the parent task's run output.
 * @template TOutput - The type of the finalize task's output.
 * @template TFinalizeTaskRunOutput - If finalize is a parent task, the type of its run output.
 *
 * @param options - The finalize task options to check.
 * @returns `true` if the options represent a simple task, `false` otherwise.
 *
 * @category Task
 * @internal
 */
export function isFinalizeTaskOptionsTaskOptions<
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: FinalizeTaskOptions<TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is TaskOptions<DefaultParentTaskOutput<TRunOutput>, TOutput> {
  return 'run' in options && !('runParent' in options)
}

/**
 * Type predicate to check if a {@link FinalizeTaskOptions} is a {@link ParentTaskOptions}.
 *
 * This utility function helps determine whether the finalize configuration is a parent task (with
 * a `runParent` function) or a simple task (with a `run` function).
 *
 * @template TRunOutput - The type of the parent task's run output.
 * @template TOutput - The type of the finalize task's output.
 * @template TFinalizeTaskRunOutput - If finalize is a parent task, the type of its run output.
 *
 * @param options - The finalize task options to check.
 * @returns `true` if the options represent a parent task, `false` otherwise.
 *
 * @category Task
 * @internal
 */
export function isFinalizeTaskOptionsParentTaskOptions<
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: FinalizeTaskOptions<TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is ParentTaskOptions<
  DefaultParentTaskOutput<TRunOutput>,
  TFinalizeTaskRunOutput,
  TOutput
> {
  return 'runParent' in options && !('run' in options)
}

/**
 * Runtime context provided to every task execution, containing metadata, signals and information
 * about the execution environment.
 *
 * ## Context Properties
 *
 * - **Identity**: `taskId`, `executionId` - Unique identifiers
 * - **Hierarchy**: `root`, `parent` - Task relationship information
 * - **Signals**: `shutdownSignal`, `abortSignal` - For graceful termination
 * - **Retry State**: `attempt`, `prevError` - Retry attempt information
 *
 * @example
 * ```ts
 * const task = executor.task({
 *   id: 'processData',
 *   timeoutMs: 60000,
 *   run: async (ctx, input) => {
 *     console.log(`Execution ${ctx.executionId}, attempt ${ctx.attempt}`)
 *
 *     // Check for executor shutdown or task cancellation
 *     if (ctx.shutdownSignal.aborted || ctx.abortSignal.aborted) {
 *       throw new DurableExecutionCancelledError()
 *     }
 *
 *     // Check previous error if retrying
 *     if (ctx.prevError) {
 *       console.log('Retrying after:', ctx.prevError.message)
 *     }
 *
 *     // Process data...
 *     return result
 *   }
 * })
 * ```
 *
 * @category Task
 */
export type TaskRunContext = {
  /**
   * The root task execution.
   */
  root?: TaskExecutionSummary
  /**
   * The parent task execution.
   */
  parent?: ParentTaskExecutionSummary
  /**
   * The task id.
   */
  taskId: string
  /**
   * The task execution id.
   */
  executionId: string
  /**
   * The shutdown signal of the task. It is set when the executor is shutting down.
   */
  shutdownSignal: AbortSignal
  /**
   * The abort signal of the task. It is cancelled when the task gets cancelled or times out.
   */
  abortSignal: AbortSignal
  /**
   * The attempt number of the task. The first attempt is 0, the second attempt is 1, etc.
   */
  attempt: number
  /**
   * The error of the previous attempt.
   */
  prevError?: DurableExecutionErrorStorageValue
}

/**
 * Represents the current state and metadata of a task execution.
 *
 * Task executions transition through various states during their lifecycle:
 * `ready` → `running` → `completed` (or `failed`/`timed_out`/`cancelled`)
 *
 * Parent tasks have additional states:
 * `running` → `waiting_for_children` → `waiting_for_finalize` → `completed`
 *
 * @see [Task execution](https://gpahal.github.io/durable-execution/index.html#task-execution)
 * for detailed state transitions.
 *
 * @category Task
 */
export type TaskExecution<TOutput = unknown> =
  | ReadyTaskExecution
  | RunningTaskExecution
  | FailedTaskExecution
  | TimedOutTaskExecution
  | WaitingForChildrenTaskExecution
  | WaitingForFinalizeTaskExecution
  | FinalizeFailedTaskExecution
  | CompletedTaskExecution<TOutput>
  | CancelledTaskExecution

/**
 * Represents a task execution that has reached a terminal state.
 *
 * Terminal states include:
 * - `completed`: Successfully finished with output
 * - `failed`: Execution failed (may have been retried)
 * - `timed_out`: Exceeded timeout limit
 * - `finalize_failed`: Parent task's finalize function failed
 * - `cancelled`: Manually cancelled or parent failed
 *
 * Once in a terminal state, the task will not execute again.
 *
 * @category Task
 */
export type FinishedTaskExecution<TOutput = unknown> =
  | FailedTaskExecution
  | TimedOutTaskExecution
  | FinalizeFailedTaskExecution
  | CompletedTaskExecution<TOutput>
  | CancelledTaskExecution

/**
 * A task execution that is ready to be run.
 *
 * @category Task
 */
export type ReadyTaskExecution = {
  root?: {
    taskId: string
    executionId: string
  }

  parent?: {
    taskId: string
    executionId: string
  }

  taskId: string
  executionId: string
  sleepingTaskUniqueId?: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  areChildrenSequential: boolean
  input: unknown
  status: 'ready'
  error?: DurableExecutionErrorStorageValue
  retryAttempts: number
  startAt: Date
  createdAt: Date
  updatedAt: Date
}

/**
 * A task execution that is running.
 *
 * @category Task
 */
export type RunningTaskExecution = Omit<ReadyTaskExecution, 'status'> & {
  status: 'running'
  executorId: string
  startedAt: Date
  expiresAt: Date
}

/**
 * A task execution that failed while running.
 *
 * @category Task
 */
export type FailedTaskExecution = Omit<RunningTaskExecution, 'executorId' | 'status' | 'error'> & {
  status: 'failed'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that timed out while running.
 *
 * @category Task
 */
export type TimedOutTaskExecution = Omit<
  RunningTaskExecution,
  'executorId' | 'status' | 'error'
> & {
  status: 'timed_out'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that is waiting for children tasks to complete.
 *
 * @category Task
 */
export type WaitingForChildrenTaskExecution = Omit<
  RunningTaskExecution,
  'executorId' | 'status' | 'error'
> & {
  status: 'waiting_for_children'
  waitingForChildrenStartedAt: Date
  children: Array<TaskExecutionSummary>
  activeChildrenCount: number
}

/**
 * A task execution that is waiting for the finalize task to complete.
 *
 * @category Task
 */
export type WaitingForFinalizeTaskExecution = Omit<
  WaitingForChildrenTaskExecution,
  'status' | 'error' | 'waitingForChildrenStartedAt'
> & {
  status: 'waiting_for_finalize'
  waitingForFinalizeStartedAt: Date
  finalize: TaskExecutionSummary

  /**
   * The time the task execution waiting for children started. This is only present for tasks which
   * have children.
   */
  waitingForChildrenStartedAt?: Date
}

/**
 * A task execution that failed while waiting for the finalize task to complete because the
 * finalize task execution failed.
 *
 * @category Task
 */
export type FinalizeFailedTaskExecution = Omit<
  WaitingForFinalizeTaskExecution,
  'status' | 'error'
> & {
  status: 'finalize_failed'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that completed successfully.
 *
 * @category Task
 */
export type CompletedTaskExecution<TOutput = unknown> = Omit<
  WaitingForChildrenTaskExecution,
  'status' | 'output' | 'waitingForChildrenStartedAt'
> & {
  status: 'completed'
  output: TOutput
  finishedAt: Date

  /**
   * The time the task execution waiting for children started. This is only present for tasks which
   * have children tasks and whose run method completed successfully.
   */
  waitingForChildrenStartedAt?: Date
  /**
   * The time the task execution waiting for finalize started. This is only present for tasks which
   * have a finalize task and whose run method completed successfully and children tasks finished.
   */
  waitingForFinalizeStartedAt?: Date
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task and
   * whose run method completed successfully and children tasks finished.
   */
  finalize?: TaskExecutionSummary
}

/**
 * A task execution that was cancelled. This can happen when a task is cancelled by using
 * the cancel method of the task handle or when the task is cancelled because it's parent task
 * failed.
 *
 * @category Task
 */
export type CancelledTaskExecution = Omit<ReadyTaskExecution, 'status' | 'error'> & {
  status: 'cancelled'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date

  /**
   * The id of the executor. This is only present for tasks which were cancelled while running.
   */
  executorId?: string
  /**
   * The time the task execution started. This is only present for tasks which started running.
   */
  startedAt?: Date
  /**
   * The time the task execution expires. This is only present for tasks which started running.
   */
  expiresAt?: Date
  /**
   * The time the task execution waiting for children started. This is only present for tasks which
   * have children tasks and whose run method completed successfully.
   */
  waitingForChildrenStartedAt?: Date
  /**
   * The time the task execution waiting for finalize started. This is only present for tasks which
   * have a finalize task and whose run method completed successfully and children tasks finished.
   */
  waitingForFinalizeStartedAt?: Date
  /**
   * The children task executions that were running when the task was cancelled. This is only present for
   * tasks whose run method completed successfully.
   */
  children?: Array<TaskExecutionSummary>
  /**
   * The number of children task executions that are still active. This is only present for tasks
   * whose run method completed successfully.
   */
  activeChildrenCount: number
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task and
   * whose run method completed successfully and children tasks finished.
   */
  finalize?: TaskExecutionSummary
}

/**
 * A child task of a task.
 *
 * @category Task
 */
export type ChildTask<TTask extends AnyTask = AnyTask> =
  undefined extends InferTaskInput<TTask>
    ? {
        task: TTask
        input?: InferTaskInput<TTask>
        options?: TaskEnqueueOptions<AnyTask>
      }
    : {
        task: TTask
        input: InferTaskInput<TTask>
        options?: TaskEnqueueOptions<AnyTask>
      }

/**
 * Create a type-safe child task. Usually used alongside the `parentTask` function. See
 * {@link ParentTaskOptions} for more details.
 *
 * @param rest - The task, input and options.
 * @returns A child task.
 *
 * @category Task
 */
export function childTask<TTask extends AnyTask = AnyTask>(
  ...rest: undefined extends InferTaskInput<TTask>
    ? [task: TTask, input?: InferTaskInput<TTask>, options?: TaskEnqueueOptions<TTask>]
    : [task: TTask, input: InferTaskInput<TTask>, options?: TaskEnqueueOptions<TTask>]
): ChildTask<TTask> {
  return {
    task: rest[0],
    input: rest[1],
    options: rest[2],
  } as ChildTask<TTask>
}

/**
 * A finished child task execution.
 *
 * This type represents both successful and failed child task executions, similar to the result
 * of `Promise.allSettled()`. The finalize function receives all children regardless of their
 * completion status, allowing for custom error handling logic.
 *
 * @category Task
 */
export type FinishedChildTaskExecution<TOutput = unknown> =
  | CompletedChildTaskExecution<TOutput>
  | ErroredChildTaskExecution

/**
 * A completed child task execution.
 *
 * @category Task
 */
export type CompletedChildTaskExecution<TOutput = unknown> = {
  taskId: string
  executionId: string
  status: 'completed'
  output: TOutput
}

/**
 * An errored child task execution.
 *
 * @category Task
 */
export type ErroredChildTaskExecution = {
  taskId: string
  executionId: string
  status: ErroredTaskExecutionStatus
  error: DurableExecutionErrorStorageValue
}

/**
 * A summary of a task execution.
 *
 * @category Task
 */
export type TaskExecutionSummary = {
  taskId: string
  executionId: string
}

/**
 * A summary of a parent task execution.
 *
 * @category Task
 */
export type ParentTaskExecutionSummary = TaskExecutionSummary & {
  indexInParentChildren: number
  isOnlyChildOfParent: boolean
  isFinalizeOfParent: boolean
}

/**
 * A status of a task execution.
 *
 * @category Task
 */
export type TaskExecutionStatus =
  | 'ready'
  | 'running'
  | 'failed'
  | 'timed_out'
  | 'waiting_for_children'
  | 'waiting_for_finalize'
  | 'finalize_failed'
  | 'completed'
  | 'cancelled'

/**
 * The statuses of a task execution that are considered errored.
 *
 * @category Task
 */
export type ErroredTaskExecutionStatus = 'failed' | 'timed_out' | 'finalize_failed' | 'cancelled'

/**
 * All possible statuses of a task execution.
 *
 * @category Task
 */
export const ALL_TASK_EXECUTION_STATUSES = [
  'ready',
  'running',
  'failed',
  'timed_out',
  'waiting_for_children',
  'waiting_for_finalize',
  'finalize_failed',
  'completed',
  'cancelled',
] as ReadonlyArray<TaskExecutionStatus>

/**
 * All possible statuses of a task execution that are considered active.
 *
 * @category Task
 */
export const ACTIVE_TASK_EXECUTION_STATUSES = [
  'ready',
  'running',
  'waiting_for_children',
  'waiting_for_finalize',
] as ReadonlyArray<TaskExecutionStatus>

/**
 * All possible statuses of a task execution that are considered finished.
 *
 * @category Task
 */
export const FINISHED_TASK_EXECUTION_STATUSES = [
  'failed',
  'timed_out',
  'finalize_failed',
  'completed',
  'cancelled',
] as ReadonlyArray<TaskExecutionStatus>

/**
 * All possible statuses of a task execution that are considered errored.
 *
 * @category Task
 */
export const ERRORED_TASK_EXECUTION_STATUSES = [
  'failed',
  'timed_out',
  'finalize_failed',
  'cancelled',
] as ReadonlyArray<TaskExecutionStatus>

/**
 * Runtime options for enqueuing a task that override the task's default configuration.
 *
 * These options allow you to customize task behavior at enqueue time without
 * changing the task definition. Useful for adjusting timeouts or retry behavior
 * based on runtime conditions.
 *
 * @example
 * ```ts
 * // Enqueue with custom timeout for urgent processing
 * const handle = await executor.enqueueTask(emailTask, input, {
 *   timeoutMs: 60_000,  // 1 minute instead of default 30s
 *   retryOptions: {
 *     maxAttempts: 1,    // Don't retry urgent emails
 *   }
 * })
 *
 * // Enqueue with delay for rate limiting
 * const handle = await executor.enqueueTask(apiTask, input, {
 *   sleepMsBeforeRun: 5000  // Wait 5 seconds before starting
 * })
 * ```
 *
 * @category Task
 */
export type TaskEnqueueOptions<TTask extends AnyTask = AnyTask> = {
  /**
   * Options for retrying the task. Has no effect on sleeping tasks.
   */
  retryOptions?: InferTaskType<TTask> extends 'sleepingTask' ? never : TaskRetryOptions
  /**
   * The number of milliseconds to wait before running the task. Has no effect on sleeping tasks.
   */
  sleepMsBeforeRun?: InferTaskType<TTask> extends 'sleepingTask' ? never : number
  /**
   * The number of milliseconds after which the task will be timed out.
   */
  timeoutMs?: number
}

/**
 * Options for waking up a sleeping task execution.
 *
 * @category Task
 */
export type WakeupSleepingTaskExecutionOptions<TOutput = unknown> =
  | {
      status: 'completed'
      output: TOutput
    }
  | {
      status: 'failed'
      error?: unknown
    }

/**
 * Type constraint for sequential task execution where each task's output becomes the next task's
 * input.
 *
 * This utility type ensures type safety in task pipelines by verifying that:
 * - The sequence is not empty
 * - Each task's output type matches the next task's input type
 * - The overall pipeline type is valid
 *
 * Used internally by {@link DurableExecutor.sequentialTasks} to provide compile-time type checking
 * for sequential task pipelines.
 *
 * @example
 * ```ts
 * // Valid sequential tasks
 * const validSequence: SequentialTasks<[
 *   Task<{name: string}, {id: number, name: string}>,
 *   Task<{id: number, name: string}, {result: string}>
 * ]> = [fetchUser, processUser]
 *
 * // Invalid: output/input types don't match
 * const invalidSequence: SequentialTasks<[
 *   Task<{name: string}, {id: number}>,
 *   Task<{email: string}, {result: string}>  // Error: needs {id: number}
 * ]> = [fetchUser, processEmail]  // TypeScript error
 * ```
 *
 * @category Task
 */
export type SequentialTasks<T extends ReadonlyArray<AnyTask>> = T extends readonly []
  ? never
  : SequentialTasksHelper<T>

/**
 * Internal helper type for implementing sequential task type checking.
 *
 * This type recursively validates that each task in the sequence can accept the output from the
 * previous task as its input, ensuring type safety throughout the pipeline.
 *
 * @internal
 * @category Task
 */
export type SequentialTasksHelper<T extends ReadonlyArray<AnyTask>> = T extends readonly []
  ? T
  : T extends readonly [Task<infer _I1, infer _O1>]
    ? T
    : T extends readonly [Task<infer I1, infer O1>, Task<infer I2, infer O2>, ...infer Rest]
      ? O1 extends I2
        ? Rest extends ReadonlyArray<AnyTask>
          ? readonly [Task<I1, O1>, ...SequentialTasksHelper<readonly [Task<I2, O2>, ...Rest]>]
          : never
        : never
      : T

/**
 * Utility type to extract the last task from a sequential task array.
 *
 * Used to determine the final output type of a sequential task pipeline.
 *
 * @example
 * ```ts
 * type LastTask = LastTaskElementInArray<[
 *   Task<string, number>,
 *   Task<number, boolean>,
 *   Task<boolean, string>
 * ]>
 * // Result: Task<boolean, string>
 * ```
 *
 * @internal
 * @category Task
 */
export type LastTaskElementInArray<T extends ReadonlyArray<AnyTask>> = T extends readonly [
  ...Array<AnyTask>,
  infer L,
]
  ? L
  : never
