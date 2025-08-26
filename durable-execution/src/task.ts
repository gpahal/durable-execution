import type { CancelSignal } from '@gpahal/std/cancel'

import type { DurableExecutionErrorStorageValue } from './errors'

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
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type Task<TInput, TOutput, TIsSleepingTask extends boolean = boolean> = {
  id: string
  isSleepingTask: TIsSleepingTask
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  areChildrenSequential: boolean
}

/**
 * Type alias for a task with unknown input and output.
 *
 * @category Task
 */
export type AnyTask =
  | Task<unknown, unknown>
  | Task<unknown, unknown, true>
  | Task<unknown, unknown, false>

/**
 * Type-safe record of available tasks.
 *
 * This type ensures compile-time safety when enqueuing tasks, providing autocomplete for task names
 * and type checking for inputs/outputs.
 *
 * @example
 * ```ts
 * // Define your tasks with proper types
 * const emailTask = executor.task<{to: string}, {messageId: string}>({...})
 * const reportTask = executor.task<{userId: string}, {reportUrl: string}>({...})
 *
 * // Create task registry
 * const tasks = {
 *   sendEmail: emailTask,
 *   generateReport: reportTask,
 * } as const  // Use 'as const' for better type inference
 *
 * // Client gets full type safety
 * const client = new DurableExecutorClient(storage, tasks)
 *
 * // TypeScript knows available tasks and their types
 * await client.enqueueTask('sendEmail', { to: 'user@example.com' })
 * // Error: 'invalidTask' doesn't exist
 * // await client.enqueueTask('invalidTask', {})
 * ```
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
 * TypeScript utility type to extract the sleeping task type from a Task.
 *
 * @example
 * ```ts
 * type EmailIsSleepingTask = InferTaskIsSleepingTask<typeof emailTask>
 * // Result: false
 * ```
 *
 * @category Task
 */
export type InferTaskIsSleepingTask<TTask extends AnyTask> =
  TTask extends Task<unknown, unknown, true> ? true : false

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
   * The base delay before each retry. Defaults to 0.
   */
  baseDelayMs?: number
  /**
   * The multiplier for the delay before each retry. Default is 1.
   */
  delayMultiplier?: number
  /**
   * The maximum delay before each retry.
   */
  maxDelayMs?: number
}

/**
 * Options for a task that can be run using a durable executor. A task is resilient to task
 * failures, process failures, network connectivity issues, and other transient errors. The task
 * should be idempotent as it may be run multiple times if there is a process failure or if the
 * task is retried.
 *
 * When enqueued with an executor, a {@link TaskExecutionHandle} is returned. It supports getting
 * the execution status, waiting for the task to complete, and cancelling the task.
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
  run: (ctx: TaskRunContext, input: TInput) => TOutput | Promise<TOutput>
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
 * output is of the form `{ output: TRunOutput, children: Array<ChildTask> }` where the children
 * are the tasks to be run in parallel after the run function completes.
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
 * `{ output: TRunOutput, children: Array<FinishedChildTaskExecution> }`.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
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
        children?: Array<ChildTask>
      }
    | Promise<{
        output: TRunOutput
        children?: Array<ChildTask>
      }>
  /**
   * Function or task to run after the runParent function and children tasks finish. This is useful
   * for combining the output of the run function and children tasks. It is called even if the
   * children tasks fail.
   *
   * If it is a function, it is called without any durability guarantees and retries. It should
   * not be a long running function.
   */
  finalize?:
    | ((input: DefaultParentTaskOutput<TRunOutput>) => TOutput | Promise<TOutput>)
    | FinalizeTaskOptions<TRunOutput, TOutput, TFinalizeTaskRunOutput>
}

/**
 * Options for the `finalize` property in {@link ParentTaskOptions}. It is similar to
 * {@link TaskOptions} or {@link ParentTaskOptions} but the input is of the form:
 *
 * ```ts
 * {
 *   output: TRunOutput,
 *   children: Array<FinishedChildTaskExecution>
 * }
 * ```
 *
 * **Important**: The `children` array includes ALL children task executions, both successful and
 * failed ones. This behaves similar to `Promise.allSettled()` - you get the results regardless of
 * individual child success or failure, allowing you to implement custom error handling logic.
 *
 * No validation is done on the input and the output of the parent task is the output of the
 * `finalize` task.
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
  children: Array<FinishedChildTaskExecution>
}

export function isFinalizeTaskOptionsTaskOptions<
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: FinalizeTaskOptions<TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is TaskOptions<DefaultParentTaskOutput<TRunOutput>, TOutput> {
  return 'run' in options && !('runParent' in options)
}

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
 * - **Signals**: `shutdownSignal`, `cancelSignal` - For graceful termination
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
 *     if (ctx.shutdownSignal.isCancelled() || ctx.cancelSignal.isCancelled()) {
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
   * The shutdown signal of the executor. It is cancelled when the executor is shutting down.
   * It can be used to gracefully shutdown the task when executor is shutting down.
   */
  shutdownSignal: CancelSignal
  /**
   * The cancel signal of the task. It can be used to gracefully shutdown the task run function
   * when the task has been cancelled.
   */
  cancelSignal: CancelSignal
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
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  status: 'ready'
  input: unknown
  error?: DurableExecutionErrorStorageValue
  retryAttempts: number
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
  startedAt: Date
  expiresAt: Date
}

/**
 * A task execution that failed while running.
 *
 * @category Task
 */
export type FailedTaskExecution = Omit<RunningTaskExecution, 'status' | 'error'> & {
  status: 'failed'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that timed out while running.
 *
 * @category Task
 */
export type TimedOutTaskExecution = Omit<RunningTaskExecution, 'status' | 'error'> & {
  status: 'timed_out'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that is waiting for children tasks to complete.
 *
 * @category Task
 */
export type WaitingForChildrenTaskExecution = Omit<RunningTaskExecution, 'status' | 'error'> & {
  status: 'waiting_for_children'
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
  'status' | 'error'
> & {
  status: 'waiting_for_finalize'
  finalize: TaskExecutionSummary
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
  'status' | 'output'
> & {
  status: 'completed'
  output: TOutput
  finishedAt: Date

  /**
   * The finalize task execution. This is only present for tasks which have a finalize task and
   * whose run method completed successfully and children task finished.
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
   * The time the task execution started. This is only present for tasks which started running.
   */
  startedAt?: Date
  /**
   * The time the task execution expires. This is only present for tasks which started running.
   */
  expiresAt?: Date
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
   * whose run method completed successfully and children task finished.
   */
  finalize?: TaskExecutionSummary
}

/**
 * A child task of a task.
 *
 * @category Task
 */
export class ChildTask<TTask extends AnyTask = AnyTask> {
  readonly task: TTask
  readonly input: InferTaskInput<TTask>
  readonly options?: TaskEnqueueOptions<AnyTask>

  constructor(
    ...rest: undefined extends InferTaskInput<TTask>
      ? [task: TTask, input?: InferTaskInput<TTask>, options?: TaskEnqueueOptions<TTask>]
      : [task: TTask, input: InferTaskInput<TTask>, options?: TaskEnqueueOptions<TTask>]
  ) {
    this.task = rest[0]
    this.input = (rest.length > 0 ? rest[1] : undefined)!
    this.options = (rest.length > 2 ? rest[2] : undefined) as
      | TaskEnqueueOptions<AnyTask>
      | undefined
  }
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
] as Array<TaskExecutionStatus>

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
] as Array<TaskExecutionStatus>

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
] as Array<TaskExecutionStatus>

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
] as Array<TaskExecutionStatus>

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
  retryOptions?: false extends InferTaskIsSleepingTask<TTask>
    ? TaskRetryOptions
    : true extends InferTaskIsSleepingTask<TTask>
      ? undefined
      : TaskRetryOptions
  /**
   * The number of milliseconds to wait before running the task. Has no effect on sleeping tasks.
   */
  sleepMsBeforeRun?: false extends InferTaskIsSleepingTask<TTask>
    ? number
    : true extends InferTaskIsSleepingTask<TTask>
      ? undefined
      : number
  /**
   * The number of milliseconds after which the task will be timed out.
   */
  timeoutMs?: number
}

/**
 * A handle providing control and monitoring capabilities for a task execution.
 *
 * TaskExecutionHandle is returned when you enqueue a task and provides methods to:
 * - Monitor execution progress and status
 * - Wait for completion with polling
 * - Cancel running or queued tasks
 * - Access task metadata
 *
 * The handle is type-safe and provides strongly-typed access to the task's output.
 *
 * ## Common Patterns
 *
 * ### Fire-and-Forget
 *
 * ```ts
 * await executor.enqueueTask(backgroundTask, input)
 * // Don't wait for result, just enqueue and continue
 * ```
 *
 * ### Wait for Result
 *
 * ```ts
 * const handle = await executor.enqueueTask(processingTask, input)
 * const result = await handle.waitAndGetFinishedExecution()
 *
 * if (result.status === 'completed') {
 *   console.log('Success:', result.output)
 * } else {
 *   console.error('Failed:', result.error?.message)
 * }
 * ```
 *
 * ### Polling with Cancellation
 *
 * ```ts
 * const handle = await executor.enqueueTask(longTask, input)
 *
 * // Cancel after timeout
 * setTimeout(() => handle.cancel(), 30_000)
 *
 * try {
 *   const result = await handle.waitAndGetFinishedExecution({
 *     pollingIntervalMs: 500  // Check every 500ms
 *   })
 * } catch (error) {
 *   // Handle cancellation or other errors
 * }
 * ```
 *
 * ### Status Monitoring
 *
 * ```ts
 * const handle = await executor.enqueueTask(batchTask, input)
 *
 * // Periodically check status
 * const interval = setInterval(async () => {
 *   const execution = await handle.getExecution()
 *   console.log(`Status: ${execution.status}`)
 *
 *   if (execution.status === 'waiting_for_children') {
 *     console.log(`Children: ${execution.activeChildrenCount} active`)
 *   }
 * }, 1000)
 *
 * await handle.waitAndGetFinishedExecution()
 * clearInterval(interval)
 * ```
 *
 * @see [Task execution](https://gpahal.github.io/durable-execution/index.html#task-execution)
 * for detailed information about execution states.
 *
 * @category Task
 */
export type TaskExecutionHandle<TOutput = unknown> = {
  /**
   * Get the unique identifier of the task definition.
   *
   * @returns The task id (e.g., 'sendEmail', 'processFile')
   */
  getTaskId: () => string
  /**
   * Get the unique identifier of this specific task execution instance.
   *
   * Each time a task is enqueued, it gets a new execution id for tracking.
   *
   * @returns The execution id (e.g., 'te_abc123')
   */
  getExecutionId: () => string
  /**
   * Get the current state and metadata of the task execution.
   *
   * This provides a snapshot of the current execution state including status,
   * timing information, error details, and child task information.
   *
   * @returns Promise resolving to the current task execution state
   */
  getExecution: () => Promise<TaskExecution<TOutput>>
  /**
   * Wait for the task to reach a terminal state and return the final result.
   *
   * This method polls the task execution status until it reaches a finished state
   * (completed, failed, timed_out, finalize_failed, or cancelled).
   *
   * @param options - Configuration for the waiting behavior
   * @param options.signal - Signal to cancel the waiting (not the task itself)
   * @param options.pollingIntervalMs - How often to check status (default: 1000ms)
   * @returns Promise resolving to the finished task execution with typed output
   *
   * @example
   * ```ts
   * const result = await handle.waitAndGetFinishedExecution({
   *   pollingIntervalMs: 500,  // Check every 500ms
   *   signal: abortController.signal  // Cancel waiting on user action
   * })
   * ```
   */
  waitAndGetFinishedExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    pollingIntervalMs?: number
  }) => Promise<FinishedTaskExecution<TOutput>>
  /**
   * Request cancellation of the task execution.
   *
   * This marks the task as cancelled and signals any running execution to stop. The task can check
   * `ctx.cancelSignal` to handle cancellation gracefully.
   *
   * - If the task is queued: It will be marked as cancelled without running
   * - If the task is running: It receives a cancellation signal via `ctx.cancelSignal`
   * - If the task has children: All child tasks are also cancelled
   *
   * @returns Promise that resolves when the cancellation request is processed
   *
   * @example
   * ```ts
   * const handle = await executor.enqueueTask(longRunningTask, input)
   *
   * // Cancel after 30 seconds
   * setTimeout(() => handle.cancel(), 30_000)
   *
   * try {
   *   await handle.waitAndGetFinishedExecution()
   * } catch (error) {
   *   // Task was cancelled
   * }
   * ```
   */
  cancel: () => Promise<void>
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
