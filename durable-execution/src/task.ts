import type { CancelSignal } from './cancel'
import type { DurableExecutionError, DurableExecutionErrorStorageValue } from './errors'

/**
 * A task that can be run using a durable executor. See the
 * [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
 *
 * @category Task
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type Task<TInput, TOutput> = {
  id: string
}

/**
 * Infer the input type of a task.
 *
 * @category Task
 */
export type InferTaskInput<TTask extends Task<unknown, unknown>> =
  TTask extends Task<infer I, unknown> ? I : never

/**
 * Infer the output type of a task.
 *
 * @category Task
 */
export type InferTaskOutput<TTask extends Task<unknown, unknown>> =
  TTask extends Task<unknown, infer O> ? O : never

/**
 * Common options for a task. These options are used by both {@link TaskOptions} and
 * {@link ParentTaskOptions}.
 *
 * @category Task
 */
export type TaskCommonOptions = {
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
   * The delay before running the task run function. If the value is < 0 or undefined, it will be
   * treated as 0.
   */
  sleepMsBeforeRun?: number
  /**
   * The timeout for the task run function. If a value < 0 is returned, the task will be marked as
   * failed and will not be retried.
   */
  timeoutMs: number
}

/**
 * The options for retrying a task. The delay after nth retry is calculated as:
 * `baseDelayMs * (delayMultiplier ** n)`. The delay is capped at `maxDelayMs` if provided.
 *
 * @category Task
 */
export type TaskRetryOptions = {
  /**
   * The maximum number of times the task can be retried.
   */
  maxAttempts: number
  /**
   * The base delay before each retry. Defaults to 1000 or 1 second.
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
 * registered with the same id, the second one will overwrite the first one, even if the first one
 * is enqueued.
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
 * ```
 *
 * @category Task
 */
export type TaskOptions<TInput = undefined, TOutput = unknown> = TaskCommonOptions & {
  /**
   * The task run logic. It returns the output.
   *
   * Behavior on throwing errors:
   * - If the task throws an error or a `{@link ExecutionError}`, the task will be marked as
   * failed
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
 * Options for a parent task that can be run using a durable executor. It is similar to
 * {@link TaskOptions} but it returns children tasks to be run in parallel after the run
 * function completes, along with the output of the parent task.
 *
 * The `runParent` function is similar to the `run` function in {@link TaskOptions}, but the
 * output is of the form `{ output: TRunOutput, childrenTasks: Array<ChildTask> }` where the
 * children are the tasks to be run in parallel after the run function completes.
 *
 * The `finalizeTask` task is run after the runParent function and all the children tasks complete.
 * It is useful for combining the output of the runParent function and children tasks. It's input
 * has the following properties:
 *
 * - `input`: The input of the `finalizeTask` task. Same as the input of runParent function
 * - `output`: The output of the runParent function
 * - `childrenTaskExecutionsOutputs`: The outputs of the children tasks
 *
 * If `finalizeTask` is provided, the output of the whole task is the output of the `finalizeTask`
 * task. If it is not provided, the output of the whole task is the output of the form
 * `{ output: TRunOutput, childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput> }`.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
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
 * ```
 *
 * @category Task
 */
export type ParentTaskOptions<
  TInput = undefined,
  TRunOutput = unknown,
  TOutput = {
    output: TRunOutput
    childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
  },
  TFinalizeTaskRunOutput = unknown,
> = TaskCommonOptions & {
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
        childrenTasks?: Array<ChildTask>
      }
    | Promise<{
        output: TRunOutput
        childrenTasks?: Array<ChildTask>
      }>
  /**
   * Task to run after the runParent function and children tasks complete. This is useful for
   * combining the output of the run function and children tasks.
   */
  finalizeTask?: FinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>
}

/**
 * Options for the `finalizeTask` property in {@link ParentTaskOptions}. It is similar to
 * {@link TaskOptions} or {@link ParentTaskOptions} but the input is of the form:
 *
 * ```ts
 * {
 *   input: TRunInput,
 *   output: TRunOutput,
 *   childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
 * }
 * ```
 *
 * No validation is done on the input and the output of the parent task is the output of the
 * `finalizeTask` task.
 *
 * @category Task
 */
export type FinalizeTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
> =
  | TaskOptions<FinalizeTaskInput<TInput, TRunOutput>, TOutput>
  | ParentTaskOptions<FinalizeTaskInput<TInput, TRunOutput>, TFinalizeTaskRunOutput, TOutput>

export function isFinalizeTaskOptionsTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: FinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is TaskOptions<FinalizeTaskInput<TInput, TRunOutput>, TOutput> {
  return 'run' in options && !('runParent' in options)
}

export function isFinalizeTaskOptionsParentTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: FinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is ParentTaskOptions<
  FinalizeTaskInput<TInput, TRunOutput>,
  TFinalizeTaskRunOutput,
  TOutput
> {
  return 'runParent' in options && !('run' in options)
}

/**
 * The input type for the finalize task.
 *
 * @category Task
 */
export type FinalizeTaskInput<TInput = unknown, TRunOutput = unknown> = {
  input: TInput
  output: TRunOutput
  childrenTaskExecutionsOutputs: Array<ChildTaskExecutionOutput>
}

/**
 * The context object passed to a task when it is run.
 *
 * @category Task
 */
export type TaskRunContext = {
  /**
   * The task id.
   */
  taskId: string
  /**
   * The task execution id.
   */
  executionId: string
  /**
   * The cancel signal of the task. It can be used to gracefully shutdown the task run function
   * when the task has been cancelled.
   */
  cancelSignal: CancelSignal
  /**
   * The shutdown signal of the executor. It is cancelled when the executor is shutting down.
   * It can be used to gracefully shutdown the task when executor is shutting down.
   */
  shutdownSignal: CancelSignal
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
 * An execution of a task. See
 * [Task execution](https://gpahal.github.io/durable-execution/index.html#task-execution) docs for
 * more details on how task executions work.
 *
 * @category Task
 */
export type TaskExecution<TOutput = unknown> =
  | TaskReadyExecution
  | TaskRunningExecution
  | TaskFailedExecution
  | TaskTimedOutExecution
  | TaskWaitingForChildrenTasksExecution
  | TaskChildrenTasksFailedExecution
  | TaskWaitingForFinalizeTaskExecution
  | TaskFinalizeTaskFailedExecution
  | TaskCompletedExecution<TOutput>
  | TaskCancelledExecution

/**
 * A finished execution of a task. See
 * [Task execution](https://gpahal.github.io/durable-execution/index.html#task-execution) docs for
 * more details on how task executions work.
 *
 * @category Task
 */
export type TaskFinishedExecution<TOutput = unknown> =
  | TaskFailedExecution
  | TaskTimedOutExecution
  | TaskChildrenTasksFailedExecution
  | TaskFinalizeTaskFailedExecution
  | TaskCompletedExecution<TOutput>
  | TaskCancelledExecution

/**
 * A task execution that is ready to be run.
 *
 * @category Task
 */
export type TaskReadyExecution = {
  rootTaskExecution?: {
    taskId: string
    executionId: string
  }

  parentTaskExecution?: {
    taskId: string
    executionId: string
  }

  taskId: string
  executionId: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  runInput: unknown
  error?: DurableExecutionErrorStorageValue
  status: 'ready'
  retryAttempts: number
  createdAt: Date
  updatedAt: Date
}

/**
 * A task execution that is running.
 *
 * @category Task
 */
export type TaskRunningExecution = Omit<TaskReadyExecution, 'status'> & {
  status: 'running'
  startedAt: Date
  expiresAt: Date
}

/**
 * A task execution that failed while running.
 *
 * @category Task
 */
export type TaskFailedExecution = Omit<TaskRunningExecution, 'status' | 'error'> & {
  status: 'failed'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that timed out while running.
 *
 * @category Task
 */
export type TaskTimedOutExecution = Omit<TaskRunningExecution, 'status' | 'error'> & {
  status: 'timed_out'
  error: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that is waiting for children tasks to complete.
 *
 * @category Task
 */
export type TaskWaitingForChildrenTasksExecution = Omit<
  TaskRunningExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_children_tasks'
  runOutput: unknown
  childrenTaskExecutionsCompletedCount: number
  childrenTaskExecutions: Array<ChildTaskExecution>
}

/**
 * A task execution that failed while waiting for children tasks to complete because of one or more
 * child task executions failed.
 *
 * @category Task
 */
export type TaskChildrenTasksFailedExecution = Omit<
  TaskWaitingForChildrenTasksExecution,
  'status'
> & {
  status: 'children_tasks_failed'
  childrenTaskExecutionsErrors: Array<ChildTaskExecutionErrorStorageValue>
  finishedAt: Date
}

/**
 * A task execution that is waiting for the finalize task to complete.
 *
 * @category Task
 */
export type TaskWaitingForFinalizeTaskExecution = Omit<
  TaskWaitingForChildrenTasksExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_finalize_task'
  finalizeTaskExecution: ChildTaskExecution
}

/**
 * A task execution that failed while waiting for the finalize task to complete because the
 * finalize task execution failed.
 *
 * @category Task
 */
export type TaskFinalizeTaskFailedExecution = Omit<
  TaskWaitingForFinalizeTaskExecution,
  'status'
> & {
  status: 'finalize_task_failed'
  finalizeTaskExecutionError: DurableExecutionErrorStorageValue
  finishedAt: Date
}

/**
 * A task execution that completed successfully.
 *
 * @category Task
 */
export type TaskCompletedExecution<TOutput = unknown> = Omit<
  TaskWaitingForChildrenTasksExecution,
  'status' | 'output'
> & {
  status: 'completed'
  output: TOutput
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task.
   */
  finalizeTaskExecution?: ChildTaskExecution
  finishedAt: Date
}

/**
 * A task execution that was cancelled. This can happen when a task is cancelled by using
 * the cancel method of the task handle or when the task is cancelled because it's parent task
 * failed.
 *
 * @category Task
 */
export type TaskCancelledExecution = Omit<TaskRunningExecution, 'status' | 'error'> & {
  status: 'cancelled'
  error: DurableExecutionErrorStorageValue
  /**
   * The output of the task. This is only present for tasks whose run method completed
   * successfully.
   */
  runOutput?: unknown
  /**
   * The number of children task executions that have been completed.
   */
  childrenTaskExecutionsCompletedCount: number
  /**
   * The children task executions that were running when the task was cancelled. This is only present for
   * tasks whose run method completed successfully.
   */
  childrenTaskExecutions?: Array<ChildTaskExecution>
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task and
   * whose run method completed successfully.
   */
  finalizeTaskExecution?: ChildTaskExecution
  finishedAt: Date
}

/**
 * A child task of a task.
 *
 * @category Task
 */
export type ChildTask<TInput = unknown, TOutput = unknown> = {
  task: Task<TInput, TOutput>
  input: TInput
  options?: TaskEnqueueOptions
}

/**
 * An execution of a child task.
 *
 * @category Task
 */
export type ChildTaskExecution = {
  taskId: string
  executionId: string
}

/**
 * A child task execution output.
 *
 * @category Task
 */
export type ChildTaskExecutionOutput<TOutput = unknown> = {
  index: number
  taskId: string
  executionId: string
  output: TOutput
}

/**
 * A child task execution error.
 *
 * @category Task
 */
export type ChildTaskExecutionError = {
  index: number
  taskId: string
  executionId: string
  error: DurableExecutionError
}

/**
 * A storage value for a child task execution error.
 *
 * @category Task
 */
export type ChildTaskExecutionErrorStorageValue = {
  index: number
  taskId: string
  executionId: string
  error: DurableExecutionErrorStorageValue
}

/**
 * A storage value for the status of a task.
 *
 * @category Storage
 */
export type TaskExecutionStatusStorageValue =
  | 'ready'
  | 'running'
  | 'failed'
  | 'timed_out'
  | 'waiting_for_children_tasks'
  | 'children_tasks_failed'
  | 'waiting_for_finalize_task'
  | 'finalize_task_failed'
  | 'completed'
  | 'cancelled'

export const ALL_TASK_EXECUTION_STATUSES_STORAGE_VALUES = [
  'ready',
  'running',
  'failed',
  'timed_out',
  'waiting_for_children_tasks',
  'children_tasks_failed',
  'waiting_for_finalize_task',
  'finalize_task_failed',
  'completed',
  'cancelled',
] as Array<TaskExecutionStatusStorageValue>
export const ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_VALUES = [
  'ready',
  'running',
  'waiting_for_children_tasks',
  'waiting_for_finalize_task',
] as Array<TaskExecutionStatusStorageValue>
export const FINISHED_TASK_EXECUTION_STATUSES_STORAGE_VALUES = [
  'failed',
  'timed_out',
  'children_tasks_failed',
  'finalize_task_failed',
  'completed',
  'cancelled',
] as Array<TaskExecutionStatusStorageValue>

/**
 * The options for enqueuing a task. If provided, the task will be enqueued with the given
 * options. If not provided, the task will be enqueued with the default options provided in the
 * task options.
 *
 * @category Task
 */
export type TaskEnqueueOptions = {
  retryOptions?: TaskRetryOptions
  sleepMsBeforeRun?: number
  timeoutMs?: number
}

/**
 * A handle to a task execution. See
 * [Task execution](https://gpahal.github.io/durable-execution/index.html#task-execution) docs for
 * more details on how task executions work.
 *
 * @example
 * ```ts
 * const handle = await executor.enqueueTask(uploadFile, {filePath: 'file.txt'})
 * // Get the task execution
 * const execution = await handle.getExecution()
 *
 * // Wait for the task execution to be finished and get it
 * const finishedExecution = await handle.waitAndGetExecution()
 * if (finishedExecution.status === 'completed') {
 *   // Do something with the result
 * } else if (finishedExecution.status === 'failed') {
 *   // Do something with the error
 * } else if (finishedExecution.status === 'timed_out') {
 *   // Do something with the timeout
 * } else if (finishedExecution.status === 'cancelled') {
 *   // Do something with the cancellation
 * } else if (finishedExecution.status === 'children_tasks_failed') {
 *   // Do something with the children tasks failure
 * } else if (finishedExecution.status === 'finalize_task_failed') {
 *   // Do something with the finalize task failure
 * }
 *
 * // Cancel the task execution
 * await handle.cancel()
 * ```
 *
 * @category Task
 */
export type TaskExecutionHandle<TOutput = unknown> = {
  /**
   * Get the task id of the task.
   *
   * @returns The task id of the task.
   */
  getTaskId: () => string
  /**
   * Get the execution id of the task execution.
   *
   * @returns The execution id of the task execution.
   */
  getExecutionId: () => string
  /**
   * Get the task execution.
   *
   * @returns The task execution.
   */
  getExecution: () => Promise<TaskExecution<TOutput>>
  /**
   * Wait for the task execution to be finished and get it.
   *
   * @param options - The options for waiting for the task execution.
   * @returns The task execution.
   */
  waitAndGetFinishedExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    pollingIntervalMs?: number
  }) => Promise<TaskFinishedExecution<TOutput>>
  /**
   * Cancel the task execution.
   */
  cancel: () => Promise<void>
}

/**
 * The type of a sequence of tasks. Disallows empty sequences and sequences with tasks that have
 * different input and output types.
 *
 * @category Task
 */
export type SequentialTasks<T extends ReadonlyArray<Task<unknown, unknown>>> = T extends readonly []
  ? never
  : SequentialTasksHelper<T>

/**
 * A helper type to create a sequence of tasks. See {@link SequentialTasks} for more details.
 *
 * @category Task
 */
export type SequentialTasksHelper<T extends ReadonlyArray<Task<unknown, unknown>>> =
  T extends readonly []
    ? T
    : T extends readonly [Task<infer _I1, infer _O1>]
      ? T
      : T extends readonly [Task<infer I1, infer O1>, Task<infer I2, infer O2>, ...infer Rest]
        ? O1 extends I2
          ? Rest extends ReadonlyArray<Task<unknown, unknown>>
            ? readonly [Task<I1, O1>, ...SequentialTasksHelper<readonly [Task<I2, O2>, ...Rest]>]
            : never
          : never
        : T

/**
 * The type of the last element of an array of tasks.
 *
 * @category Task
 */
export type LastTaskElementInArray<T extends ReadonlyArray<Task<unknown, unknown>>> =
  T extends readonly [...Array<Task<unknown, unknown>>, infer L] ? L : never
