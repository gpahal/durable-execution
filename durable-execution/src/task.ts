import type { CancelSignal } from './cancel'
import type {
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionErrorStorageObject,
  DurableExecutionTimedOutError,
} from './errors'

/**
 * A durable task that can be run using a durable executor. See the
 * [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
 *
 * @category Task
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type DurableTask<TInput, TOutput> = {
  id: string
}

/**
 * Common options for a durable task. These options are used by both {@link DurableTaskOptions} and
 * {@link DurableParentTaskOptions}.
 *
 * @category Task
 */
export type DurableTaskCommonOptions = {
  /**
   * A unique identifier for the task. Can only contain alphanumeric characters and underscores.
   * The identifier must be unique among all the durable tasks in the same durable executor.
   */
  id: string
  /**
   * The options for retrying the task.
   */
  retryOptions?: DurableTaskRetryOptions
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
 * The options for retrying a durable task. The delay after nth retry is calculated as:
 * `baseDelayMs * (delayMultiplier ** n)`. The delay is capped at `maxDelayMs` if provided.
 *
 * @category Task
 */
export type DurableTaskRetryOptions = {
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
 * Options for a durable task that can be run using a durable executor. A task is resilient to task
 * failures, process failures, network connectivity issues, and other transient errors. The task
 * should be idempotent as it may be run multiple times if there is a process failure or if the
 * task is retried.
 *
 * When enqueued with an executor, a {@link DurableTaskHandle} is returned. It supports getting
 * the execution status, waiting for the task to complete, and cancelling the task.
 *
 * The output of the {@link run} function is the output of the task.
 *
 * The input and output are serialized and deserialized using the serializer passed to the durable
 * executor.
 *
 * Make sure the id is unique among all the durable tasks in the same durable executor. If two
 * tasks are registered with the same id, the second one will overwrite the first one, even if the
 * first one is enqueued.
 *
 * The tasks can be added to an executor using the {@link DurableExecutor.task} method. If the input
 * to a task needs to be validated, it can be done using the {@link DurableExecutor.validateInput}
 * or {@link DurableExecutor.inputSchema} methods.
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
export type DurableTaskOptions<TInput = unknown, TOutput = unknown> = DurableTaskCommonOptions & {
  /**
   * The task run logic. It returns the output.
   *
   * Behavior on throwing errors:
   * - If the task throws an error or a `{@link DurableExecutionError}`, the task will be marked as
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
  run: (ctx: DurableTaskRunContext, input: TInput) => TOutput | Promise<TOutput>
}

/**
 * Options for a durable parent task that can be run using a durable executor. It is similar to
 * {@link DurableTaskOptions} but it returns children tasks to be run in parallel after the run
 * function completes, along with the output of the parent task.
 *
 * The {@link runParent} function is similar to the `run` function in {@link DurableTaskOptions},
 * but the output is of the form `{ output: TRunOutput, childrenTasks: Array<DurableChildTask> }` where
 * the children are the tasks to be run in parallel after the run function completes.
 *
 * The {@link finalizeTask} task is run after the runParent function and all the children tasks
 * complete. It is useful for combining the output of the runParent function and children tasks.
 * It's input has the following properties:
 *
 * - `input`: The input of the `finalizeTask` task. Same as the input of runParent function
 * - `output`: The output of the runParent function
 * - `childrenTasksOutputs`: The outputs of the children tasks
 *
 * If {@link finalizeTask} is provided, the output of the whole task is the output of the
 * {@link finalizeTask} task. If it is not provided, the output of the whole task is the output of
 * the form `{ output: TRunOutput, childrenTasksOutputs: Array<DurableChildTaskExecutionOutput> }`.
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
 * ```
 *
 * @category Task
 */
export type DurableParentTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = {
    output: TRunOutput
    childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
  },
  TFinalizeTaskRunOutput = unknown,
> = DurableTaskCommonOptions & {
  /**
   * The task run logic. It is similar to the `run` function in {@link DurableTaskOptions} but it
   * returns the output and children tasks to be run in parallel after the run function completes.
   *
   * @param ctx - The context object to the task.
   * @param input - The input of the task.
   * @returns The output of the task and children tasks to be run in parallel after the run
   * function completes.
   */
  runParent: (
    ctx: DurableTaskRunContext,
    input: TInput,
  ) =>
    | {
        output: TRunOutput
        childrenTasks?: Array<DurableChildTask>
      }
    | Promise<{
        output: TRunOutput
        childrenTasks?: Array<DurableChildTask>
      }>
  /**
   * Task to run after the runParent function and children tasks complete. This is useful for
   * combining the output of the run function and children tasks.
   */
  finalizeTask?: DurableFinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>
}

/**
 * Options for the `finalizeTask` property in {@link DurableParentTaskOptions}. It is similar to
 * {@link DurableTaskOptions} or {@link DurableParentTaskOptions} but the input is of the form:
 *
 * ```ts
 * {
 *   input: TRunInput,
 *   output: TRunOutput,
 *   childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
 * }
 * ```
 *
 * No validation is done on the input and the output of the parent task is the output of the
 * `finalizeTask` task.
 *
 * @category Task
 */
export type DurableFinalizeTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
> =
  | DurableTaskOptions<DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>, TOutput>
  | DurableParentTaskOptions<
      DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>,
      TFinalizeTaskRunOutput,
      TOutput
    >

export function isDurableFinalizeTaskOptionsTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: DurableFinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is DurableTaskOptions<DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>, TOutput> {
  return 'run' in options && !('runParent' in options)
}

export function isDurableFinalizeTaskOptionsParentTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TFinalizeTaskRunOutput = unknown,
>(
  options: DurableFinalizeTaskOptions<TInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
): options is DurableParentTaskOptions<
  DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>,
  TFinalizeTaskRunOutput,
  TOutput
> {
  return 'runParent' in options && !('run' in options)
}

/**
 * The input type for the on children complete task.
 *
 * @category Task
 */
export type DurableTaskOnChildrenCompleteInput<TInput = unknown, TRunOutput = unknown> = {
  input: TInput
  output: TRunOutput
  childrenTasksOutputs: Array<DurableChildTaskExecutionOutput>
}

/**
 * The context object passed to a durable task when it is run.
 *
 * @category Task
 */
export type DurableTaskRunContext = {
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
  prevError?: DurableExecutionError
}

/**
 * An execution of a durable task. See
 * [Durable task execution](https://gpahal.github.io/durable-execution/index.html#durable-task-execution)
 * docs for more details on how task executions work.
 *
 * @category Task
 */
export type DurableTaskExecution<TOutput = unknown> =
  | DurableTaskReadyExecution
  | DurableTaskRunningExecution
  | DurableTaskFailedExecution
  | DurableTaskTimedOutExecution
  | DurableTaskWaitingForChildrenTasksExecution
  | DurableTaskChildrenTasksFailedExecution
  | DurableTaskWaitingForFinalizeTaskExecution
  | DurableTaskFinalizeTaskFailedExecution
  | DurableTaskCompletedExecution<TOutput>
  | DurableTaskCancelledExecution

/**
 * A finished execution of a durable task. See
 * [Durable task execution](https://gpahal.github.io/durable-execution/index.html#durable-task-execution)
 * docs for more details on how task executions work.
 *
 * @category Task
 */
export type DurableTaskFinishedExecution<TOutput = unknown> =
  | DurableTaskFailedExecution
  | DurableTaskTimedOutExecution
  | DurableTaskChildrenTasksFailedExecution
  | DurableTaskFinalizeTaskFailedExecution
  | DurableTaskCompletedExecution<TOutput>
  | DurableTaskCancelledExecution

/**
 * A durable task execution that is ready to be run.
 *
 * @category Task
 */
export type DurableTaskReadyExecution = {
  rootTask?: {
    taskId: string
    executionId: string
  }

  parentTask?: {
    taskId: string
    executionId: string
  }

  taskId: string
  executionId: string
  retryOptions: DurableTaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  runInput: unknown
  error?: DurableExecutionError
  status: 'ready'
  retryAttempts: number
  createdAt: Date
  updatedAt: Date
}

/**
 * A durable task execution that is running.
 *
 * @category Task
 */
export type DurableTaskRunningExecution = Omit<DurableTaskReadyExecution, 'status'> & {
  status: 'running'
  startedAt: Date
  expiresAt: Date
}

/**
 * A durable task execution that failed while running.
 *
 * @category Task
 */
export type DurableTaskFailedExecution = Omit<DurableTaskRunningExecution, 'status' | 'error'> & {
  status: 'failed'
  error: DurableExecutionError
  finishedAt: Date
}

/**
 * A durable task execution that timed out while running.
 *
 * @category Task
 */
export type DurableTaskTimedOutExecution = Omit<DurableTaskRunningExecution, 'status' | 'error'> & {
  status: 'timed_out'
  error: DurableExecutionTimedOutError
  finishedAt: Date
}

/**
 * A durable task execution that is waiting for children tasks to complete.
 *
 * @category Task
 */
export type DurableTaskWaitingForChildrenTasksExecution = Omit<
  DurableTaskRunningExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_children_tasks'
  runOutput: unknown
  childrenTasks: Array<DurableChildTaskExecution>
}

/**
 * A durable task execution that failed while waiting for children tasks to complete because of one
 * or more child task executions failed.
 *
 * @category Task
 */
export type DurableTaskChildrenTasksFailedExecution = Omit<
  DurableTaskWaitingForChildrenTasksExecution,
  'status'
> & {
  status: 'children_tasks_failed'
  childrenTasksErrors: Array<DurableChildTaskExecutionError>
  finishedAt: Date
}

/**
 * A durable task execution that is waiting for the finalize task to complete.
 *
 * @category Task
 */
export type DurableTaskWaitingForFinalizeTaskExecution = Omit<
  DurableTaskWaitingForChildrenTasksExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_finalize_task'
  finalizeTask: DurableChildTaskExecution
}

/**
 * A durable task execution that failed while waiting for the finalize task to complete because the
 * finalize task execution failed.
 *
 * @category Task
 */
export type DurableTaskFinalizeTaskFailedExecution = Omit<
  DurableTaskWaitingForFinalizeTaskExecution,
  'status'
> & {
  status: 'finalize_task_failed'
  finalizeTaskError: DurableExecutionError
  finishedAt: Date
}

/**
 * A durable task execution that completed successfully.
 *
 * @category Task
 */
export type DurableTaskCompletedExecution<TOutput = unknown> = Omit<
  DurableTaskWaitingForChildrenTasksExecution,
  'status' | 'output'
> & {
  status: 'completed'
  output: TOutput
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task.
   */
  finalizeTask?: DurableChildTaskExecution
  finishedAt: Date
}

/**
 * A durable task execution that was cancelled. This can happen when a task is cancelled by using
 * the cancel method of the task handle or when the task is cancelled because it's parent task
 * failed.
 *
 * @category Task
 */
export type DurableTaskCancelledExecution = Omit<
  DurableTaskRunningExecution,
  'status' | 'error'
> & {
  status: 'cancelled'
  error: DurableExecutionCancelledError
  /**
   * The output of the task. This is only present for tasks whose run method completed
   * successfully.
   */
  runOutput?: unknown
  /**
   * The children tasks that were running when the task was cancelled. This is only present for
   * tasks whose run method completed successfully.
   */
  childrenTasks?: Array<DurableChildTaskExecution>
  /**
   * The finalize task execution. This is only present for tasks which have a finalize task and
   * whose run method completed successfully.
   */
  finalizeTask?: DurableChildTaskExecution
  finishedAt: Date
}

/**
 * A child task of a durable task.
 *
 * @category Task
 */
export type DurableChildTask<TInput = unknown, TOutput = unknown> = {
  task: DurableTask<TInput, TOutput>
  input: TInput
  options?: DurableTaskEnqueueOptions
}

/**
 * An execution of a child task of a durable task.
 *
 * @category Task
 */
export type DurableChildTaskExecution = {
  taskId: string
  executionId: string
}

/**
 * A child task output of a durable task.
 *
 * @category Task
 */
export type DurableChildTaskExecutionOutput<TOutput = unknown> = {
  index: number
  taskId: string
  executionId: string
  output: TOutput
}

/**
 * A child task error of a durable task.
 *
 * @category Task
 */
export type DurableChildTaskExecutionError = {
  index: number
  taskId: string
  executionId: string
  error: DurableExecutionError
}

/**
 * A storage object for a child task execution error of a durable task execution.
 *
 * @category Task
 */
export type DurableChildTaskExecutionErrorStorageObject = {
  index: number
  taskId: string
  executionId: string
  error: DurableExecutionErrorStorageObject
}

/**
 * A storage object for the status of a durable task execution.
 *
 * @category Storage
 */
export type DurableTaskExecutionStatusStorageObject =
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

export const ALL_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS = [
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
] as Array<DurableTaskExecutionStatusStorageObject>
export const ACTIVE_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS = [
  'ready',
  'running',
  'waiting_for_children_tasks',
  'waiting_for_finalize_task',
] as Array<DurableTaskExecutionStatusStorageObject>
export const FINISHED_TASK_EXECUTION_STATUSES_STORAGE_OBJECTS = [
  'failed',
  'timed_out',
  'children_tasks_failed',
  'finalize_task_failed',
  'completed',
  'cancelled',
] as Array<DurableTaskExecutionStatusStorageObject>

/**
 * The options for enqueuing a task. If provided, the task will be enqueued with the given
 * options. If not provided, the task will be enqueued with the default options provided in the
 * task options.
 *
 * @category Task
 */
export type DurableTaskEnqueueOptions = {
  retryOptions?: DurableTaskRetryOptions
  sleepMsBeforeRun?: number
  timeoutMs?: number
}

/**
 * A handle to a durable task execution. See
 * [Durable task execution](https://gpahal.github.io/durable-execution/index.html#durable-task-execution)
 * docs for more details on how task executions work.
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
export type DurableTaskHandle<TOutput = unknown> = {
  /**
   * Get the task id of the durable task.
   *
   * @returns The task id of the durable task.
   */
  getTaskId: () => string
  /**
   * Get the execution id of the durable task execution.
   *
   * @returns The execution id of the durable task execution.
   */
  getTaskExecutionId: () => string
  /**
   * Get the durable task execution.
   *
   * @returns The durable task execution.
   */
  getTaskExecution: () => Promise<DurableTaskExecution<TOutput>>
  /**
   * Wait for the durable task execution to be finished and get it.
   *
   * @param options - The options for waiting for the durable task execution.
   * @returns The durable task execution.
   */
  waitAndGetTaskFinishedExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    pollingIntervalMs?: number
  }) => Promise<DurableTaskFinishedExecution<TOutput>>
  /**
   * Cancel the durable task execution.
   */
  cancel: () => Promise<void>
}
