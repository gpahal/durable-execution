import * as v from 'valibot'

import { getErrorMessage } from '@gpahal/std/errors'
import { isFunction } from '@gpahal/std/functions'

import { createCancellablePromise, createTimeoutCancelSignal, type CancelSignal } from './cancel'
import {
  DurableTaskError,
  DurableTaskTimedOutError,
  type DurableTaskCancelledError,
} from './errors'
import { generateId } from './utils'

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

const vMaxRetryAttempts = v.pipe(
  v.nullish(v.pipe(v.number(), v.integer())),
  v.transform((val) => {
    if (val == null || val < 0) {
      return 0
    }
    return val
  }),
)

const vTimeoutMs = v.pipe(v.number(), v.integer(), v.minValue(1))

const vSleepMsBeforeAttempt = v.pipe(
  v.nullish(v.pipe(v.number(), v.integer())),
  v.transform((val) => {
    if (val == null || val <= 0) {
      return 0
    }
    return val
  }),
)

export class DurableTaskInternal {
  private readonly taskInternalsMap: Map<string, DurableTaskInternal>

  readonly id: string
  private readonly timeoutMs: number | ((attempt: number) => number)
  readonly maxRetryAttempts: number
  private readonly sleepMsBeforeAttempt: number | ((attempt: number) => number)
  private readonly validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  readonly disableChildrenOutputsInOutput: boolean
  private readonly runParent: (
    ctx: DurableTaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    children: Array<DurableTaskChild>
  }>
  readonly onRunAndChildrenComplete: DurableTaskInternal | undefined

  constructor(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    id: string,
    timeoutMs: number | ((attempt: number) => number),
    maxRetryAttempts: number,
    sleepMsBeforeAttempt: number | ((attempt: number) => number),
    validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
    disableChildrenOutputsInOutput: boolean,
    runParent: (
      ctx: DurableTaskRunContext,
      input: unknown,
    ) => Promise<{
      output: unknown
      children: Array<DurableTaskChild>
    }>,
    onRunAndChildrenComplete?: DurableTaskInternal,
  ) {
    this.taskInternalsMap = taskInternalsMap
    this.id = id
    this.timeoutMs = timeoutMs
    this.maxRetryAttempts = maxRetryAttempts
    this.sleepMsBeforeAttempt = sleepMsBeforeAttempt
    this.validateInputFn = validateInputFn
    this.disableChildrenOutputsInOutput = disableChildrenOutputsInOutput
    this.runParent = runParent
    this.onRunAndChildrenComplete = onRunAndChildrenComplete
  }

  static validateCommonTaskOptions(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: Omit<DurableTaskOptions<unknown, unknown>, 'run'>,
  ): {
    id: string
    timeoutMs: number | ((attempt: number) => number)
    maxRetryAttempts: number
    sleepMsBeforeAttempt: number | ((attempt: number) => number)
  } {
    validateTaskId(taskOptions.id)
    if (taskInternalsMap.has(taskOptions.id)) {
      throw new DurableTaskError(
        `Task ${taskOptions.id} already exists. Use unique ids for tasks`,
        false,
      )
    }

    const parsedMaxRetryAttempts = v.safeParse(vMaxRetryAttempts, taskOptions.maxRetryAttempts)
    if (!parsedMaxRetryAttempts.success) {
      throw new DurableTaskError(
        `Invalid max retry attempts for task ${taskOptions.id}: ${v.summarize(parsedMaxRetryAttempts.issues)}`,
        false,
      )
    }

    return {
      id: taskOptions.id,
      timeoutMs: taskOptions.timeoutMs,
      maxRetryAttempts: parsedMaxRetryAttempts.output,
      sleepMsBeforeAttempt: taskOptions.sleepMsBeforeAttempt ?? 0,
    }
  }

  static fromDurableTaskOptions<TRunInput, TInput, TOutput>(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: DurableTaskOptions<TRunInput, TOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): DurableTaskInternal {
    const commonOptions = DurableTaskInternal.validateCommonTaskOptions(
      taskInternalsMap,
      taskOptions,
    )

    const runParent = async (ctx: DurableTaskRunContext, input: TRunInput) => {
      const output = await taskOptions.run(ctx, input)
      return {
        output,
        children: [],
      }
    }

    const taskInternal = new DurableTaskInternal(
      taskInternalsMap,
      commonOptions.id,
      commonOptions.timeoutMs,
      commonOptions.maxRetryAttempts,
      commonOptions.sleepMsBeforeAttempt,
      validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      true,
      runParent as (
        ctx: DurableTaskRunContext,
        input: unknown,
      ) => Promise<{
        output: unknown
        children: Array<DurableTaskChild>
      }>,
      undefined,
    )
    taskInternalsMap.set(taskInternal.id, taskInternal)
    return taskInternal
  }

  static fromDurableParentTaskOptions<
    TRunInput,
    TInput = TRunInput,
    TRunOutput = unknown,
    TOutput = unknown,
  >(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: DurableParentTaskOptions<TRunInput, TRunOutput, TOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): DurableTaskInternal {
    const commonOptions = DurableTaskInternal.validateCommonTaskOptions(
      taskInternalsMap,
      taskOptions,
    )

    const runParent = async (ctx: DurableTaskRunContext, input: TRunInput) => {
      return await taskOptions.runParent(ctx, input)
    }

    let onRunAndChildrenComplete: DurableTaskInternal | undefined
    if (taskOptions.onRunAndChildrenComplete) {
      onRunAndChildrenComplete = isDurableOnRunAndChildrenCompleteTaskOptionsParentTaskOptions(
        taskOptions.onRunAndChildrenComplete,
      )
        ? DurableTaskInternal.fromDurableParentTaskOptions(
            taskInternalsMap,
            taskOptions.onRunAndChildrenComplete,
          )
        : isDurableOnRunAndChildrenCompleteTaskOptionsTaskOptions(
              taskOptions.onRunAndChildrenComplete,
            )
          ? DurableTaskInternal.fromDurableTaskOptions(
              taskInternalsMap,
              taskOptions.onRunAndChildrenComplete,
            )
          : undefined
    }

    const taskInternal = new DurableTaskInternal(
      taskInternalsMap,
      commonOptions.id,
      commonOptions.timeoutMs,
      commonOptions.maxRetryAttempts,
      commonOptions.sleepMsBeforeAttempt,
      validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      false,
      runParent as (
        ctx: DurableTaskRunContext,
        input: unknown,
      ) => Promise<{
        output: unknown
        children: Array<DurableTaskChild>
      }>,
      onRunAndChildrenComplete,
    )
    taskInternalsMap.set(taskInternal.id, taskInternal)
    return taskInternal
  }

  getTimeoutMs(attempt: number): number {
    let timeoutMs: unknown
    try {
      timeoutMs =
        this.timeoutMs && isFunction(this.timeoutMs) ? this.timeoutMs(attempt) : this.timeoutMs
    } catch (error) {
      throw new DurableTaskError(
        `Error in timeout function for task ${this.id} on attempt ${attempt}: ${getErrorMessage(error)}`,
        false,
      )
    }

    const parsedTimeoutMs = v.safeParse(vTimeoutMs, timeoutMs)
    if (!parsedTimeoutMs.success) {
      throw new DurableTaskError(
        `Invalid timeout value for task ${this.id} on attempt ${attempt}: ${v.summarize(parsedTimeoutMs.issues)}`,
        false,
      )
    }
    return parsedTimeoutMs.output
  }

  getSleepMsBeforeAttempt(attempt: number): number {
    let sleepMsBeforeAttempt: unknown
    try {
      sleepMsBeforeAttempt =
        this.sleepMsBeforeAttempt && isFunction(this.sleepMsBeforeAttempt)
          ? this.sleepMsBeforeAttempt(attempt)
          : this.sleepMsBeforeAttempt
    } catch {
      return 0
    }

    const parsedSleepMsBeforeAttempt = v.safeParse(vSleepMsBeforeAttempt, sleepMsBeforeAttempt)
    if (!parsedSleepMsBeforeAttempt.success) {
      return 0
    }
    return parsedSleepMsBeforeAttempt.output
  }

  async validateInput(input: unknown): Promise<unknown> {
    if (!this.validateInputFn) {
      return input
    }
    return this.validateInputFn(this.id, input)
  }

  async runWithTimeoutAndCancellation(
    ctx: DurableTaskRunContext,
    input: unknown,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    children: Array<DurableTaskChild>
  }> {
    const timeoutMs = this.getTimeoutMs(ctx.attempt)
    const timeoutCancelSignal = createTimeoutCancelSignal(timeoutMs)
    return await createCancellablePromise(
      createCancellablePromise(
        this.runParent(ctx, input),
        timeoutCancelSignal,
        new DurableTaskTimedOutError(),
      ),
      cancelSignal,
    )
  }
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
   * The timeout for the task run function. If the value is a function, it will be called with the
   * attempt number and should return the timeout in milliseconds. For the first attempt, the
   * attempt number would be 0, and then for further retries, it would be 1, 2, etc.
   *
   * If a value < 0 is returned, the task will be marked as failed and will not be retried.
   */
  timeoutMs: number | ((attempt: number) => number)
  /**
   * The maximum number of times to retry the task.
   *
   * If the value is 0, the task will not be retried. If the value is < 0 or undefined, it will be
   * treated as 0.
   */
  maxRetryAttempts?: number
  /**
   * The delay before running the task run function. If a function is provided, it will be called
   * with the attempt number and should return the delay in milliseconds. For the first attempt, the
   * attempt number would be 0, and then for further retries, it would be 1, 2, etc.
   *
   * If the value is < 0 or undefined, it will be treated as 0.
   */
  sleepMsBeforeAttempt?: number | ((attempt: number) => number)
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
   * - If the task throws an error or a `{@link DurableTaskError}`, the task will be marked as
   * failed
   * - If the task throws a `{@link DurableTaskTimedOutError}`, it will be marked as timed out
   * - If the task throws a `{@link DurableTaskCancelledError}`, it will be marked as cancelled
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
 * but the output is of the form `{ output: TRunOutput, children: Array<DurableTaskChild> }` where
 * the children are the tasks to be run in parallel after the run function completes.
 *
 * The {@link onRunAndChildrenComplete} task is run after the runParent function and all the
 * children tasks complete. It is useful for combining the output of the runParent function and
 * children tasks. It's input has the following properties:
 *
 * - `input`: The input of the `onRunAndChildrenComplete` task. Same as the input of runParent
 *   function.
 * - `output`: The output of the runParent function.
 * - `childrenOutputs`: The outputs of the children tasks.
 *
 * If {@link onRunAndChildrenComplete} is provided, the output of the whole task is the output of
 * the {@link onRunAndChildrenComplete} task. If it is not provided, the output of the whole task is
 * the output of the form
 * `{ output: TRunOutput, childrenOutputs: Array<DurableTaskChildExecutionOutput> }`.
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
 *     onRunAndChildrenComplete: {
 *       id: 'onUploadFileAndChildrenComplete',
 *       timeoutMs: 60_000, // 1 minute
 *       run: async (ctx, { input, output, childrenOutputs }) => {
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
    childrenOutputs: Array<DurableTaskChildExecutionOutput>
  },
  TOnRunAndChildrenCompleteRunOutput = unknown,
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
        children: Array<DurableTaskChild>
      }
    | Promise<{
        output: TRunOutput
        children: Array<DurableTaskChild>
      }>
  /**
   * Task to run after the runParent function and children tasks complete. This is useful for
   * combining the output of the run function and children tasks.
   */
  onRunAndChildrenComplete?: DurableOnRunAndChildrenCompleteTaskOptions<
    TInput,
    TRunOutput,
    TOutput,
    TOnRunAndChildrenCompleteRunOutput
  >
}

/**
 * Options for the `onRunAndChildrenComplete` property in {@link DurableParentTaskOptions}. It is
 * similar to {@link DurableTaskOptions} or {@link DurableParentTaskOptions} but the input is of
 * the form:
 *
 * ```ts
 * {
 *   input: TRunInput,
 *   output: TRunOutput,
 *   childrenOutputs: Array<DurableTaskChildExecutionOutput>
 * }
 * ```
 *
 * No validation is done on the input and the output of the parent task is the output of the
 * `onRunAndChildrenComplete` task.
 *
 * @category Task
 */
export type DurableOnRunAndChildrenCompleteTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TOnRunAndChildrenCompleteRunOutput = unknown,
> =
  | DurableTaskOptions<DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>, TOutput>
  | DurableParentTaskOptions<
      DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>,
      TOnRunAndChildrenCompleteRunOutput,
      TOutput
    >

function isDurableOnRunAndChildrenCompleteTaskOptionsTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TOnRunAndChildrenCompleteRunOutput = unknown,
>(
  options: DurableOnRunAndChildrenCompleteTaskOptions<
    TInput,
    TRunOutput,
    TOutput,
    TOnRunAndChildrenCompleteRunOutput
  >,
): options is DurableTaskOptions<DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>, TOutput> {
  return 'run' in options && !('runParent' in options)
}

function isDurableOnRunAndChildrenCompleteTaskOptionsParentTaskOptions<
  TInput = unknown,
  TRunOutput = unknown,
  TOutput = unknown,
  TOnRunAndChildrenCompleteRunOutput = unknown,
>(
  options: DurableOnRunAndChildrenCompleteTaskOptions<
    TInput,
    TRunOutput,
    TOutput,
    TOnRunAndChildrenCompleteRunOutput
  >,
): options is DurableParentTaskOptions<
  DurableTaskOnChildrenCompleteInput<TInput, TRunOutput>,
  TOnRunAndChildrenCompleteRunOutput,
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
  childrenOutputs: Array<DurableTaskChildExecutionOutput>
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
  prevError?: DurableTaskError
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
  | DurableTaskWaitingForChildrenExecution
  | DurableTaskChildrenFailedExecution
  | DurableTaskWaitingForOnRunAndChildrenCompleteExecution
  | DurableTaskOnRunAndChildrenCompleteFailedExecution
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
  | DurableTaskChildrenFailedExecution
  | DurableTaskOnRunAndChildrenCompleteFailedExecution
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
  runInput: unknown
  error?: DurableTaskError
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
  error: DurableTaskError
  finishedAt: Date
}

/**
 * A durable task execution that timed out while running.
 *
 * @category Task
 */
export type DurableTaskTimedOutExecution = Omit<DurableTaskRunningExecution, 'status' | 'error'> & {
  status: 'timed_out'
  error: DurableTaskTimedOutError
  finishedAt: Date
}

/**
 * A durable task execution that is waiting for children to complete.
 *
 * @category Task
 */
export type DurableTaskWaitingForChildrenExecution = Omit<
  DurableTaskRunningExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_children'
  runOutput: unknown
  children: Array<DurableTaskChildExecution>
}

/**
 * A durable task execution that failed while waiting for children to complete because of one or
 * more child task executions failed.
 *
 * @category Task
 */
export type DurableTaskChildrenFailedExecution = Omit<
  DurableTaskWaitingForChildrenExecution,
  'status'
> & {
  status: 'children_failed'
  childrenErrors: Array<DurableTaskChildExecutionError>
  finishedAt: Date
}

/**
 * A durable task execution that is waiting for the on run and children complete task to complete.
 *
 * @category Task
 */
export type DurableTaskWaitingForOnRunAndChildrenCompleteExecution = Omit<
  DurableTaskWaitingForChildrenExecution,
  'status' | 'error'
> & {
  status: 'waiting_for_on_run_and_children_complete'
  onRunAndChildrenComplete: DurableTaskChildExecution
}

/**
 * A durable task execution that failed while waiting for the on run and children complete task to
 * complete because the on run and children complete task execution failed.
 *
 * @category Task
 */
export type DurableTaskOnRunAndChildrenCompleteFailedExecution = Omit<
  DurableTaskWaitingForOnRunAndChildrenCompleteExecution,
  'status'
> & {
  status: 'on_run_and_children_complete_failed'
  onRunAndChildrenCompleteError: DurableTaskError
  finishedAt: Date
}

/**
 * A durable task execution that completed successfully.
 *
 * @category Task
 */
export type DurableTaskCompletedExecution<TOutput = unknown> = Omit<
  DurableTaskWaitingForChildrenExecution,
  'status' | 'output'
> & {
  status: 'completed'
  output: TOutput
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
  error: DurableTaskCancelledError
  /**
   * The output of the task. This is only present for tasks whose run method completed
   * successfully.
   */
  runOutput?: unknown
  /**
   * The children task executions that were running when the task was cancelled. This is only
   * present for tasks whose run method completed successfully.
   */
  children?: Array<DurableTaskChildExecution>
  finishedAt: Date
}

/**
 * A child task of a durable task.
 *
 * @category Task
 */
export type DurableTaskChild<TInput = unknown, TOutput = unknown> = {
  task: DurableTask<TInput, TOutput>
  input: TInput
}

/**
 * An execution of a child task of a durable task.
 *
 * @category Task
 */
export type DurableTaskChildExecution = {
  taskId: string
  executionId: string
}

/**
 * A child task output of a durable task.
 *
 * @category Task
 */
export type DurableTaskChildExecutionOutput<TOutput = unknown> = {
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
export type DurableTaskChildExecutionError = {
  index: number
  taskId: string
  executionId: string
  error: DurableTaskError
}

/**
 * The status of a durable task execution.
 *
 * @category Task
 */
export type DurableTaskExecutionStatus =
  | 'ready'
  | 'running'
  | 'failed'
  | 'timed_out'
  | 'waiting_for_children'
  | 'children_failed'
  | 'waiting_for_on_run_and_children_complete'
  | 'on_run_and_children_complete_failed'
  | 'completed'
  | 'cancelled'

export const ALL_TASK_EXECUTION_STATUSES = [
  'ready',
  'running',
  'failed',
  'timed_out',
  'waiting_for_children',
  'children_failed',
  'waiting_for_on_run_and_children_complete',
  'on_run_and_children_complete_failed',
  'completed',
  'cancelled',
] as Array<DurableTaskExecutionStatus>
export const ACTIVE_TASK_EXECUTION_STATUSES = [
  'ready',
  'running',
  'waiting_for_children',
  'waiting_for_on_run_and_children_complete',
] as Array<DurableTaskExecutionStatus>
export const FINISHED_TASK_EXECUTION_STATUSES = [
  'failed',
  'timed_out',
  'children_failed',
  'on_run_and_children_complete_failed',
  'completed',
  'cancelled',
] as Array<DurableTaskExecutionStatus>

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
 * } else if (finishedExecution.status === 'children_failed') {
 *   // Do something with the children failure
 * } else if (finishedExecution.status === 'on_run_and_children_complete_failed') {
 *   // Do something with the on run and children complete task failure
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

/**
 * Generate a task id.
 *
 * @returns A task id.
 *
 * @category Task
 */
export function generateTaskId(): string {
  return `t_${generateId(24)}`
}

/**
 * Generate a task execution id.
 *
 * @returns A task execution id.
 *
 * @category Task
 */
export function generateTaskExecutionId(): string {
  return `te_${generateId(24)}`
}

const _TASK_ID_REGEX = /^\w+$/

/**
 * Validate an id. Make sure it is not empty, not longer than 255 characters, and only contains
 * alphanumeric characters and underscores.
 *
 * @param id - The id to validate.
 * @throws An error if the id is invalid.
 *
 * @category Task
 */
export function validateTaskId(id: string): void {
  if (id.length === 0) {
    throw new DurableTaskError('Id cannot be empty', false)
  }
  if (id.length > 255) {
    throw new DurableTaskError('Id cannot be longer than 255 characters', false)
  }
  if (!_TASK_ID_REGEX.test(id)) {
    throw new DurableTaskError('Id can only contain alphanumeric characters and underscores', false)
  }
}
