import type { StandardSchemaV1 } from '@standard-schema/spec'
import * as v from 'valibot'

import { getErrorMessage } from '@gpahal/std/errors'
import { isFunction } from '@gpahal/std/functions'

import { createCancellablePromise, createTimeoutCancelSignal, type CancelSignal } from './cancel'
import {
  DurableTaskError,
  DurableTaskTimedOutError,
  type DurableTaskCancelledError,
} from './errors'
import { generateId, validateStandardSchema } from './utils'

/**
 * A durable task that can be run using a durable executor. See the
 * [usage](https://gpahal.github.io/durable-execution/index.html#usage) and
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) sections
 * for more details on creating and enqueuing tasks.
 *
 * @category Task
 */
export class DurableTask<
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  TOutput,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  TRunInput,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  TInput,
> {
  readonly id: string

  constructor(id: string) {
    this.id = id
  }
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

export class DurableTaskInternal<TOutput, TRunInput, TInput> {
  readonly id: string
  readonly timeoutMs: number | ((attempt: number) => number)
  readonly maxRetryAttempts: number
  readonly sleepMsBeforeAttempt: number | ((attempt: number) => number)
  readonly validateInput: (input: TInput) => Promise<TRunInput>
  readonly disableChildrenOutputsInOutput: boolean
  readonly run: (
    ctx: DurableTaskRunContext,
    input: TRunInput,
  ) => Promise<{
    output: unknown
    children: Array<DurableTaskChild>
  }>
  readonly onRunAndChildrenComplete?: DurableTaskInternal<
    TOutput,
    unknown,
    DurableTaskOnChildrenCompleteInput<unknown, TRunInput>
  >

  constructor(
    id: string,
    timeoutMs: number | ((attempt: number) => number),
    maxRetryAttempts: number,
    sleepMsBeforeAttempt: number | ((attempt: number) => number),
    validateInput: (input: TInput) => Promise<TRunInput>,
    disableChildrenOutputsInOutput: boolean,
    run: (
      ctx: DurableTaskRunContext,
      input: TRunInput,
    ) => Promise<{
      output: unknown
      children: Array<DurableTaskChild>
    }>,
    onRunAndChildrenComplete?: DurableTaskInternal<
      TOutput,
      unknown,
      DurableTaskOnChildrenCompleteInput<unknown, TRunInput>
    >,
  ) {
    this.id = id
    this.timeoutMs = timeoutMs
    this.maxRetryAttempts = maxRetryAttempts
    this.sleepMsBeforeAttempt = sleepMsBeforeAttempt
    this.validateInput = validateInput
    this.disableChildrenOutputsInOutput = disableChildrenOutputsInOutput
    this.run = run
    this.onRunAndChildrenComplete = onRunAndChildrenComplete
  }

  static fromDurableAnyTaskOptions<TOutput, TRunInput, TInput = TRunInput>(
    taskOptions: DurableAnyTaskOptions<TOutput, TRunInput, TInput>,
    taskInternalsMap: Map<string, DurableTaskInternal<unknown, unknown, unknown>>,
  ): DurableTaskInternal<TOutput, TRunInput, TInput> {
    let parentTaskOptions: DurableParentTaskOptions<unknown, TOutput, TRunInput, TInput>
    let disableChildrenOutputsInOutput = false
    const commonOptions = {
      id: taskOptions.id,
      timeoutMs: taskOptions.timeoutMs,
      maxRetryAttempts: taskOptions.maxRetryAttempts,
      sleepMsBeforeAttempt: taskOptions.sleepMsBeforeAttempt,
    } as const
    if (isDurableTaskOptions(taskOptions)) {
      parentTaskOptions = {
        ...commonOptions,
        validateInput: taskOptions.validateInput,
        runParent: async (ctx: DurableTaskRunContext, input: TRunInput) => {
          const output = await taskOptions.run(ctx, input)
          return {
            output,
            children: [],
          }
        },
        onRunAndChildrenComplete: undefined,
      }
      disableChildrenOutputsInOutput = true
    } else if (isDurableParentTaskOptions(taskOptions)) {
      parentTaskOptions = {
        ...commonOptions,
        validateInput: taskOptions.validateInput,
        runParent: taskOptions.runParent,
        onRunAndChildrenComplete: taskOptions.onRunAndChildrenComplete,
      }
    } else if (isDurableSchemaTaskOptions(taskOptions)) {
      parentTaskOptions = {
        ...commonOptions,
        validateInput: async (input: TInput) => {
          try {
            return await validateStandardSchema(taskOptions.inputSchema, input)
          } catch (error) {
            throw new DurableTaskError(
              `Invalid input for schema task ${taskOptions.id}: ${getErrorMessage(error)}`,
              false,
            )
          }
        },
        runParent: async (ctx: DurableTaskRunContext, input: TRunInput) => {
          const output = await taskOptions.run(ctx, input)
          return {
            output,
            children: [],
          }
        },
        onRunAndChildrenComplete: undefined,
      }
      disableChildrenOutputsInOutput = true
    } else if (isDurableParentSchemaTaskOptions(taskOptions)) {
      parentTaskOptions = {
        ...commonOptions,
        validateInput: async (input: TInput) => {
          try {
            return await validateStandardSchema(taskOptions.inputSchema, input)
          } catch (error) {
            throw new DurableTaskError(
              `Invalid input for schema task ${taskOptions.id}: ${getErrorMessage(error)}`,
              false,
            )
          }
        },
        runParent: taskOptions.runParent,
        onRunAndChildrenComplete: taskOptions.onRunAndChildrenComplete,
      }
    } else {
      throw new DurableTaskError(
        `Invalid task options for task ${commonOptions.id}: ${JSON.stringify(taskOptions)}`,
        false,
      )
    }

    validateTaskId(parentTaskOptions.id)
    if (taskInternalsMap.has(parentTaskOptions.id)) {
      throw new DurableTaskError(
        `Task ${parentTaskOptions.id} already exists. Use unique ids for tasks`,
        false,
      )
    }
    if (
      parentTaskOptions.onRunAndChildrenComplete &&
      taskInternalsMap.has(parentTaskOptions.onRunAndChildrenComplete.id)
    ) {
      throw new DurableTaskError(
        `Task ${parentTaskOptions.id} has a children complete task ${parentTaskOptions.onRunAndChildrenComplete.id} which already exists. Use unique ids for tasks`,
        false,
      )
    }

    const parsedMaxRetryAttempts = v.safeParse(
      vMaxRetryAttempts,
      parentTaskOptions.maxRetryAttempts,
    )
    if (!parsedMaxRetryAttempts.success) {
      throw new DurableTaskError(
        `Invalid max retry attempts for task ${parentTaskOptions.id}: ${v.summarize(parsedMaxRetryAttempts.issues)}`,
        false,
      )
    }

    const maxRetryAttempts = parsedMaxRetryAttempts.output
    const sleepMsBeforeAttempt = parentTaskOptions.sleepMsBeforeAttempt ?? 0

    const validateInput = async (input: TInput): Promise<TRunInput> => {
      if (parentTaskOptions.validateInput == null) {
        return input as unknown as TRunInput
      }

      try {
        const parsedInput = await parentTaskOptions.validateInput(input)
        return parsedInput as TRunInput
      } catch (error) {
        throw new DurableTaskError(
          `Invalid input to task ${parentTaskOptions.id}: ${getErrorMessage(error)}`,
          false,
        )
      }
    }

    const onRunAndChildrenComplete = parentTaskOptions.onRunAndChildrenComplete
      ? (DurableTaskInternal.fromDurableAnyTaskOptions(
          parentTaskOptions.onRunAndChildrenComplete,
          taskInternalsMap,
        ) as DurableTaskInternal<
          TOutput,
          unknown,
          DurableTaskOnChildrenCompleteInput<unknown, TRunInput>
        >)
      : undefined

    const taskInternal = new DurableTaskInternal<TOutput, TRunInput, TInput>(
      parentTaskOptions.id,
      parentTaskOptions.timeoutMs,
      maxRetryAttempts,
      sleepMsBeforeAttempt,
      validateInput,
      disableChildrenOutputsInOutput ?? false,
      async (ctx, input) => {
        const result = await parentTaskOptions.runParent(ctx, input)
        return {
          output: result.output,
          children: result.children ?? [],
        }
      },
      onRunAndChildrenComplete,
    )
    taskInternalsMap.set(
      parentTaskOptions.id,
      taskInternal as DurableTaskInternal<unknown, unknown, unknown>,
    )
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

  async runWithTimeoutAndCancellation(
    ctx: DurableTaskRunContext,
    input: TRunInput,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    children: Array<DurableTaskChild>
  }> {
    const timeoutMs = this.getTimeoutMs(ctx.attempt)
    const timeoutCancelSignal = createTimeoutCancelSignal(timeoutMs)
    return await createCancellablePromise(
      createCancellablePromise(
        this.run(ctx, input),
        timeoutCancelSignal,
        new DurableTaskTimedOutError(),
      ),
      cancelSignal,
    )
  }
}

/**
 * A union type of all the possible task options. It can be used to create a task with the
 * {@link DurableExecutor.task} method. See the
 * [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples) section
 * for more details on creating tasks.
 *
 * @category Task
 */
export type DurableAnyTaskOptions<TOutput, TRunInput, TInput> =
  | DurableTaskOptions<TOutput, TRunInput, TInput>
  | DurableParentTaskOptions<unknown, TOutput, TRunInput, TInput>
  | DurableSchemaTaskOptions<TOutput, StandardSchemaV1<TInput, TRunInput>>
  | DurableParentSchemaTaskOptions<unknown, TOutput, StandardSchemaV1<TInput, TRunInput>>

function isDurableTaskOptions<TOutput, TRunInput, TInput>(
  options: DurableAnyTaskOptions<TOutput, TRunInput, TInput>,
): options is DurableTaskOptions<TOutput, TRunInput, TInput> {
  return 'run' in options && !('inputSchema' in options)
}

function isDurableParentTaskOptions<TOutput, TRunInput, TInput>(
  options: DurableAnyTaskOptions<TOutput, TRunInput, TInput>,
): options is DurableParentTaskOptions<unknown, TOutput, TRunInput, TInput> {
  return 'runParent' in options && !('inputSchema' in options)
}

function isDurableSchemaTaskOptions<TOutput, TRunInput, TInput>(
  options: DurableAnyTaskOptions<TOutput, TRunInput, TInput>,
): options is DurableSchemaTaskOptions<TOutput, StandardSchemaV1<TInput, TRunInput>> {
  return 'run' in options && 'inputSchema' in options
}

function isDurableParentSchemaTaskOptions<TOutput, TRunInput, TInput>(
  options: DurableAnyTaskOptions<TOutput, TRunInput, TInput>,
): options is DurableParentSchemaTaskOptions<
  unknown,
  TOutput,
  StandardSchemaV1<TInput, TRunInput>
> {
  return 'runParent' in options && 'inputSchema' in options
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
 * The input and output are serialized/deserialized using the serializer passed to the durable
 * executor.
 *
 * Make sure the id is unique among all the durable tasks in the same durable executor. If two
 * tasks are registered with the same id, the second one will overwrite the first one, even if the
 * first one is enqueued.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
 * const extractFileTitle = durableExecutor.task({
 *   id: 'extractFileTitle',
 *   timeoutMs: 30_000, // 30 seconds
 *   run: async (ctx, input: { filePath: string }) => {
 *     // ... extract the file title
 *     return {
 *       title: 'File Title',
 *     }
 *   },
 * })
 * ```
 *
 * @category Task
 */
export type DurableTaskOptions<TOutput, TRunInput, TInput = TRunInput> = {
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
  /**
   * The function to validate the input of the task. It should throw an error if the input is
   * invalid.
   */
  validateInput?: (input: TInput) => TRunInput | Promise<TRunInput>
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
   * @param input - The input of the task.
   * @returns The output of the task.
   */
  run: (ctx: DurableTaskRunContext, input: TRunInput) => TOutput | Promise<TOutput>
}

/**
 * Options for a durable task with children tasks that can be run using a durable executor. It is
 * similar to {@link DurableParentTaskOptions} with the added functionality that the run function
 * can return children tasks. These children tasks will be run in parallel after the run function
 * completes.
 *
 * The task will be marked completed only after all the children tasks complete successfully. If any
 * of the children tasks fail, the task will be marked as `children_failed`. In between the run
 * function and the children tasks, the task will be marked as `waiting_for_children`.
 *
 * If the run function output and children tasks output need to be combined to produce the final
 * output, you can use {@link onRunAndChildrenComplete} to transform the output to a compatible
 * type. If this is not provided, the task will return an output of type
 * `{ output: TOutput, childrenOutputs: Array<DurableTaskChildOutput> }`. Even if no children tasks
 * are returned, the output will be of this type if {@link onRunAndChildrenComplete} is not
 * provided.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
 * const extractFileTitle = durableExecutor.task({
 *   id: 'extractFileTitle',
 *   timeoutMs: 30_000, // 30 seconds
 *   run: async (ctx, input: { filePath: string }) => {
 *     // ... extract the file title
 *     return {
 *       title: 'File Title',
 *     }
 *   },
 * })
 *
 * const summarizeFile = durableExecutor.task({
 *   id: 'summarizeFile',
 *   timeoutMs: 30_000, // 30 seconds
 *   run: async (ctx, input: { filePath: string }) => {
 *     // ... summarize the file
 *     return {
 *       summary: 'File summary',
 *     }
 *   },
 * })
 *
 * const uploadFile = durableExecutor.parentTask({
 *   id: 'uploadFile',
 *   timeoutMs: 60_000, // 1 minute
 *   runParent: async (ctx, input: { filePath: string; uploadUrl: string }) => {
 *     // ... upload file to the given uploadUrl
 *     // Extract the file title and summarize the file in parallel
 *     return {
 *       output: {
 *         filePath: input.filePath,
 *         uploadUrl: input.uploadUrl,
 *         fileSize: 100,
 *       },
 *       children: [
 *         {
 *           task: extractFileTitle,
 *           input: { filePath: input.filePath },
 *         },
 *         {
 *           task: summarizeFile,
 *           input: { filePath: input.filePath },
 *         },
 *       ],
 *     }
 *   },
 *   onRunAndChildrenComplete: {
 *     id: 'onUploadFileChildrenComplete',
 *     timeoutMs: 60_000, // 1 minute
 *     run: async (ctx, { input, output, childrenOutputs }) => {
 *       // ... combine the output of the run function and children tasks
 *       return {
 *         filePath: input.filePath,
 *         uploadUrl: input.uploadUrl,
 *         fileSize: 100,
 *         title: 'File Title',
 *         summary: 'File summary',
 *       }
 *     },
 *   },
 * })
 * ```
 *
 * @category Task
 */
export type DurableParentTaskOptions<
  TRunOutput,
  TOutput = {
    output: TRunOutput
    childrenOutputs: Array<DurableTaskChildExecutionOutput>
  },
  TRunInput = unknown,
  TInput = TRunInput,
  TOnRunAndChildrenCompleteRunInput = DurableTaskOnChildrenCompleteInput<TRunOutput, TRunInput>,
> = Omit<DurableTaskOptions<TOutput, TRunInput, TInput>, 'run'> & {
  /**
   * The task run logic. It is similar to {@link DurableTaskOptions.run} but it returns the output
   * and children tasks to be run in parallel after the run function completes.
   *
   * @param ctx - The context object to the task.
   * @param input - The input of the task.
   * @returns The output of the task and children tasks to be run in parallel after the run function
   * completes.
   */
  runParent: (
    ctx: DurableTaskRunContext,
    input: TRunInput,
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
   * Task to run after the run function and children tasks complete. This is useful for combining
   * the output of the run function and children tasks.
   *
   * If this is not provided, the task will return an output of type
   * `{ output: TOutput, childrenOutputs: Array<DurableTaskChildOutput> }`.
   */
  onRunAndChildrenComplete?: DurableAnyTaskOptions<
    TOutput,
    TOnRunAndChildrenCompleteRunInput,
    DurableTaskOnChildrenCompleteInput<TRunOutput, TRunInput>
  >
}

/**
 * Options for a durable task with input schema that can be run using a durable executor. It is
 * similar to {@link DurableTaskOptions} but with an input schema that can validate the input
 * instead of the {@link DurableTaskOptions.validateInput} function.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
 * const extractFileTitle = durableExecutor.schemaTask({
 *   id: 'extractFileTitle',
 *   timeoutMs: 30_000, // 30 seconds
 *   inputSchema: z.object({
 *     filePath: z.string(),
 *   }),
 *   run: async (ctx, input) => {
 *     // ... extract the file title
 *     return {
 *       output: {
 *         title: 'File Title',
 *       },
 *     }
 *   },
 * })
 * ```
 *
 * @category Task
 */
export type DurableSchemaTaskOptions<TOutput, TInputSchema extends StandardSchemaV1> = Omit<
  DurableTaskOptions<
    TOutput,
    StandardSchemaV1.InferOutput<TInputSchema>,
    StandardSchemaV1.InferInput<TInputSchema>
  >,
  'validateInput'
> & {
  /**
   * The input schema of the task.
   */
  inputSchema: TInputSchema
}

/**
 * Options for a durable task with input schema that can be run using a durable executor. It is
 * similar to {@link DurableTaskOptions} but with an input schema that can validate the input
 * instead of the {@link DurableTaskOptions.validateInput} function.
 *
 * See the [task examples](https://gpahal.github.io/durable-execution/index.html#task-examples)
 * section for more details on creating tasks.
 *
 * @example
 * ```ts
 * const extractFileTitle = durableExecutor.task({
 *   id: 'extractFileTitle',
 *   timeoutMs: 30_000, // 30 seconds
 *   run: async (ctx, input: { filePath: string }) => {
 *     // ... extract the file title
 *     return {
 *       title: 'File Title',
 *     }
 *   },
 * })
 *
 * const summarizeFile = durableExecutor.task({
 *   id: 'summarizeFile',
 *   timeoutMs: 30_000, // 30 seconds
 *   run: async (ctx, input: { filePath: string }) => {
 *     // ... summarize the file
 *     return {
 *       summary: 'File summary',
 *     }
 *   },
 * })
 *
 * const uploadFile = durableExecutor.parentSchemaTask({
 *   id: 'uploadFile',
 *   timeoutMs: 60_000, // 1 minute
 *   inputSchema: z.object({
 *     filePath: z.string(),
 *     uploadUrl: z.string(),
 *   }),
 *   runParent: async (ctx, input) => {
 *     // ... upload file to the given uploadUrl
 *     // Extract the file title and summarize the file in parallel
 *     return {
 *       output: {
 *         filePath: input.filePath,
 *         uploadUrl: input.uploadUrl,
 *         fileSize: 100,
 *       },
 *       children: [
 *         {
 *           task: extractFileTitle,
 *           input: { filePath: input.filePath },
 *         },
 *         {
 *           task: summarizeFile,
 *           input: { filePath: input.filePath },
 *         },
 *       ],
 *     }
 *   },
 *   onRunAndChildrenComplete: {
 *     id: 'onUploadFileChildrenComplete',
 *     timeoutMs: 60_000, // 1 minute
 *     run: async (ctx, { input, output, childrenOutputs }) => {
 *       // ... combine the output of the run function and children tasks
 *       return {
 *         filePath: input.filePath,
 *         uploadUrl: input.uploadUrl,
 *         fileSize: 100,
 *         title: 'File Title',
 *         summary: 'File summary',
 *       }
 *     },
 *   },
 * })
 * ```
 *
 * @category Task
 */
export type DurableParentSchemaTaskOptions<
  TRunOutput,
  TOutput = {
    output: TRunOutput
    childrenOutputs: Array<DurableTaskChildExecutionOutput>
  },
  TInputSchema extends StandardSchemaV1 = StandardSchemaV1<unknown, unknown>,
> = Omit<
  DurableParentTaskOptions<
    TRunOutput,
    TOutput,
    StandardSchemaV1.InferOutput<TInputSchema>,
    StandardSchemaV1.InferInput<TInputSchema>
  >,
  'validateInput'
> & {
  /**
   * The input schema of the task.
   */
  inputSchema: TInputSchema
}

/**
 * The input type for the on children complete task.
 *
 * @category Task
 */
export type DurableTaskOnChildrenCompleteInput<TRunOutput = unknown, TRunInput = unknown> = {
  input: TRunInput
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
export type DurableTaskExecution<TOutput = unknown, TRunInput = unknown> =
  | DurableTaskReadyExecution<TRunInput>
  | DurableTaskRunningExecution<TRunInput>
  | DurableTaskFailedExecution<TRunInput>
  | DurableTaskTimedOutExecution<TRunInput>
  | DurableTaskWaitingForChildrenExecution<TRunInput>
  | DurableTaskChildrenFailedExecution<TRunInput>
  | DurableTaskWaitingForOnRunAndChildrenCompleteExecution<TRunInput>
  | DurableTaskOnRunAndChildrenCompleteFailedExecution<TRunInput>
  | DurableTaskCompletedExecution<TOutput, TRunInput>
  | DurableTaskCancelledExecution<TRunInput>

/**
 * A finished execution of a durable task. See
 * [Durable task execution](https://gpahal.github.io/durable-execution/index.html#durable-task-execution)
 * docs for more details on how task executions work.
 *
 * @category Task
 */
export type DurableTaskFinishedExecution<TOutput = unknown, TRunInput = unknown> =
  | DurableTaskFailedExecution<TRunInput>
  | DurableTaskTimedOutExecution<TRunInput>
  | DurableTaskChildrenFailedExecution<TRunInput>
  | DurableTaskOnRunAndChildrenCompleteFailedExecution<TRunInput>
  | DurableTaskCompletedExecution<TOutput, TRunInput>
  | DurableTaskCancelledExecution<TRunInput>

/**
 * A durable task execution that is ready to be run.
 *
 * @category Task
 */
export type DurableTaskReadyExecution<TRunInput = unknown> = {
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
  runInput: TRunInput
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
export type DurableTaskRunningExecution<TRunInput = unknown> = Omit<
  DurableTaskReadyExecution<TRunInput>,
  'status'
> & {
  status: 'running'
  startedAt: Date
  expiresAt: Date
}

/**
 * A durable task execution that failed while running.
 *
 * @category Task
 */
export type DurableTaskFailedExecution<TRunInput = unknown> = Omit<
  DurableTaskRunningExecution<TRunInput>,
  'status' | 'error'
> & {
  status: 'failed'
  error: DurableTaskError
  finishedAt: Date
}

/**
 * A durable task execution that timed out while running.
 *
 * @category Task
 */
export type DurableTaskTimedOutExecution<TRunInput = unknown> = Omit<
  DurableTaskRunningExecution<TRunInput>,
  'status' | 'error'
> & {
  status: 'timed_out'
  error: DurableTaskTimedOutError
  finishedAt: Date
}

/**
 * A durable task execution that is waiting for children to complete.
 *
 * @category Task
 */
export type DurableTaskWaitingForChildrenExecution<TRunInput = unknown> = Omit<
  DurableTaskRunningExecution<TRunInput>,
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
export type DurableTaskChildrenFailedExecution<TRunInput = unknown> = Omit<
  DurableTaskWaitingForChildrenExecution<TRunInput>,
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
export type DurableTaskWaitingForOnRunAndChildrenCompleteExecution<TRunInput = unknown> = Omit<
  DurableTaskWaitingForChildrenExecution<TRunInput>,
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
export type DurableTaskOnRunAndChildrenCompleteFailedExecution<TRunInput = unknown> = Omit<
  DurableTaskWaitingForOnRunAndChildrenCompleteExecution<TRunInput>,
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
export type DurableTaskCompletedExecution<TOutput = unknown, TRunInput = unknown> = Omit<
  DurableTaskWaitingForChildrenExecution<TRunInput>,
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
export type DurableTaskCancelledExecution<TRunInput = unknown> = Omit<
  DurableTaskRunningExecution<TRunInput>,
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
export type DurableTaskChild<TInput = unknown> = {
  task: DurableTask<unknown, unknown, TInput>
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
 * const handle = await durableExecutor.enqueueTask(uploadFile, {filePath: 'file.txt'})
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
export type DurableTaskHandle<TOutput = unknown, TRunInput = unknown> = {
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
  getTaskExecution: () => Promise<DurableTaskExecution<TOutput, TRunInput>>
  /**
   * Wait for the durable task execution to be finished and get it.
   *
   * @param options - The options for waiting for the durable task execution.
   * @returns The durable task execution.
   */
  waitAndGetTaskFinishedExecution: (options?: {
    signal?: CancelSignal | AbortSignal
    pollingIntervalMs?: number
  }) => Promise<DurableTaskFinishedExecution<TOutput, TRunInput>>
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
