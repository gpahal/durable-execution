import { Either, Schema } from 'effect'

import {
  combineCancelSignals,
  createCancellablePromise,
  createTimeoutCancelSignal,
  type CancelSignal,
} from '@gpahal/std/cancel'
import { isFunction } from '@gpahal/std/functions'

import {
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionTimedOutError,
} from './errors'
import {
  isFinalizeTaskOptionsParentTaskOptions,
  isFinalizeTaskOptionsTaskOptions,
  type ChildTask,
  type CommonTaskOptions,
  type DefaultParentTaskOutput,
  type FinalizeTaskOptions,
  type ParentTaskOptions,
  type SleepingTaskOptions,
  type TaskEnqueueOptions,
  type TaskOptions,
  type TaskRetryOptions,
  type TaskRunContext,
} from './task'
import { generateId } from './utils'

export type TaskOptionsInternal = {
  taskType: 'task' | 'sleepingTask' | 'parentTask'
  id: string
  retryOptions: TaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  areChildrenSequential: boolean
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    children: ReadonlyArray<ChildTask>
  }>
  finalize: ((input: DefaultParentTaskOutput) => Promise<unknown>) | TaskOptionsInternal | undefined
}

function convertTaskOptionsOptionsInternal(
  taskOptions: TaskOptions<unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'task',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    areChildrenSequential: false,
    runParent: async (ctx, input) => {
      const output = await taskOptions.run(ctx, input)
      return {
        output,
        children: [],
      }
    },
    finalize: undefined,
  }
}

function convertSleepingTaskOptionsOptionsInternal(
  taskOptions: SleepingTaskOptions<unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'sleepingTask',
    id: taskOptions.id,
    retryOptions: undefined,
    sleepMsBeforeRun: undefined,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    areChildrenSequential: false,
    runParent: () => {
      throw DurableExecutionError.any('Sleeping tasks cannot be run', false, true)
    },
    finalize: undefined,
  }
}

function convertParentTaskOptionsOptionsInternal(
  taskOptions: ParentTaskOptions<unknown, unknown, unknown, unknown>,
  areChildrenSequential: boolean,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'parentTask',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    areChildrenSequential,
    runParent: async (ctx, input) => {
      const runParentOutput = await taskOptions.runParent(ctx, input)
      return {
        output: runParentOutput.output,
        children: runParentOutput.children ?? [],
      }
    },
    finalize: taskOptions.finalize
      ? isFunction(taskOptions.finalize)
        ? (taskOptions.finalize as (input: DefaultParentTaskOutput) => Promise<unknown>)
        : isFinalizeTaskOptionsParentTaskOptions(taskOptions.finalize as FinalizeTaskOptions)
          ? convertParentTaskOptionsOptionsInternal(
              taskOptions.finalize as ParentTaskOptions<unknown, unknown, unknown, unknown>,
              false,
              undefined,
            )
          : isFinalizeTaskOptionsTaskOptions(taskOptions.finalize as FinalizeTaskOptions)
            ? convertTaskOptionsOptionsInternal(
                taskOptions.finalize as TaskOptions<unknown, unknown>,
                undefined,
              )
            : undefined
      : undefined,
  }
}

const RetryOptionsSchema = Schema.Struct({
  maxAttempts: Schema.Int.pipe(Schema.between(0, 1000)),
  baseDelayMs: Schema.Int.pipe(
    Schema.between(0, 3_600_000), // 0 to 1 hour
    Schema.optionalWith({ nullable: true }),
  ),
  delayMultiplier: Schema.Number.pipe(
    Schema.between(0.1, 10),
    Schema.optionalWith({ nullable: true }),
  ),
  maxDelayMs: Schema.Int.pipe(
    Schema.between(0, 86_400_000), // 0 to 24 hours
    Schema.optionalWith({ nullable: true }),
  ),
}).pipe(
  Schema.filter((val) => {
    if (val.maxDelayMs != null && val.baseDelayMs != null && val.maxDelayMs < val.baseDelayMs) {
      return `maxDelayMs must be greater than or equal to baseDelayMs`
    }
    return true
  }),
  Schema.NullishOr,
  Schema.transform(
    Schema.Struct({
      maxAttempts: Schema.Int,
      baseDelayMs: Schema.Int.pipe(Schema.optional),
      delayMultiplier: Schema.Number.pipe(Schema.optional),
      maxDelayMs: Schema.Int.pipe(Schema.optional),
    }),
    {
      strict: true,
      encode: (val) => {
        return {
          maxAttempts: val.maxAttempts,
          baseDelayMs: val.baseDelayMs,
          delayMultiplier: val.delayMultiplier,
          maxDelayMs: val.maxDelayMs,
        }
      },
      decode: (val) => {
        if (val == null) {
          return {
            maxAttempts: 0,
            baseDelayMs: undefined,
            delayMultiplier: undefined,
            maxDelayMs: undefined,
          }
        }

        return {
          maxAttempts: val.maxAttempts,
          baseDelayMs: val.baseDelayMs,
          delayMultiplier: val.delayMultiplier,
          maxDelayMs: val.maxDelayMs,
        }
      },
    },
  ),
)

const SleepMsBeforeRunSchema = Schema.Number.pipe(
  Schema.greaterThanOrEqualTo(0),
  Schema.NullishOr,
  Schema.transform(Schema.Number, {
    strict: true,
    encode: (val) => {
      return val
    },
    decode: (val) => {
      if (val == null) {
        return 0
      }
      return val
    },
  }),
)

const TimeoutMsSchema = Schema.Int.pipe(Schema.greaterThanOrEqualTo(1))

/**
 * An internal representation of a task.
 *
 * @category Task
 * @internal
 */
export class TaskInternal {
  readonly taskType: 'task' | 'sleepingTask' | 'parentTask'
  readonly id: string
  readonly retryOptions: TaskRetryOptions
  readonly sleepMsBeforeRun: number
  readonly timeoutMs: number
  readonly areChildrenSequential: boolean
  private readonly validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  private readonly runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    children: ReadonlyArray<ChildTask>
  }>
  readonly finalize:
    | ((input: DefaultParentTaskOutput) => Promise<unknown>)
    | TaskInternal
    | undefined

  constructor(
    taskType: 'task' | 'sleepingTask' | 'parentTask',
    id: string,
    retryOptions: TaskRetryOptions,
    sleepMsBeforeRun: number,
    timeoutMs: number,
    areChildrenSequential: boolean,
    validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
    runParent: (
      ctx: TaskRunContext,
      input: unknown,
    ) => Promise<{
      output: unknown
      children: ReadonlyArray<ChildTask>
    }>,
    finalize: ((input: DefaultParentTaskOutput) => Promise<unknown>) | TaskInternal | undefined,
  ) {
    this.taskType = taskType
    this.id = id
    this.retryOptions = retryOptions
    this.sleepMsBeforeRun = sleepMsBeforeRun
    this.timeoutMs = timeoutMs
    this.areChildrenSequential = areChildrenSequential
    this.validateInputFn = validateInputFn
    this.runParent = runParent
    this.finalize = finalize
  }

  static fromTaskOptionsInternal(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: TaskOptionsInternal,
  ): TaskInternal {
    const validatedCommonTaskOptions = validateCommonTaskOptions(taskOptions)
    if (taskInternalsMap.has(taskOptions.id)) {
      throw DurableExecutionError.nonRetryable(
        `Task ${taskOptions.id} already exists. Use unique ids for tasks`,
      )
    }

    const finalize = taskOptions.finalize
      ? isFunction(taskOptions.finalize)
        ? (taskOptions.finalize as (input: DefaultParentTaskOutput) => Promise<unknown>)
        : TaskInternal.fromTaskOptionsInternal(
            taskInternalsMap,
            taskOptions.finalize as TaskOptionsInternal,
          )
      : undefined

    const taskInternal = new TaskInternal(
      taskOptions.taskType,
      taskOptions.id,
      validatedCommonTaskOptions.retryOptions,
      validatedCommonTaskOptions.sleepMsBeforeRun,
      validatedCommonTaskOptions.timeoutMs,
      taskOptions.areChildrenSequential,
      taskOptions.validateInputFn,
      taskOptions.runParent,
      finalize,
    )
    taskInternalsMap.set(taskInternal.id, taskInternal)
    return taskInternal
  }

  static fromTaskOptions<TRunInput, TInput, TOutput>(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: TaskOptions<TRunInput, TOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): TaskInternal {
    return TaskInternal.fromTaskOptionsInternal(
      taskInternalsMap,
      convertTaskOptionsOptionsInternal(
        taskOptions as TaskOptions<unknown, unknown>,
        validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      ),
    )
  }

  static fromSleepingTaskOptions<TInput, TOutput>(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: SleepingTaskOptions<TOutput>,
    validateInputFn?: (id: string, input: TInput) => string | Promise<string>,
  ): TaskInternal {
    return TaskInternal.fromTaskOptionsInternal(
      taskInternalsMap,
      convertSleepingTaskOptionsOptionsInternal(
        taskOptions,
        validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      ),
    )
  }

  static fromParentTaskOptions<
    TRunInput,
    TInput = TRunInput,
    TRunOutput = unknown,
    TOutput = unknown,
    TFinalizeTaskRunOutput = unknown,
  >(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    areChildrenSequential: boolean,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): TaskInternal {
    return TaskInternal.fromTaskOptionsInternal(
      taskInternalsMap,
      convertParentTaskOptionsOptionsInternal(
        taskOptions as ParentTaskOptions<unknown, unknown, unknown, unknown>,
        areChildrenSequential,
        validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      ),
    )
  }

  async runParentWithTimeoutAndCancellation(
    ctx: Omit<TaskRunContext, 'cancelSignal'>,
    input: unknown,
    timeoutMs: number,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    children: ReadonlyArray<ChildTask>
  }> {
    if (this.validateInputFn) {
      input = await this.validateInputFn(this.id, input)
    }
    const [timeoutCancelSignal, clearTimeout] = createTimeoutCancelSignal(timeoutMs)
    const finalCancelSignal = combineCancelSignals([cancelSignal, timeoutCancelSignal])
    const result = await createCancellablePromise(
      createCancellablePromise(
        this.runParent({ ...ctx, cancelSignal: finalCancelSignal }, input),
        timeoutCancelSignal,
        new DurableExecutionTimedOutError(),
      ),
      cancelSignal,
      new DurableExecutionCancelledError(),
    )
    clearTimeout()
    return result
  }
}

export function validateCommonTaskOptions(taskOptions: CommonTaskOptions): {
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
} {
  validateTaskId(taskOptions.id)

  const parsedRetryOptions = Schema.decodeUnknownEither(RetryOptionsSchema)(
    taskOptions.retryOptions,
  )
  if (Either.isLeft(parsedRetryOptions)) {
    throw DurableExecutionError.nonRetryable(
      `Invalid retry options for task ${taskOptions.id}: ${parsedRetryOptions.left.message}`,
    )
  }

  const parsedSleepMsBeforeRun = Schema.decodeUnknownEither(SleepMsBeforeRunSchema)(
    taskOptions.sleepMsBeforeRun,
  )
  if (Either.isLeft(parsedSleepMsBeforeRun)) {
    throw DurableExecutionError.nonRetryable(
      `Invalid sleep ms before run for task ${taskOptions.id}: ${parsedSleepMsBeforeRun.left.message}`,
    )
  }

  const parsedTimeoutMs = Schema.decodeUnknownEither(TimeoutMsSchema)(taskOptions.timeoutMs)
  if (Either.isLeft(parsedTimeoutMs)) {
    throw DurableExecutionError.nonRetryable(
      `Invalid timeout value for task ${taskOptions.id}: ${parsedTimeoutMs.left.message}`,
    )
  }

  return {
    retryOptions: parsedRetryOptions.right,
    sleepMsBeforeRun: parsedSleepMsBeforeRun.right,
    timeoutMs: parsedTimeoutMs.right,
  }
}

export function validateEnqueueOptions(
  taskId: string,
  options?: TaskEnqueueOptions,
): TaskEnqueueOptions {
  const validatedOptions: TaskEnqueueOptions = {}

  if (options?.retryOptions) {
    const parsedRetryOptions = Schema.decodeUnknownEither(RetryOptionsSchema)(options.retryOptions)
    if (Either.isLeft(parsedRetryOptions)) {
      throw DurableExecutionError.nonRetryable(
        `Invalid retry options for task ${taskId}: ${parsedRetryOptions.left.message}`,
      )
    }

    validatedOptions.retryOptions = parsedRetryOptions.right
  }

  if (options?.sleepMsBeforeRun != null) {
    const parsedSleepMsBeforeRun = Schema.decodeUnknownEither(SleepMsBeforeRunSchema)(
      options.sleepMsBeforeRun,
    )
    if (Either.isLeft(parsedSleepMsBeforeRun)) {
      throw DurableExecutionError.nonRetryable(
        `Invalid sleep ms before run for task ${taskId}: ${parsedSleepMsBeforeRun.left.message}`,
      )
    }

    validatedOptions.sleepMsBeforeRun = parsedSleepMsBeforeRun.right
  }

  if (options?.timeoutMs != null) {
    const parsedTimeoutMs = Schema.decodeUnknownEither(TimeoutMsSchema)(options.timeoutMs)
    if (Either.isLeft(parsedTimeoutMs)) {
      throw DurableExecutionError.nonRetryable(
        `Invalid timeout value for task ${taskId}: ${parsedTimeoutMs.left.message}`,
      )
    }

    validatedOptions.timeoutMs = parsedTimeoutMs.right
  }

  return validatedOptions
}

export function overrideTaskEnqueueOptions(
  existingOptions: {
    retryOptions: TaskRetryOptions
    sleepMsBeforeRun: number
    timeoutMs: number
  },
  overrideOptions?: TaskEnqueueOptions,
): {
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
} {
  return {
    retryOptions:
      overrideOptions?.retryOptions == null
        ? existingOptions.retryOptions
        : overrideOptions.retryOptions,
    sleepMsBeforeRun:
      overrideOptions?.sleepMsBeforeRun == null
        ? existingOptions.sleepMsBeforeRun
        : overrideOptions.sleepMsBeforeRun,
    timeoutMs:
      overrideOptions?.timeoutMs == null ? existingOptions.timeoutMs : overrideOptions.timeoutMs,
  }
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
    throw DurableExecutionError.nonRetryable('Task id cannot be empty')
  }
  if (id.length > 255) {
    throw DurableExecutionError.nonRetryable('Task id cannot be longer than 255 characters')
  }
  if (!_TASK_ID_REGEX.test(id)) {
    throw DurableExecutionError.nonRetryable(
      'Task id can only contain alphanumeric characters and underscores',
    )
  }
}
