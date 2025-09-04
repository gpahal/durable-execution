import { Duration, Effect, Schema } from 'effect'

import { isFunction } from '@gpahal/std/functions'

import { DurableExecutionError, DurableExecutionTimedOutError } from './errors'
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
  type TaskType,
} from './task'
import { convertMaybePromiseOrEffectToEffect, generateId } from './utils'

export type TaskOptionsInternal = {
  taskType: TaskType
  id: string
  retryOptions: TaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  validateInputFn:
    | ((id: string, input: unknown) => Effect.Effect<unknown, DurableExecutionError>)
    | undefined
  runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Effect.Effect<
    {
      output: unknown
      children: ReadonlyArray<ChildTask>
    },
    DurableExecutionError
  >
  finalize:
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError, never>)
    | TaskOptionsInternal
    | undefined
}

export function convertTaskOptionsToOptionsInternal<TRunInput, TInput, TOutput>(
  taskOptions: TaskOptions<TRunInput, TOutput>,
  validateInputFn:
    | ((id: string, input: TInput) => Effect.Effect<TRunInput, DurableExecutionError>)
    | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'task',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: validateInputFn as
      | ((id: string, input: unknown) => Effect.Effect<unknown, DurableExecutionError>)
      | undefined,
    runParent: (ctx, input) =>
      Effect.gen(function* () {
        const runOutput = yield* convertMaybePromiseOrEffectToEffect(() =>
          taskOptions.run(ctx, input as TRunInput),
        )
        return {
          output: runOutput,
          children: [],
        }
      }),
    finalize: undefined,
  }
}

export function convertSleepingTaskOptionsToOptionsInternal<TInput, TOutput>(
  taskOptions: SleepingTaskOptions<TOutput>,
  validateInputFn: ((id: string, input: TInput) => Effect.Effect<string, unknown>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'sleepingTask',
    id: taskOptions.id,
    retryOptions: undefined,
    sleepMsBeforeRun: undefined,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: validateInputFn as
      | ((id: string, input: unknown) => Effect.Effect<unknown, DurableExecutionError>)
      | undefined,
    runParent: () =>
      Effect.fail(new DurableExecutionError('Sleeping tasks cannot be run', { isInternal: true })),
    finalize: undefined,
  }
}

export function convertParentTaskOptionsToOptionsInternal<
  TRunInput,
  TInput,
  TRunOutput,
  TOutput,
  TFinalizeTaskRunOutput,
>(
  taskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
  validateInputFn: ((id: string, input: TInput) => Effect.Effect<TRunInput, unknown>) | undefined,
): TaskOptionsInternal {
  let finalize:
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError, never>)
    | TaskOptionsInternal
    | undefined = undefined
  if (isFunction(taskOptions.finalize)) {
    finalize = (input: DefaultParentTaskOutput) =>
      convertMaybePromiseOrEffectToEffect(() =>
        (taskOptions.finalize as (input: DefaultParentTaskOutput) => unknown)(input),
      )
  } else if (taskOptions.finalize != null) {
    finalize = isFinalizeTaskOptionsParentTaskOptions(taskOptions.finalize as FinalizeTaskOptions)
      ? convertParentTaskOptionsToOptionsInternal(
          taskOptions.finalize as ParentTaskOptions<unknown, unknown, unknown, unknown>,
          undefined,
        )
      : isFinalizeTaskOptionsTaskOptions(taskOptions.finalize as FinalizeTaskOptions)
        ? convertTaskOptionsToOptionsInternal(
            taskOptions.finalize as TaskOptions<unknown, unknown>,
            undefined,
          )
        : undefined
  }

  return {
    taskType: 'parentTask',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: validateInputFn as
      | ((id: string, input: unknown) => Effect.Effect<unknown, DurableExecutionError>)
      | undefined,
    runParent: (ctx, input) =>
      Effect.gen(function* () {
        const runParentOutput = yield* convertMaybePromiseOrEffectToEffect(() =>
          taskOptions.runParent(ctx, input as TRunInput),
        )
        return {
          output: runParentOutput.output,
          children: runParentOutput.children ?? [],
        }
      }),
    finalize,
  }
}

export type TaskInternal = {
  taskType: TaskType
  id: string
  retryOptions: TaskRetryOptions
  sleepMsBeforeRun: number
  timeoutMs: number
  validateInputFn:
    | ((id: string, input: unknown) => Effect.Effect<unknown, DurableExecutionError>)
    | undefined
  runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Effect.Effect<
    {
      output: unknown
      children: ReadonlyArray<ChildTask>
    },
    DurableExecutionError
  >
  finalize:
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError, never>)
    | TaskInternal
    | undefined
}

export const addTaskInternal: (
  taskInternalsMap: Map<string, TaskInternal>,
  optionsInternal: TaskOptionsInternal,
) => Effect.Effect<TaskInternal, DurableExecutionError, never> = Effect.fn(function* (
  taskInternalsMap: Map<string, TaskInternal>,
  optionsInternal: TaskOptionsInternal,
) {
  const validatedCommonTaskOptions = yield* validateCommonTaskOptions(optionsInternal)
  if (taskInternalsMap.has(optionsInternal.id)) {
    return yield* Effect.fail(
      DurableExecutionError.nonRetryable(
        `Task with given id already exists [taskId=${optionsInternal.id}]`,
      ),
    )
  }

  let finalize: TaskInternal['finalize']
  if (optionsInternal.finalize == null) {
    finalize = undefined
  } else if (isFunction(optionsInternal.finalize)) {
    finalize = optionsInternal.finalize as (
      input: DefaultParentTaskOutput,
    ) => Effect.Effect<unknown, DurableExecutionError, never>
  } else {
    finalize = yield* addTaskInternal(taskInternalsMap, optionsInternal.finalize as TaskInternal)
  }

  const taskInternal = {
    ...optionsInternal,
    retryOptions: validatedCommonTaskOptions.retryOptions,
    sleepMsBeforeRun: validatedCommonTaskOptions.sleepMsBeforeRun,
    timeoutMs: validatedCommonTaskOptions.timeoutMs,
    finalize,
  }
  taskInternalsMap.set(optionsInternal.id, taskInternal)
  return taskInternal
})

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
const decodeRetryOptions = Schema.decodeUnknown(RetryOptionsSchema)

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
const decodeSleepMsBeforeRun = Schema.decodeUnknown(SleepMsBeforeRunSchema)

const TimeoutMsSchema = Schema.Int.pipe(Schema.greaterThanOrEqualTo(1))
const decodeTimeoutMs = Schema.decodeUnknown(TimeoutMsSchema)

export const runParentWithTimeout = Effect.fn(function* (
  taskInternal: TaskInternal,
  ctx: Omit<TaskRunContext, 'abortSignal'>,
  input: unknown,
  timeoutMs: number,
) {
  if (taskInternal.validateInputFn) {
    input = yield* taskInternal.validateInputFn(taskInternal.id, input)
  }

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const controller = yield* Effect.acquireRelease(
        Effect.sync(() => new AbortController()),
        (c) => Effect.sync(() => c.abort()),
      )
      const result = yield* taskInternal.runParent(
        { ...ctx, abortSignal: controller.signal },
        input,
      )
      return result
    }),
  ).pipe(
    Effect.timeoutFail({
      duration: Duration.millis(timeoutMs),
      onTimeout: () => new DurableExecutionTimedOutError() as DurableExecutionError,
    }),
  )
})

export const validateCommonTaskOptions = Effect.fn(function* (taskOptions: CommonTaskOptions) {
  yield* validateTaskId(taskOptions.id)

  const retryOptions = yield* decodeRetryOptions(taskOptions.retryOptions).pipe(
    Effect.mapError((error) =>
      DurableExecutionError.nonRetryable(
        `Invalid retry options for task ${taskOptions.id}: ${error.message}`,
      ),
    ),
  )

  const sleepMsBeforeRun = yield* decodeSleepMsBeforeRun(taskOptions.sleepMsBeforeRun).pipe(
    Effect.mapError((error) =>
      DurableExecutionError.nonRetryable(
        `Invalid sleep ms before run for task ${taskOptions.id}: ${error.message}`,
      ),
    ),
  )

  const timeoutMs = yield* decodeTimeoutMs(taskOptions.timeoutMs).pipe(
    Effect.mapError((error) =>
      DurableExecutionError.nonRetryable(
        `Invalid timeout value for task ${taskOptions.id}: ${error.message}`,
      ),
    ),
  )

  return {
    retryOptions,
    sleepMsBeforeRun,
    timeoutMs,
  }
})

export const validateEnqueueOptions = Effect.fn(function* (
  taskId: string,
  options?: TaskEnqueueOptions,
) {
  const validatedOptions: TaskEnqueueOptions = {}

  if (options?.retryOptions) {
    validatedOptions.retryOptions = yield* decodeRetryOptions(options.retryOptions).pipe(
      Effect.mapError((error) =>
        DurableExecutionError.nonRetryable(
          `Invalid retry options for task ${taskId}: ${error.message}`,
        ),
      ),
    )
  }

  if (options?.sleepMsBeforeRun != null) {
    validatedOptions.sleepMsBeforeRun = yield* decodeSleepMsBeforeRun(
      options.sleepMsBeforeRun,
    ).pipe(
      Effect.mapError((error) =>
        DurableExecutionError.nonRetryable(
          `Invalid sleep ms before run for task ${taskId}: ${error.message}`,
        ),
      ),
    )
  }

  if (options?.timeoutMs != null) {
    validatedOptions.timeoutMs = yield* decodeTimeoutMs(options.timeoutMs).pipe(
      Effect.mapError((error) =>
        DurableExecutionError.nonRetryable(
          `Invalid timeout value for task ${taskId}: ${error.message}`,
        ),
      ),
    )
  }

  return validatedOptions
})

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
 * @returns An effect that fails with an error if the id is invalid.
 *
 * @category Task
 */
export const validateTaskId = Effect.fn(function* (id: string) {
  if (id.length === 0) {
    return yield* Effect.fail(DurableExecutionError.nonRetryable('Task id cannot be empty'))
  }
  if (id.length > 255) {
    return yield* Effect.fail(
      DurableExecutionError.nonRetryable(
        `Task id cannot be longer than 255 characters [idLength=${id.length}]`,
      ),
    )
  }
  if (!_TASK_ID_REGEX.test(id)) {
    return yield* Effect.fail(
      DurableExecutionError.nonRetryable(
        'Task id can only contain alphanumeric characters and underscores',
      ),
    )
  }
})
