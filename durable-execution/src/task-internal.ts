import { Duration, Effect, Schema, SchemaTransformation } from 'effect'

import { DurableExecutionError, DurableExecutionTimedOutError } from './errors'
import { isFinalizeTaskOptionsParentTaskOptions, isFinalizeTaskOptionsTaskOptions } from './task'
import type {
  ChildTask,
  CommonTaskOptions,
  DefaultParentTaskOutput,
  FinalizeTaskOptions,
  ParentTaskOptions,
  SleepingTaskOptions,
  TaskEnqueueOptions,
  TaskOptions,
  TaskRetryOptions,
  TaskRunContext,
  TaskType,
} from './task'
import { convertMaybePromiseOrEffectToEffect, generateId } from './utils'

export type TaskOptionsInternal = {
  taskType: TaskType
  id: string
  retryOptions: TaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  validateInputFn: ValidateInputFnInternal | undefined
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
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError>)
    | TaskOptionsInternal
    | undefined
}

type ValidateInputFnInternal = (
  id: string,
  input: unknown,
) => Effect.Effect<unknown, DurableExecutionError>

/**
 * Erases the input and output types of a validate input function.
 */
function toValidateInputFnInternal(
  validateInputFn:
    ((id: string, input: never) => Effect.Effect<unknown, DurableExecutionError>) | undefined,
): ValidateInputFnInternal | undefined {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the executor calls it only with the input of an enqueued task, which the task's types restrict to the function's input type
  return validateInputFn as ValidateInputFnInternal | undefined
}

export function convertTaskOptionsToOptionsInternal<TRunInput, TOutput>(
  taskOptions: TaskOptions<TRunInput, TOutput>,
  validateInputFn:
    ((id: string, input: never) => Effect.Effect<TRunInput, DurableExecutionError>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'task',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: toValidateInputFnInternal(validateInputFn),
    runParent: (ctx, input) =>
      Effect.gen(function* () {
        const runOutput = yield* convertMaybePromiseOrEffectToEffect(
          (): TOutput | Promise<TOutput> | Effect.Effect<TOutput, unknown> =>
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the executor passes the validated input of the task, which is TRunInput
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

export function convertSleepingTaskOptionsToOptionsInternal<TOutput>(
  taskOptions: SleepingTaskOptions<TOutput>,
  validateInputFn:
    ((id: string, input: never) => Effect.Effect<string, DurableExecutionError>) | undefined,
): TaskOptionsInternal {
  return {
    taskType: 'sleepingTask',
    id: taskOptions.id,
    retryOptions: undefined,
    sleepMsBeforeRun: undefined,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: toValidateInputFnInternal(validateInputFn),
    runParent: () =>
      Effect.fail(new DurableExecutionError('Sleeping tasks cannot be run', { isInternal: true })),
    finalize: undefined,
  }
}

export function convertParentTaskOptionsToOptionsInternal<
  TRunInput,
  TRunOutput,
  TOutput,
  TFinalizeTaskRunOutput,
>(
  taskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
  validateInputFn:
    ((id: string, input: never) => Effect.Effect<TRunInput, DurableExecutionError>) | undefined,
): TaskOptionsInternal {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the executor calls finalize with the run output of this task and its children, so the erased run output type matches
  const taskFinalize = taskOptions.finalize as
    ((input: DefaultParentTaskOutput) => unknown) | FinalizeTaskOptions | undefined
  let finalize:
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError>)
    | TaskOptionsInternal
    | undefined
  if (typeof taskFinalize === 'function') {
    finalize = (input: DefaultParentTaskOutput) =>
      convertMaybePromiseOrEffectToEffect(() => taskFinalize.call(taskOptions, input))
  } else if (taskFinalize != null) {
    finalize = isFinalizeTaskOptionsParentTaskOptions(taskFinalize)
      ? convertParentTaskOptionsToOptionsInternal(taskFinalize, undefined)
      : // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- public API: JavaScript callers can pass finalize options with both or neither of `run` and `runParent`
        isFinalizeTaskOptionsTaskOptions(taskFinalize)
        ? convertTaskOptionsToOptionsInternal(taskFinalize, undefined)
        : undefined
  }

  return {
    taskType: 'parentTask',
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: toValidateInputFnInternal(validateInputFn),
    runParent: (ctx, input) =>
      Effect.gen(function* () {
        const runParentOutput = yield* convertMaybePromiseOrEffectToEffect(
          (): ReturnType<typeof taskOptions.runParent> =>
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the executor passes the validated input of the task, which is TRunInput
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
  validateInputFn: ValidateInputFnInternal | undefined
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
    | ((input: DefaultParentTaskOutput) => Effect.Effect<unknown, DurableExecutionError>)
    | TaskInternal
    | undefined
}

export const addTaskInternal: (
  taskInternalsMap: Map<string, TaskInternal>,
  optionsInternal: TaskOptionsInternal,
) => Effect.Effect<TaskInternal, DurableExecutionError> = Effect.fn(function* (
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
  } else if (typeof optionsInternal.finalize === 'function') {
    finalize = optionsInternal.finalize
  } else {
    finalize = yield* addTaskInternal(taskInternalsMap, optionsInternal.finalize)
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

const RetryOptionsInputSchema = Schema.NullishOr(
  Schema.Struct({
    maxAttempts: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 })),
    baseDelayMs: Schema.optional(
      Schema.NullOr(
        Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 3_600_000 })), // 0 to 1 hour
      ),
    ),
    delayMultiplier: Schema.optional(
      Schema.NullOr(Schema.Number.check(Schema.isBetween({ minimum: 0.1, maximum: 10 }))),
    ),
    maxDelayMs: Schema.optional(
      Schema.NullOr(
        Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 86_400_000 })), // 0 to 24 hours
      ),
    ),
  }).check(
    Schema.makeFilter((val) => {
      if (val.maxDelayMs != null && val.baseDelayMs != null && val.maxDelayMs < val.baseDelayMs) {
        return `maxDelayMs must be greater than or equal to baseDelayMs`
      }
      return true
    }),
  ),
)

const RetryOptionsOutputSchema = Schema.Struct({
  maxAttempts: Schema.Int,
  baseDelayMs: Schema.optional(Schema.Int),
  delayMultiplier: Schema.optional(Schema.Number),
  maxDelayMs: Schema.optional(Schema.Int),
})

const RetryOptionsSchema = RetryOptionsInputSchema.pipe(
  Schema.decodeTo(
    RetryOptionsOutputSchema,
    SchemaTransformation.transform<
      typeof RetryOptionsOutputSchema.Type,
      typeof RetryOptionsInputSchema.Type
    >({
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
          baseDelayMs: val.baseDelayMs ?? undefined,
          delayMultiplier: val.delayMultiplier ?? undefined,
          maxDelayMs: val.maxDelayMs ?? undefined,
        }
      },
    }),
  ),
)
const decodeRetryOptions = Schema.decodeUnknownEffect(RetryOptionsSchema)

const SleepMsBeforeRunSchema = Schema.NullishOr(
  Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
).pipe(
  Schema.decodeTo(
    Schema.Number,
    SchemaTransformation.transform<number, number | null | undefined>({
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
  ),
)
const decodeSleepMsBeforeRun = Schema.decodeUnknownEffect(SleepMsBeforeRunSchema)

const TimeoutMsSchema = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))
const decodeTimeoutMs = Schema.decodeUnknownEffect(TimeoutMsSchema)

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
    Effect.timeoutOrElse({
      duration: Duration.millis(timeoutMs),
      orElse: () => Effect.fail(new DurableExecutionTimedOutError()),
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
    retryOptions: overrideOptions?.retryOptions ?? existingOptions.retryOptions,
    sleepMsBeforeRun: overrideOptions?.sleepMsBeforeRun ?? existingOptions.sleepMsBeforeRun,
    timeoutMs: overrideOptions?.timeoutMs ?? existingOptions.timeoutMs,
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
