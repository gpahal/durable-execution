import { z } from 'zod'

import { createTimeoutCancelSignal, type CancelSignal } from '@gpahal/std/cancel'
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
} from './task'
import { createCancellablePromiseCustom, generateId } from './utils'

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
    children: Array<ChildTask>
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
): TaskOptionsInternal {
  return {
    taskType: 'sleepingTask',
    id: taskOptions.id,
    retryOptions: undefined,
    sleepMsBeforeRun: undefined,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn: undefined,
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

const zRetryOptions = z
  .object({
    maxAttempts: z.number().int().min(0).max(100), // Reasonable maximum
    baseDelayMs: z
      .number()
      .int()
      .min(0)
      .max(3_600_000) // 1 hour
      .nullish()
      .transform((val) => {
        if (val == null) {
          return undefined
        }
        return val
      }),
    delayMultiplier: z
      .number()
      .min(0.1)
      .max(10)
      .nullish()
      .transform((val) => {
        if (val == null) {
          return undefined
        }
        return val
      }),
    maxDelayMs: z
      .number()
      .int()
      .min(0)
      .max(86_400_000) // 24 hours
      .nullish()
      .transform((val) => {
        if (val == null) {
          return undefined
        }
        return val
      }),
  })
  .nullish()
  .transform((val) => {
    if (val == null) {
      return {
        maxAttempts: 0,
        baseDelayMs: undefined,
        delayMultiplier: undefined,
        maxDelayMs: undefined,
      }
    }
    return val
  })
  .refine(
    (val) => {
      if (val.maxDelayMs != null && val.baseDelayMs != null) {
        return val.maxDelayMs >= val.baseDelayMs
      }
      return true
    },
    {
      message: 'maxDelayMs must be greater than or equal to baseDelayMs',
    },
  )

const zSleepMsBeforeRun = z
  .number()
  .int()
  .min(0)
  .nullish()
  .transform((val) => {
    if (val == null || val <= 0) {
      return 0
    }
    return val
  })

const zTimeoutMs = z.number().int().min(1)

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
    children: Array<ChildTask>
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
      children: Array<ChildTask>
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

  static fromSleepingTaskOptions<TOutput>(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: SleepingTaskOptions<TOutput>,
  ): TaskInternal {
    return TaskInternal.fromTaskOptionsInternal(
      taskInternalsMap,
      convertSleepingTaskOptionsOptionsInternal(taskOptions),
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
    ctx: TaskRunContext,
    input: unknown,
    timeoutMs: number,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    children: Array<ChildTask>
  }> {
    if (this.validateInputFn) {
      input = await this.validateInputFn(this.id, input)
    }

    const [timeoutCancelSignal, clearTimeout] = createTimeoutCancelSignal(timeoutMs)
    const result = await createCancellablePromiseCustom(
      createCancellablePromiseCustom(
        createCancellablePromiseCustom(
          this.runParent(ctx, input),
          timeoutCancelSignal,
          new DurableExecutionTimedOutError(),
        ),
        cancelSignal,
      ),
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

  const parsedRetryOptions = zRetryOptions.safeParse(taskOptions.retryOptions)
  if (!parsedRetryOptions.success) {
    throw DurableExecutionError.nonRetryable(
      `Invalid retry options for task ${taskOptions.id}: ${z.prettifyError(parsedRetryOptions.error)}`,
    )
  }

  const parsedSleepMsBeforeRun = zSleepMsBeforeRun.safeParse(taskOptions.sleepMsBeforeRun)
  if (!parsedSleepMsBeforeRun.success) {
    throw DurableExecutionError.nonRetryable(
      `Invalid sleep ms before run for task ${taskOptions.id}: ${z.prettifyError(parsedSleepMsBeforeRun.error)}`,
    )
  }

  const parsedTimeoutMs = zTimeoutMs.safeParse(taskOptions.timeoutMs)
  if (!parsedTimeoutMs.success) {
    throw DurableExecutionError.nonRetryable(
      `Invalid timeout value for task ${taskOptions.id}: ${z.prettifyError(parsedTimeoutMs.error)}`,
    )
  }

  return {
    retryOptions: parsedRetryOptions.data,
    sleepMsBeforeRun: parsedSleepMsBeforeRun.data,
    timeoutMs: parsedTimeoutMs.data,
  }
}

export function validateEnqueueOptions(
  taskId: string,
  options?: TaskEnqueueOptions,
): TaskEnqueueOptions {
  const validatedOptions: TaskEnqueueOptions = {}

  if (options?.retryOptions) {
    const parsedRetryOptions = zRetryOptions.safeParse(options.retryOptions)
    if (!parsedRetryOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid retry options for task ${taskId}: ${z.prettifyError(parsedRetryOptions.error)}`,
      )
    }

    validatedOptions.retryOptions = parsedRetryOptions.data
  }

  if (options?.sleepMsBeforeRun != null) {
    const parsedSleepMsBeforeRun = zSleepMsBeforeRun.safeParse(options.sleepMsBeforeRun)
    if (!parsedSleepMsBeforeRun.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid sleep ms before run for task ${taskId}: ${z.prettifyError(parsedSleepMsBeforeRun.error)}`,
      )
    }

    validatedOptions.sleepMsBeforeRun = parsedSleepMsBeforeRun.data
  }

  if (options?.timeoutMs != null) {
    const parsedTimeoutMs = zTimeoutMs.safeParse(options.timeoutMs)
    if (!parsedTimeoutMs.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid timeout value for task ${taskId}: ${z.prettifyError(parsedTimeoutMs.error)}`,
      )
    }

    validatedOptions.timeoutMs = parsedTimeoutMs.data
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
