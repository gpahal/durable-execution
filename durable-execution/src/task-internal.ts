import { z } from 'zod'

import {
  createCancelSignal,
  createTimeoutCancelSignal,
  type CancelSignal,
} from '@gpahal/std/cancel'
import { sleep } from '@gpahal/std/promises'

import {
  convertDurableExecutionErrorToStorageValue,
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from './errors'
import type { Logger } from './logger'
import type { SerializerInternal } from './serializer'
import {
  convertTaskExecutionStorageValueToTaskExecution,
  type TaskExecutionsStorageInternal,
} from './storage'
import {
  ACTIVE_TASK_EXECUTION_STATUSES,
  FINISHED_TASK_EXECUTION_STATUSES,
  isFinalizeTaskOptionsParentTaskOptions,
  isFinalizeTaskOptionsTaskOptions,
  type ChildTask,
  type CommonTaskOptions,
  type FinishedTaskExecution,
  type ParentTaskOptions,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
  type TaskOptions,
  type TaskRetryOptions,
  type TaskRunContext,
} from './task'
import { createCancellablePromiseCustom, generateId } from './utils'

export type TaskOptionsInternal = {
  isParentTaskOptions: boolean
  id: string
  retryOptions: TaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    children: Array<ChildTask>
  }>
  finalize: TaskOptionsInternal | undefined
}

function convertTaskOptionsOptionsInternal(
  taskOptions: TaskOptions<unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    isParentTaskOptions: false,
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
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

function convertParentTaskOptionsOptionsInternal(
  taskOptions: ParentTaskOptions<unknown, unknown, unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    isParentTaskOptions: true,
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    runParent: async (ctx, input) => {
      const runParentOutput = await taskOptions.runParent(ctx, input)
      return {
        output: runParentOutput.output,
        children: runParentOutput.children ?? [],
      }
    },
    finalize: taskOptions.finalize
      ? isFinalizeTaskOptionsParentTaskOptions(taskOptions.finalize)
        ? convertParentTaskOptionsOptionsInternal(
            taskOptions.finalize as ParentTaskOptions<unknown, unknown, unknown, unknown>,
            undefined,
          )
        : isFinalizeTaskOptionsTaskOptions(taskOptions.finalize)
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

const zTimeoutMs = z.number().int().min(1).max(3_600_000) // 1 hour

export class TaskInternal {
  readonly isParentTask: boolean
  readonly id: string
  readonly retryOptions: TaskRetryOptions
  readonly sleepMsBeforeRun: number
  readonly timeoutMs: number
  private readonly validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  private readonly runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    children: Array<ChildTask>
  }>
  readonly finalize: TaskInternal | undefined

  constructor(
    isParentTask: boolean,
    id: string,
    retryOptions: TaskRetryOptions,
    sleepMsBeforeRun: number,
    timeoutMs: number,
    validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
    runParent: (
      ctx: TaskRunContext,
      input: unknown,
    ) => Promise<{
      output: unknown
      children: Array<ChildTask>
    }>,
    finalize: TaskInternal | undefined,
  ) {
    this.isParentTask = isParentTask
    this.id = id
    this.retryOptions = retryOptions
    this.sleepMsBeforeRun = sleepMsBeforeRun
    this.timeoutMs = timeoutMs
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
      ? TaskInternal.fromTaskOptionsInternal(taskInternalsMap, taskOptions.finalize)
      : undefined

    const taskInternal = new TaskInternal(
      taskOptions.isParentTaskOptions,
      taskOptions.id,
      validatedCommonTaskOptions.retryOptions,
      validatedCommonTaskOptions.sleepMsBeforeRun,
      validatedCommonTaskOptions.timeoutMs,
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

  static fromParentTaskOptions<
    TRunInput,
    TInput = TRunInput,
    TRunOutput = unknown,
    TOutput = unknown,
    TFinalizeTaskRunOutput = unknown,
  >(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: ParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): TaskInternal {
    return TaskInternal.fromTaskOptionsInternal(
      taskInternalsMap,
      convertParentTaskOptionsOptionsInternal(
        taskOptions as ParentTaskOptions<unknown, unknown, unknown, unknown>,
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

    const timeoutCancelSignal = createTimeoutCancelSignal(timeoutMs)
    return await createCancellablePromiseCustom(
      createCancellablePromiseCustom(
        this.runParent(ctx, input),
        timeoutCancelSignal,
        new DurableExecutionTimedOutError(),
      ),
      cancelSignal,
    )
  }
}

export function getTaskHandleInternal<TOutput>(
  storage: TaskExecutionsStorageInternal,
  serializer: SerializerInternal,
  logger: Logger,
  taskId: string,
  executionId: string,
): TaskExecutionHandle<TOutput> {
  return {
    getTaskId: () => taskId,
    getExecutionId: () => executionId,
    getExecution: async () => {
      const execution = await storage.getById(executionId, {})
      if (!execution) {
        throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
      }
      return convertTaskExecutionStorageValueToTaskExecution(execution, serializer)
    },
    waitAndGetFinishedExecution: async ({
      signal,
      pollingIntervalMs,
    }: {
      signal?: CancelSignal | AbortSignal
      pollingIntervalMs?: number
    } = {}) => {
      const cancelSignal =
        signal instanceof AbortSignal ? createCancelSignal({ abortSignal: signal })[0] : signal

      const resolvedPollingIntervalMs =
        pollingIntervalMs && pollingIntervalMs > 0 ? pollingIntervalMs : 1000
      let isFirstIteration = true
      while (true) {
        if (cancelSignal?.isCancelled()) {
          throw new DurableExecutionCancelledError()
        }

        if (isFirstIteration) {
          isFirstIteration = false
        } else {
          await createCancellablePromiseCustom(sleep(resolvedPollingIntervalMs), cancelSignal)

          if (cancelSignal?.isCancelled()) {
            throw new DurableExecutionCancelledError()
          }
        }

        const execution = await storage.getById(executionId, {})
        if (!execution) {
          throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
        }

        if (FINISHED_TASK_EXECUTION_STATUSES.includes(execution.status)) {
          return convertTaskExecutionStorageValueToTaskExecution(
            execution,
            serializer,
          ) as FinishedTaskExecution<TOutput>
        } else {
          logger.debug(
            `Waiting for task ${executionId} to be finished. Status: ${execution.status}`,
          )
        }
      }
    },
    cancel: async () => {
      const now = new Date()
      await storage.updateById(
        now,
        executionId,
        {
          statuses: ACTIVE_TASK_EXECUTION_STATUSES,
        },
        {
          status: 'cancelled',
          error: convertDurableExecutionErrorToStorageValue(new DurableExecutionCancelledError()),
          needsPromiseCancellation: true,
        },
      )
      logger.debug(`Cancelled task execution ${executionId}`)
    },
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
