import { z } from 'zod'

import { createTimeoutCancelSignal, type CancelSignal } from '@gpahal/std/cancel'

import { createCancellablePromiseCustom } from './cancel'
import { DurableExecutionError, DurableExecutionTimedOutError } from './errors'
import {
  isFinalizeTaskOptionsParentTaskOptions,
  isFinalizeTaskOptionsTaskOptions,
  type ChildTask,
  type ParentTaskOptions,
  type TaskEnqueueOptions,
  type TaskOptions,
  type TaskRetryOptions,
  type TaskRunContext,
} from './task'
import { generateId } from './utils'

export type TaskOptionsInternal = {
  id: string
  retryOptions: TaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  disableChildrenTaskExecutionsOutputsInOutput: boolean
  runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    childrenTasks: Array<ChildTask>
  }>
  finalizeTask: TaskOptionsInternal | undefined
}

function convertTaskOptionsOptionsInternal(
  taskOptions: TaskOptions<unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    disableChildrenTaskExecutionsOutputsInOutput: true,
    runParent: async (ctx, input) => {
      const output = await taskOptions.run(ctx, input)
      return {
        output,
        childrenTasks: [],
      }
    },
    finalizeTask: undefined,
  }
}

function convertParentTaskOptionsOptionsInternal(
  taskOptions: ParentTaskOptions<unknown, unknown, unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): TaskOptionsInternal {
  return {
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    disableChildrenTaskExecutionsOutputsInOutput: false,
    runParent: async (ctx, input) => {
      const runParentOutput = await taskOptions.runParent(ctx, input)
      return {
        output: runParentOutput.output,
        childrenTasks: runParentOutput.childrenTasks ?? [],
      }
    },
    finalizeTask: taskOptions.finalizeTask
      ? isFinalizeTaskOptionsParentTaskOptions(taskOptions.finalizeTask)
        ? convertParentTaskOptionsOptionsInternal(
            taskOptions.finalizeTask as ParentTaskOptions<unknown, unknown, unknown, unknown>,
            undefined,
          )
        : isFinalizeTaskOptionsTaskOptions(taskOptions.finalizeTask)
          ? convertTaskOptionsOptionsInternal(
              taskOptions.finalizeTask as TaskOptions<unknown, unknown>,
              undefined,
            )
          : undefined
      : undefined,
  }
}

const zRetryOptions = z
  .object({
    maxAttempts: z.number().int().min(0),
    baseDelayMs: z
      .number()
      .int()
      .min(0)
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
      }
    }
    return val
  })

const zSleepMsBeforeRun = z
  .number()
  .int()
  .nullish()
  .transform((val) => {
    if (val == null || val <= 0) {
      return 0
    }
    return val
  })

const zTimeoutMs = z.number().int().min(1)

export class TaskInternal {
  readonly id: string
  readonly retryOptions: TaskRetryOptions
  private readonly sleepMsBeforeRun: number
  private readonly timeoutMs: number
  private readonly validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  readonly disableChildrenTaskExecutionsOutputsInOutput: boolean
  private readonly runParent: (
    ctx: TaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    childrenTasks: Array<ChildTask>
  }>
  readonly finalizeTask: TaskInternal | undefined

  constructor(
    id: string,
    retryOptions: TaskRetryOptions,
    sleepMsBeforeRun: number,
    timeoutMs: number,
    validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
    disableChildrenTaskExecutionsOutputsInOutput: boolean,
    runParent: (
      ctx: TaskRunContext,
      input: unknown,
    ) => Promise<{
      output: unknown
      childrenTasks: Array<ChildTask>
    }>,
    finalizeTask: TaskInternal | undefined,
  ) {
    this.id = id
    this.retryOptions = retryOptions
    this.sleepMsBeforeRun = sleepMsBeforeRun
    this.timeoutMs = timeoutMs
    this.validateInputFn = validateInputFn
    this.disableChildrenTaskExecutionsOutputsInOutput = disableChildrenTaskExecutionsOutputsInOutput
    this.runParent = runParent
    this.finalizeTask = finalizeTask
  }

  static fromTaskOptionsInternal(
    taskInternalsMap: Map<string, TaskInternal>,
    taskOptions: TaskOptionsInternal,
  ): TaskInternal {
    validateTaskId(taskOptions.id)
    if (taskInternalsMap.has(taskOptions.id)) {
      throw new DurableExecutionError(
        `Task ${taskOptions.id} already exists. Use unique ids for tasks`,
        false,
      )
    }

    const parsedRetryOptions = zRetryOptions.safeParse(taskOptions.retryOptions)
    if (!parsedRetryOptions.success) {
      throw new DurableExecutionError(
        `Invalid retry options for task ${taskOptions.id}: ${z.prettifyError(parsedRetryOptions.error)}`,
        false,
      )
    }

    const parsedSleepMsBeforeRun = zSleepMsBeforeRun.safeParse(taskOptions.sleepMsBeforeRun)
    if (!parsedSleepMsBeforeRun.success) {
      throw new DurableExecutionError(
        `Invalid sleep ms before run for task ${taskOptions.id}: ${z.prettifyError(parsedSleepMsBeforeRun.error)}`,
        false,
      )
    }

    const parsedTimeoutMs = zTimeoutMs.safeParse(taskOptions.timeoutMs)
    if (!parsedTimeoutMs.success) {
      throw new DurableExecutionError(
        `Invalid timeout value for task ${taskOptions.id}: ${z.prettifyError(parsedTimeoutMs.error)}`,
        false,
      )
    }

    const finalizeTask = taskOptions.finalizeTask
      ? TaskInternal.fromTaskOptionsInternal(taskInternalsMap, taskOptions.finalizeTask)
      : undefined

    const taskInternal = new TaskInternal(
      taskOptions.id,
      parsedRetryOptions.data,
      parsedSleepMsBeforeRun.data,
      parsedTimeoutMs.data,
      taskOptions.validateInputFn,
      taskOptions.disableChildrenTaskExecutionsOutputsInOutput,
      taskOptions.runParent,
      finalizeTask,
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

  validateEnqueueOptions(options?: TaskEnqueueOptions): TaskEnqueueOptions {
    const validatedOptions: TaskEnqueueOptions = {}

    if (options?.retryOptions) {
      const parsedRetryOptions = zRetryOptions.safeParse(options.retryOptions)
      if (!parsedRetryOptions.success) {
        throw new DurableExecutionError(
          `Invalid retry options for task ${this.id}: ${z.prettifyError(parsedRetryOptions.error)}`,
          false,
        )
      }

      validatedOptions.retryOptions = parsedRetryOptions.data
    }

    if (options?.sleepMsBeforeRun) {
      const parsedSleepMsBeforeRun = zSleepMsBeforeRun.safeParse(options.sleepMsBeforeRun)
      if (!parsedSleepMsBeforeRun.success) {
        throw new DurableExecutionError(
          `Invalid sleep ms before run for task ${this.id}: ${z.prettifyError(parsedSleepMsBeforeRun.error)}`,
          false,
        )
      }

      validatedOptions.sleepMsBeforeRun = parsedSleepMsBeforeRun.data
    }

    if (options?.timeoutMs) {
      const parsedTimeoutMs = zTimeoutMs.safeParse(options.timeoutMs)
      if (!parsedTimeoutMs.success) {
        throw new DurableExecutionError(
          `Invalid timeout value for task ${this.id}: ${z.prettifyError(parsedTimeoutMs.error)}`,
          false,
        )
      }

      validatedOptions.timeoutMs = parsedTimeoutMs.data
    }

    return validatedOptions
  }

  getRetryOptions(options?: TaskEnqueueOptions): TaskRetryOptions {
    return options?.retryOptions != null ? options.retryOptions : this.retryOptions
  }

  getSleepMsBeforeRun(options?: TaskEnqueueOptions): number {
    return options?.sleepMsBeforeRun != null ? options.sleepMsBeforeRun : this.sleepMsBeforeRun
  }

  getTimeoutMs(options?: TaskEnqueueOptions): number {
    return options?.timeoutMs != null ? options.timeoutMs : this.timeoutMs
  }

  async validateInput(input: unknown): Promise<unknown> {
    if (!this.validateInputFn) {
      return input
    }
    return this.validateInputFn(this.id, input)
  }

  async runParentWithTimeoutAndCancellation(
    ctx: TaskRunContext,
    input: unknown,
    timeoutMs: number,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    childrenTasks: Array<ChildTask>
  }> {
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
    throw new DurableExecutionError('Task id cannot be empty', false)
  }
  if (id.length > 255) {
    throw new DurableExecutionError('Task id cannot be longer than 255 characters', false)
  }
  if (!_TASK_ID_REGEX.test(id)) {
    throw new DurableExecutionError(
      'Task id can only contain alphanumeric characters and underscores',
      false,
    )
  }
}
