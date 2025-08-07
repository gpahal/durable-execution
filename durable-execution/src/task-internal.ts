import { z } from 'zod'

import { createCancellablePromise, createTimeoutCancelSignal, type CancelSignal } from './cancel'
import { DurableExecutionError, DurableExecutionTimedOutError } from './errors'
import {
  isDurableFinalizeTaskOptionsParentTaskOptions,
  isDurableFinalizeTaskOptionsTaskOptions,
  type DurableChildTask,
  type DurableParentTaskOptions,
  type DurableTaskEnqueueOptions,
  type DurableTaskOptions,
  type DurableTaskRetryOptions,
  type DurableTaskRunContext,
} from './task'
import { generateId } from './utils'

export type DurableTaskOptionsInternal = {
  id: string
  retryOptions: DurableTaskRetryOptions | undefined
  sleepMsBeforeRun: number | undefined
  timeoutMs: number
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  disableChildrenTasksOutputsInOutput: boolean
  runParent: (
    ctx: DurableTaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    childrenTasks: Array<DurableChildTask>
  }>
  finalizeTask: DurableTaskOptionsInternal | undefined
}

function convertDurableTaskOptionsOptionsInternal(
  taskOptions: DurableTaskOptions<unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): DurableTaskOptionsInternal {
  return {
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    disableChildrenTasksOutputsInOutput: true,
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

function convertDurableParentTaskOptionsOptionsInternal(
  taskOptions: DurableParentTaskOptions<unknown, unknown, unknown, unknown>,
  validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
): DurableTaskOptionsInternal {
  return {
    id: taskOptions.id,
    retryOptions: taskOptions.retryOptions,
    sleepMsBeforeRun: taskOptions.sleepMsBeforeRun,
    timeoutMs: taskOptions.timeoutMs,
    validateInputFn,
    disableChildrenTasksOutputsInOutput: false,
    runParent: async (ctx, input) => {
      const runParentOutput = await taskOptions.runParent(ctx, input)
      return {
        output: runParentOutput.output,
        childrenTasks: runParentOutput.childrenTasks ?? [],
      }
    },
    finalizeTask: taskOptions.finalizeTask
      ? isDurableFinalizeTaskOptionsParentTaskOptions(taskOptions.finalizeTask)
        ? convertDurableParentTaskOptionsOptionsInternal(
            taskOptions.finalizeTask as DurableParentTaskOptions,
            undefined,
          )
        : isDurableFinalizeTaskOptionsTaskOptions(taskOptions.finalizeTask)
          ? convertDurableTaskOptionsOptionsInternal(
              taskOptions.finalizeTask as DurableTaskOptions,
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

export class DurableTaskInternal {
  private readonly taskInternalsMap: Map<string, DurableTaskInternal>

  readonly id: string
  readonly retryOptions: DurableTaskRetryOptions
  private readonly sleepMsBeforeRun: number
  private readonly timeoutMs: number
  private readonly validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined
  readonly disableChildrenTasksOutputsInOutput: boolean
  private readonly runParent: (
    ctx: DurableTaskRunContext,
    input: unknown,
  ) => Promise<{
    output: unknown
    childrenTasks: Array<DurableChildTask>
  }>
  readonly finalizeTask: DurableTaskInternal | undefined

  constructor(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    id: string,
    retryOptions: DurableTaskRetryOptions,
    sleepMsBeforeRun: number,
    timeoutMs: number,
    validateInputFn: ((id: string, input: unknown) => Promise<unknown>) | undefined,
    disableChildrenTasksOutputsInOutput: boolean,
    runParent: (
      ctx: DurableTaskRunContext,
      input: unknown,
    ) => Promise<{
      output: unknown
      childrenTasks: Array<DurableChildTask>
    }>,
    finalizeTask: DurableTaskInternal | undefined,
  ) {
    this.taskInternalsMap = taskInternalsMap
    this.id = id
    this.retryOptions = retryOptions
    this.sleepMsBeforeRun = sleepMsBeforeRun
    this.timeoutMs = timeoutMs
    this.validateInputFn = validateInputFn
    this.disableChildrenTasksOutputsInOutput = disableChildrenTasksOutputsInOutput
    this.runParent = runParent
    this.finalizeTask = finalizeTask
  }

  static fromDurableTaskOptionsInternal(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: DurableTaskOptionsInternal,
  ): DurableTaskInternal {
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
      ? DurableTaskInternal.fromDurableTaskOptionsInternal(
          taskInternalsMap,
          taskOptions.finalizeTask,
        )
      : undefined

    const taskInternal = new DurableTaskInternal(
      taskInternalsMap,
      taskOptions.id,
      parsedRetryOptions.data,
      parsedSleepMsBeforeRun.data,
      parsedTimeoutMs.data,
      taskOptions.validateInputFn,
      taskOptions.disableChildrenTasksOutputsInOutput,
      taskOptions.runParent,
      finalizeTask,
    )
    taskInternalsMap.set(taskInternal.id, taskInternal)
    return taskInternal
  }

  static fromDurableTaskOptions<TRunInput, TInput, TOutput>(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: DurableTaskOptions<TRunInput, TOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): DurableTaskInternal {
    return DurableTaskInternal.fromDurableTaskOptionsInternal(
      taskInternalsMap,
      convertDurableTaskOptionsOptionsInternal(
        taskOptions as DurableTaskOptions<unknown, unknown>,
        validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      ),
    )
  }

  static fromDurableParentTaskOptions<
    TRunInput,
    TInput = TRunInput,
    TRunOutput = unknown,
    TOutput = unknown,
    TFinalizeTaskRunOutput = unknown,
  >(
    taskInternalsMap: Map<string, DurableTaskInternal>,
    taskOptions: DurableParentTaskOptions<TRunInput, TRunOutput, TOutput, TFinalizeTaskRunOutput>,
    validateInputFn?: (id: string, input: TInput) => TRunInput | Promise<TRunInput>,
  ): DurableTaskInternal {
    return DurableTaskInternal.fromDurableTaskOptionsInternal(
      taskInternalsMap,
      convertDurableParentTaskOptionsOptionsInternal(
        taskOptions as DurableParentTaskOptions<unknown, unknown, unknown, unknown>,
        validateInputFn as ((id: string, input: unknown) => Promise<unknown>) | undefined,
      ),
    )
  }

  validateEnqueueOptions(options?: DurableTaskEnqueueOptions): DurableTaskEnqueueOptions {
    const validatedOptions: DurableTaskEnqueueOptions = {}

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

  getRetryOptions(options?: DurableTaskEnqueueOptions): DurableTaskRetryOptions {
    return options?.retryOptions != null ? options.retryOptions : this.retryOptions
  }

  getSleepMsBeforeRun(options?: DurableTaskEnqueueOptions): number {
    return options?.sleepMsBeforeRun != null ? options.sleepMsBeforeRun : this.sleepMsBeforeRun
  }

  getTimeoutMs(options?: DurableTaskEnqueueOptions): number {
    return options?.timeoutMs != null ? options.timeoutMs : this.timeoutMs
  }

  async validateInput(input: unknown): Promise<unknown> {
    if (!this.validateInputFn) {
      return input
    }
    return this.validateInputFn(this.id, input)
  }

  async runParentWithTimeoutAndCancellation(
    ctx: DurableTaskRunContext,
    input: unknown,
    timeoutMs: number,
    cancelSignal: CancelSignal,
  ): Promise<{
    output: unknown
    childrenTasks: Array<DurableChildTask>
  }> {
    const timeoutCancelSignal = createTimeoutCancelSignal(timeoutMs)
    return await createCancellablePromise(
      createCancellablePromise(
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
    throw new DurableExecutionError('Id cannot be empty', false)
  }
  if (id.length > 255) {
    throw new DurableExecutionError('Id cannot be longer than 255 characters', false)
  }
  if (!_TASK_ID_REGEX.test(id)) {
    throw new DurableExecutionError(
      'Id can only contain alphanumeric characters and underscores',
      false,
    )
  }
}
