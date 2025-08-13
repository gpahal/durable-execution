import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'

import { DurableExecutionError, DurableExecutionNotFoundError } from './errors'
import { zDurableExecutorOptions } from './executor'
import { createLoggerWithDebugDisabled, type Logger } from './logger'
import { WrappedSerializer, type Serializer } from './serializer'
import { createTaskExecutionStorageValue, StorageInternal, type Storage } from './storage'
import {
  type InferTaskInput,
  type InferTaskOutput,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
} from './task'
import {
  generateTaskExecutionId,
  getTaskHandleInternal,
  overrideTaskEnqueueOptions,
  validateEnqueueOptions,
} from './task-internal'

const zDurableExecutorClientOptions = zDurableExecutorOptions.pick({
  logger: true,
  serializer: true,
  enableDebug: true,
  maxSerializedInputDataSize: true,
  storageMaxRetryAttempts: true,
})

/**
 * A durable executor client. It is used to enqueue tasks to a durable executor without any of the
 * background processes.
 *
 * @category ExecutorClient
 */
export class DurableExecutorClient<TTasks extends AnyTasks> {
  private readonly logger: Logger
  private readonly storage: StorageInternal
  private readonly serializer: WrappedSerializer
  private readonly maxSerializedInputDataSize: number
  private readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private readonly tasks: TTasks

  /**
   * Create a durable executor client.
   *
   * @param storage - The storage to use for the durable executor client.
   * @param tasks - The tasks to use for the durable executor client.
   * @param options - The options for the durable executor client.
   * @param options.logger - The logger to use for the durable executor client. If not provided, a
   *   console logger will be used.
   * @param options.serializer - The serializer to use for the durable executor client. If not provided, a
   *   default serializer using superjson will be used.
   * @param options.enableDebug - Whether to enable debug logging. If `true`, debug logging will
   *   be enabled.
   * @param options.maxSerializedInputDataSize - The maximum size of serialized input data in
   *   bytes. If not provided, defaults to 1MB.
   * @param options.storageMaxRetryAttempts - The maximum number of times to retry a storage
   *   operation. If not provided, defaults to 1.
   */
  constructor(
    storage: Storage,
    tasks: TTasks,
    options: {
      logger?: Logger
      serializer?: Serializer
      enableDebug?: boolean
      maxSerializedInputDataSize?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    const parsedOptions = zDurableExecutorClientOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw new DurableExecutionError(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
        false,
      )
    }

    const { logger, serializer, enableDebug, maxSerializedInputDataSize, storageMaxRetryAttempts } =
      parsedOptions.data

    this.logger = logger
    if (!enableDebug) {
      this.logger = createLoggerWithDebugDisabled(this.logger)
    }

    this.storage = new StorageInternal(this.logger, storage, storageMaxRetryAttempts)
    this.serializer = new WrappedSerializer(serializer)
    this.maxSerializedInputDataSize = maxSerializedInputDataSize

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel
    this.tasks = tasks
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw new DurableExecutionError('Durable executor shutdown', false)
    }
  }

  /**
   * Enqueue a task for execution.
   *
   * @param rest - The task id to enqueue, input, and options.
   * @returns A handle to the task execution.
   */
  async enqueueTask<TTaskId extends keyof TTasks & string>(
    ...rest: undefined extends InferTaskInput<TTasks[TTaskId]>
      ? [
          taskId: TTaskId,
          input?: InferTaskInput<TTasks[TTaskId]>,
          options?: TaskEnqueueOptions & {
            tx?: Pick<Storage, 'insertTaskExecutions'>
          },
        ]
      : [
          taskId: TTaskId,
          input: InferTaskInput<TTasks[TTaskId]>,
          options?: TaskEnqueueOptions & {
            tx?: Pick<Storage, 'insertTaskExecutions'>
          },
        ]
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTasks[TTaskId]>>> {
    this.throwIfShutdown()

    const taskId = rest[0]
    const input = rest.length > 1 ? rest[1]! : undefined
    const options = rest.length > 2 ? rest[2]! : undefined
    const task = this.tasks[taskId]
    if (!task) {
      throw new DurableExecutionNotFoundError(`Task ${taskId} not found`)
    }

    const executionId = generateTaskExecutionId()
    const now = new Date()
    const validatedEnqueueOptions = validateEnqueueOptions(
      taskId,
      options
        ? {
            retryOptions: options.retryOptions,
            sleepMsBeforeRun: options.sleepMsBeforeRun,
            timeoutMs: options.timeoutMs,
          }
        : undefined,
    )
    const finalEnqueueOptions = overrideTaskEnqueueOptions(task, validatedEnqueueOptions)
    await (options?.tx ?? this.storage).insertTaskExecutions([
      createTaskExecutionStorageValue({
        now,
        taskId: task.id,
        executionId,
        retryOptions: finalEnqueueOptions.retryOptions,
        sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
        timeoutMs: finalEnqueueOptions.timeoutMs,
        input: this.serializer.serialize(input, this.maxSerializedInputDataSize),
      }),
    ])

    this.logger.debug(`Enqueued task ${task.id} with execution id ${executionId}`)
    return getTaskHandleInternal(this.storage, this.serializer, this.logger, task.id, executionId)
  }

  /**
   * Get a handle to a task execution.
   *
   * @param taskId - The id of the task to get the handle for.
   * @param executionId - The id of the execution to get the handle for.
   * @returns The handle to the task execution.
   */
  async getTaskHandle<TTaskId extends keyof TTasks & string>(
    taskId: TTaskId,
    executionId: string,
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTasks[TTaskId]>>> {
    if (!this.tasks[taskId]) {
      throw new DurableExecutionNotFoundError(`Task ${taskId} not found`)
    }

    const execution = await this.storage.getTaskExecutionById(executionId)
    if (!execution) {
      throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
    }
    if (execution.taskId !== taskId) {
      throw new DurableExecutionNotFoundError(
        `Task execution ${executionId} not found for task ${taskId} (belongs to ${execution.taskId})`,
      )
    }

    return getTaskHandleInternal(this.storage, this.serializer, this.logger, taskId, executionId)
  }

  /**
   * Shutdown the durable executor client. Cancels all active executions and stop executing new
   * tasks.
   */
  shutdown(): void {
    this.logger.info('Shutting down durable executor')
    if (!this.shutdownSignal.isCancelled()) {
      this.cancelShutdownSignal()
    }
    this.logger.info('Durable executor cancelled. Durable executor client shut down')
  }
}

/**
 * A record of tasks. This type signals to the client which tasks are available to be enqueued.
 *
 * @example
 * ```ts
 * const tasks = {
 *   task1: task1,
 *   task2: task2,
 * }
 * ```
 *
 * @category ExecutorClient
 */
export type AnyTasks = Record<string, Task<unknown, unknown>>
