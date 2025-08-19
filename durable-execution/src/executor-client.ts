import z from 'zod'

import { createCancelSignal, type CancelSignal } from '@gpahal/std/cancel'

import { DurableExecutionError, DurableExecutionNotFoundError } from './errors'
import { zDurableExecutorOptions } from './executor'
import { LoggerInternal, type Logger, type LogLevel } from './logger'
import { SerializerInternal, type Serializer } from './serializer'
import {
  createTaskExecutionStorageValue,
  TaskExecutionsStorageInternal,
  type TaskExecutionsStorage,
} from './storage'
import {
  type AnyTasks,
  type FinishedTaskExecution,
  type InferTaskInput,
  type InferTaskOutput,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecutionHandle,
  type WakeupSleepingTaskExecutionOptions,
} from './task'
import {
  generateTaskExecutionId,
  getTaskExecutionHandleInternal,
  overrideTaskEnqueueOptions,
  validateEnqueueOptions,
  wakeupSleepingTaskExecutionInternal,
} from './task-internal'

const zDurableExecutorClientOptions = zDurableExecutorOptions.pick({
  serializer: true,
  logger: true,
  logLevel: true,
  maxSerializedInputDataSize: true,
  storageMaxRetryAttempts: true,
})

/**
 * A lightweight client for enqueuing tasks to a durable execution system without running background
 * processing.
 *
 * Use `DurableExecutorClient` when you want to:
 * - Enqueue tasks from a web server or API endpoint
 * - Separate task submission from task execution
 * - Build a distributed system with dedicated worker processes
 *
 * The client shares the same storage as {@link DurableExecutor} instances but doesn't run any
 * background processes for task execution.
 *
 * @example
 * ```ts
 * // Define task types (shared between client and executor)
 * const tasks = {
 *   sendEmail: emailTask,
 *   processFile: fileTask,
 *   generateReport: reportTask
 * } as const
 *
 * // In your API server
 * const client = new DurableExecutorClient(storage, tasks)
 *
 * app.post('/api/send-email', async (req, res) => {
 *   const handle = await client.enqueueTask('sendEmail', {
 *     to: req.body.email,
 *     subject: 'Welcome!'
 *   })
 *   res.json({ executionId: handle.executionId })
 * })
 *
 * // In your worker process
 * const executor = new DurableExecutor(storage)
 * // Register the same tasks and start processing
 * ```
 *
 * @category ExecutorClient
 */
export class DurableExecutorClient<TTasks extends AnyTasks> {
  private readonly logger: LoggerInternal
  private readonly storage: TaskExecutionsStorageInternal
  private readonly serializer: SerializerInternal
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
   * @param options.serializer - The serializer to use for the durable executor client. If not provided, a
   *   default serializer using superjson will be used.
   * @param options.logger - The logger to use for the durable executor client. If not provided, a
   *   console logger will be used.
   * @param options.logLevel - The log level to use for the durable executor client. If not provided,
   *   defaults to `info`.
   * @param options.maxSerializedInputDataSize - The maximum size of serialized input data in
   *   bytes. If not provided, defaults to 1MB.
   * @param options.storageMaxRetryAttempts - The maximum number of times to retry a storage
   *   operation. If not provided, defaults to 1.
   */
  constructor(
    storage: TaskExecutionsStorage,
    tasks: TTasks,
    options: {
      serializer?: Serializer
      logger?: Logger
      logLevel?: LogLevel
      maxSerializedInputDataSize?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    const parsedOptions = zDurableExecutorClientOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    const { serializer, logger, logLevel, maxSerializedInputDataSize, storageMaxRetryAttempts } =
      parsedOptions.data

    this.serializer = new SerializerInternal(serializer)
    this.logger = new LoggerInternal(logger, logLevel)
    this.maxSerializedInputDataSize = maxSerializedInputDataSize
    this.storage = new TaskExecutionsStorageInternal(this.logger, storage, storageMaxRetryAttempts)

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.tasks = tasks
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw DurableExecutionError.nonRetryable('Durable executor client shutdown')
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
          options?: TaskEnqueueOptions<TTasks[TTaskId]> & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
          },
        ]
      : [
          taskId: TTaskId,
          input: InferTaskInput<TTasks[TTaskId]>,
          options?: TaskEnqueueOptions<TTasks[TTaskId]> & {
            taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
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
    await (options?.taskExecutionsStorageTransaction ?? this.storage).insertMany([
      createTaskExecutionStorageValue({
        now,
        taskId: task.id,
        executionId,
        isSleepingTask: task.isSleepingTask,
        retryOptions: finalEnqueueOptions.retryOptions,
        sleepMsBeforeRun: finalEnqueueOptions.sleepMsBeforeRun,
        timeoutMs: finalEnqueueOptions.timeoutMs,
        input: this.serializer.serialize(input, this.maxSerializedInputDataSize),
      }),
    ])

    this.logger.debug(`Enqueued task ${task.id} with execution id ${executionId}`)
    return getTaskExecutionHandleInternal(
      this.storage,
      this.serializer,
      this.logger,
      task.id,
      executionId,
    )
  }

  /**
   * Get a handle to a task execution.
   *
   * @param taskId - The id of the task to get the handle for.
   * @param executionId - The id of the execution to get the handle for.
   * @returns The handle to the task execution.
   */
  async getTaskExecutionHandle<TTaskId extends keyof TTasks & string>(
    taskId: TTaskId,
    executionId: string,
  ): Promise<TaskExecutionHandle<InferTaskOutput<TTasks[TTaskId]>>> {
    if (!this.tasks[taskId]) {
      throw new DurableExecutionNotFoundError(`Task ${taskId} not found`)
    }

    const execution = await this.storage.getById(executionId, {})
    if (!execution) {
      throw new DurableExecutionNotFoundError(`Task execution ${executionId} not found`)
    }
    if (execution.taskId !== taskId) {
      throw new DurableExecutionNotFoundError(
        `Task execution ${executionId} belongs to task ${execution.taskId}`,
      )
    }

    return getTaskExecutionHandleInternal(
      this.storage,
      this.serializer,
      this.logger,
      taskId,
      executionId,
    )
  }

  /**
   * Wake up a sleeping task execution. The `sleepingTaskUniqueId` is the input passed to the
   * sleeping task when it was enqueued.
   *
   * @param task - The task to wake up.
   * @param sleepingTaskUniqueId - The unique id of the sleeping task to wake up.
   * @param options - The options to wake up the sleeping task execution.
   * @returns The finished task execution.
   */
  async wakeupSleepingTaskExecution<TTask extends Task<unknown, unknown, true>>(
    task: TTask,
    sleepingTaskUniqueId: string,
    options: WakeupSleepingTaskExecutionOptions<InferTaskOutput<TTask>>,
  ): Promise<FinishedTaskExecution<InferTaskOutput<TTask>>> {
    this.throwIfShutdown()

    return await wakeupSleepingTaskExecutionInternal(
      this.storage,
      this.serializer,
      this.logger,
      task,
      sleepingTaskUniqueId,
      options,
    )
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
