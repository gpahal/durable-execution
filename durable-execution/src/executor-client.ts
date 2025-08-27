import { DurableExecutionNotFoundError } from './errors'
import { DurableExecutorCore } from './executor-core'
import { type Logger, type LogLevel } from './logger'
import { type Serializer } from './serializer'
import { type TaskExecutionsStorage } from './storage'
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
  private readonly core: DurableExecutorCore
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
   * @param options.enableStorageBatching - Whether to enable storage batching. If not provided,
   *   defaults to false.
   * @param options.storageBatchingBackgroundProcessIntraBatchSleepMs - The sleep duration between
   *   batches of storage operations. Only applicable if storage batching is enabled. If not
   *   provided, defaults to 10ms.
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
      enableStorageBatching?: boolean
      storageBatchingBackgroundProcessIntraBatchSleepMs?: number
      storageMaxRetryAttempts?: number
    } = {},
  ) {
    this.core = new DurableExecutorCore(storage, options)
    this.tasks = tasks
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
    this.core.throwIfShutdown()

    const taskId = rest[0]
    const task = this.tasks[taskId]
    if (!task) {
      throw new DurableExecutionNotFoundError(`Task ${taskId} not found`)
    }

    const input = rest.length > 1 ? rest[1]! : undefined
    let options:
      | (TaskEnqueueOptions<TTasks[TTaskId]> & {
          skipTaskPresenceCheck?: boolean
          taskExecutionsStorageTransaction?: Pick<TaskExecutionsStorage, 'insertMany'>
        })
      | undefined = rest.length > 2 ? rest[2]! : undefined
    if (!options) {
      options = {}
    }
    options.skipTaskPresenceCheck = true

    // @ts-expect-error - This is safe
    return await this.core.enqueueTask(task, input, options)
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
    const task = this.tasks[taskId]
    if (!task) {
      throw new DurableExecutionNotFoundError(`Task ${taskId} not found`)
    }

    return await this.core.getTaskExecutionHandle(task, executionId)
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
    return await this.core.wakeupSleepingTaskExecution(task, sleepingTaskUniqueId, options)
  }

  /**
   * Shutdown the durable executor client. Cancels all active executions and stop executing new
   * tasks.
   */
  async shutdown(): Promise<void> {
    await this.core.shutdown()
  }
}
