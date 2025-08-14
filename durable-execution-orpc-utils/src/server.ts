import { safe, toORPCError, type ClientRest, type FriendlyClientOptions } from '@orpc/client'
import {
  ORPCError,
  type AnySchema,
  type ErrorMap,
  type InferSchemaInput,
  type InferSchemaOutput,
  type Meta,
} from '@orpc/contract'
import {
  type,
  type Builder,
  type ClientContext,
  type Context,
  type DecoratedProcedure,
  type ProcedureClient,
  type Schema,
} from '@orpc/server'
import {
  DurableExecutionError,
  DurableExecutionNotFoundError,
  type AnyTasks,
  type CommonTaskOptions,
  type DurableExecutor,
  type InferTaskInput,
  type Task,
  type TaskEnqueueOptions,
  type TaskExecution,
} from 'durable-execution'

import { getErrorMessage } from '@gpahal/std/errors'

/**
 * Type for the enqueueTask procedure that submits tasks for execution.
 */
export type EnqueueTaskProcedure<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
> = DecoratedProcedure<
  TInitialContext,
  TCurrentContext,
  Schema<
    { taskId: keyof TTasks & string; input: unknown; options?: TaskEnqueueOptions },
    { taskId: keyof TTasks & string; input: unknown; options?: TaskEnqueueOptions }
  >,
  Schema<string, string>,
  TErrorMap,
  TMeta
>

/**
 * Type for the getTaskExecution procedure that retrieves task execution status.
 */
export type GetTaskExecutionProcedure<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
> = DecoratedProcedure<
  TInitialContext,
  TCurrentContext,
  Schema<
    { taskId: keyof TTasks & string; executionId: string },
    { taskId: keyof TTasks & string; executionId: string }
  >,
  Schema<TaskExecution, TaskExecution>,
  TErrorMap,
  TMeta
>

function createEnqueueTaskProcedure<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
  TBuilder extends Builder<
    TInitialContext,
    TCurrentContext,
    Schema<unknown, unknown>,
    Schema<unknown, unknown>,
    TErrorMap,
    TMeta
  >,
>(
  osBuilder: TBuilder,
  executor: DurableExecutor,
  tasks: TTasks,
): EnqueueTaskProcedure<TTasks, TInitialContext, TCurrentContext, TErrorMap, TMeta> {
  return osBuilder
    .input(
      type<{
        taskId: keyof TTasks & string
        input: unknown
        options?: TaskEnqueueOptions
      }>(),
    )
    .output(type<string>())
    .handler(async ({ input }) => {
      const task = tasks[input.taskId as keyof TTasks]
      if (!task) {
        throw new ORPCError('NOT_FOUND', {
          message: `Task ${input.taskId} not found`,
        })
      }

      try {
        const handle = await executor.enqueueTask(
          task,
          input.input as InferTaskInput<typeof task>,
          input.options,
        )
        return handle.getExecutionId()
      } catch (error) {
        if (error instanceof DurableExecutionError) {
          if (error instanceof DurableExecutionNotFoundError) {
            throw new ORPCError('NOT_FOUND', {
              message: error.message,
            })
          }

          throw new ORPCError(error.isInternal ? 'INTERNAL_SERVER_ERROR' : 'BAD_REQUEST', {
            message: error.message,
          })
        }

        throw new ORPCError('INTERNAL_SERVER_ERROR', {
          message: getErrorMessage(error),
        })
      }
    })
}

function createGetTaskExecutionProcedure<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
  TBuilder extends Builder<
    TInitialContext,
    TCurrentContext,
    Schema<unknown, unknown>,
    Schema<unknown, unknown>,
    TErrorMap,
    TMeta
  >,
>(
  osBuilder: TBuilder,
  executor: DurableExecutor,
  tasks: TTasks,
): GetTaskExecutionProcedure<TTasks, TInitialContext, TCurrentContext, TErrorMap, TMeta> {
  return osBuilder
    .input(
      type<{
        taskId: keyof TTasks & string
        executionId: string
      }>(),
    )
    .output(type<TaskExecution>())
    .handler(async ({ input }) => {
      const task = tasks[input.taskId as keyof TTasks]
      if (!task) {
        throw new ORPCError('NOT_FOUND', {
          message: `Task ${input.taskId} not found`,
        })
      }

      try {
        const handle = await executor.getTaskHandle(task, input.executionId)
        return await handle.getExecution()
      } catch (error) {
        if (error instanceof DurableExecutionError) {
          if (error instanceof DurableExecutionNotFoundError) {
            throw new ORPCError('NOT_FOUND', {
              message: error.message,
            })
          }

          throw new ORPCError(error.isInternal ? 'INTERNAL_SERVER_ERROR' : 'BAD_REQUEST', {
            message: error.message,
          })
        }

        throw new ORPCError('INTERNAL_SERVER_ERROR', {
          message: getErrorMessage(error),
        })
      }
    })
}

/**
 * Router type containing both enqueueTask and getTaskExecution procedures.
 */
export type TasksRouter<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
> = {
  enqueueTask: EnqueueTaskProcedure<TTasks, TInitialContext, TCurrentContext, TErrorMap, TMeta>
  getTaskExecution: GetTaskExecutionProcedure<
    TTasks,
    TInitialContext,
    TCurrentContext,
    TErrorMap,
    TMeta
  >
}

/**
 * Creates an oRPC router with procedures for managing durable task executions.
 *
 * Exposes two procedures:
 * - `enqueueTask` - Submit a task for execution
 * - `getTaskExecution` - Get task execution status
 *
 * @example
 * ```ts
 * import { os } from '@orpc/server'
 * import { DurableExecutor, InMemoryTaskExecutionsStorage } from 'durable-execution'
 * import { createTasksRouter } from 'durable-execution-orpc-utils/server'
 *
 * // Create executor with storage implementation
 * const executor = new DurableExecutor(new InMemoryTaskExecutionsStorage())
 *
 * // Define tasks
 * const sendEmail = executor.task({
 *   id: 'sendEmail',
 *   timeoutMs: 30000,
 *   run: async (_, input: { to: string; subject: string }) => {
 *     // Send email logic here
 *     return { messageId: '123' }
 *   },
 * })
 *
 * const tasks = { sendEmail }
 *
 * // Create router
 * const tasksRouter = createTasksRouter(os, executor, tasks)
 *
 * // Export for use in oRPC server
 * export { tasksRouter }
 * ```
 *
 * @param osBuilder - The oRPC builder instance used to construct the procedures
 * @param executor - The DurableExecutor instance that manages task execution and persistence
 * @param tasks - A record of Task objects that can be enqueued and executed
 * @returns A router object containing the `enqueueTask` and `getTaskExecution` procedures
 */
export function createTasksRouter<
  TTasks extends AnyTasks,
  TInitialContext extends Context,
  TCurrentContext extends Context,
  TErrorMap extends ErrorMap,
  TMeta extends Meta,
  TBuilder extends Builder<
    TInitialContext,
    TCurrentContext,
    Schema<unknown, unknown>,
    Schema<unknown, unknown>,
    TErrorMap,
    TMeta
  >,
>(
  osBuilder: TBuilder,
  executor: DurableExecutor,
  tasks: TTasks,
): TasksRouter<TTasks, TInitialContext, TCurrentContext, TErrorMap, TMeta> {
  return {
    enqueueTask: createEnqueueTaskProcedure(osBuilder, executor, tasks),
    getTaskExecution: createGetTaskExecutionProcedure(osBuilder, executor, tasks),
  }
}

const RETRYABLE_STATUSES = new Set([408, 429, 500, 502, 503, 504])

/**
 * Converts an oRPC procedure client into a durable task.
 *
 * Enables business logic to remain in your application while the executor manages retries and
 * persistence. The task calls back to your app via RPC.
 *
 * Automatically handles:
 * - Error mapping (HTTP to DurableExecutionError)
 * - Retry logic (408, 429, 500, 502, 503, 504)
 * - Error classification (retryable/non-retryable)
 *
 * @example
 * ```ts
 * import { createORPCClient } from '@orpc/client'
 * import { RPCLink } from '@orpc/client/fetch'
 * import { convertProcedureClientToTask } from 'durable-execution-orpc-utils/server'
 *
 * // Create client for your web app's procedures
 * const webAppLink = new RPCLink({
 *   url: 'https://your-app.com/api/rpc',
 *   headers: () => ({ authorization: 'Bearer token' }),
 * })
 * const webAppClient = createORPCClient(webAppLink)
 *
 * // Convert a client procedure to a durable task
 * const processPayment = convertProcedureClientToTask(
 *   executor,
 *   {
 *     id: 'processPayment',
 *     timeoutMs: 60000, // 1 minute
 *     retryOptions: {
 *       maxAttempts: 3,
 *       baseDelayMs: 1000,
 *     },
 *   },
 *   webAppClient.processPayment,
 * )
 *
 * // Now processPayment can be enqueued and executed durably
 * const handle = await executor.enqueueTask(processPayment, {
 *   orderId: '123',
 *   amount: 99.99,
 * })
 * ```
 *
 * @param executor - The DurableExecutor instance that will manage this task
 * @param taskOptions - Configuration for the task including id, timeoutMs, and retry settings
 * @param procedure - The oRPC client procedure to wrap as a task
 * @param rest - Optional client configuration (headers, auth, etc.) for the RPC calls
 * @returns A task that executes the procedure when run
 */
export function convertProcedureClientToTask<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  TProcedure extends ProcedureClient<any, AnySchema, AnySchema, ErrorMap>,
>(
  executor: DurableExecutor,
  taskOptions: CommonTaskOptions,
  procedure: TProcedure,
  ...rest: TProcedure extends ProcedureClient<infer TClientContext, AnySchema, AnySchema, ErrorMap>
    ? [options?: FriendlyClientOptions<TClientContext>]
    : never
): TProcedure extends ProcedureClient<
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  any,
  infer TInputSchema,
  infer TOutputSchema,
  ErrorMap
>
  ? Task<InferSchemaInput<TInputSchema>, InferSchemaOutput<TOutputSchema>>
  : never {
  return executor.task({
    ...taskOptions,
    run: async (_, input) => {
      const context = rest.length > 0 ? rest[0]! : undefined
      const procedureRest = [input, context] as unknown as TProcedure extends ProcedureClient<
        infer TClientContext,
        infer TInputSchema,
        AnySchema,
        ErrorMap
      >
        ? ClientRest<TClientContext, InferSchemaInput<TInputSchema>>
        : never
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const { error, data, isSuccess } = await safe(procedure(...procedureRest))
      if (error) {
        const orpcError = toORPCError(error)
        if (orpcError.status === 404) {
          throw new DurableExecutionNotFoundError(orpcError.message)
        }

        let isRetryable = false
        let isInternal = false
        if (RETRYABLE_STATUSES.has(orpcError.status)) {
          isRetryable = true
        }
        if (orpcError.status >= 500) {
          isInternal = true
        }

        throw DurableExecutionError.any(orpcError.message, isRetryable, isInternal)
      } else if (isSuccess) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return data as TProcedure extends ProcedureClient<
          ClientContext,
          AnySchema,
          infer TOutputSchema,
          ErrorMap
        >
          ? InferSchemaOutput<TOutputSchema>
          : never
      } else {
        throw DurableExecutionError.nonRetryable('Unknown error')
      }
    },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  }) as TProcedure extends ProcedureClient<any, infer TInputSchema, infer TOutputSchema, ErrorMap>
    ? Task<InferSchemaInput<TInputSchema>, InferSchemaOutput<TOutputSchema>>
    : never
}
