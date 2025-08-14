import type { ClientRest, FriendlyClientOptions } from '@orpc/client'
import type { ErrorMap, Meta } from '@orpc/contract'
import type { ClientContext, Context, ProcedureClient, Schema } from '@orpc/server'
import type {
  AnyTasks,
  InferTaskInput,
  InferTaskOutput,
  TaskEnqueueOptions,
  TaskExecution,
} from 'durable-execution'

import type { TasksRouter } from './server'

/**
 * Client type for interacting with a durable executor server's task procedures.
 *
 * Provides typed access to:
 * - `enqueueTask` - Submit tasks for execution
 * - `getTaskExecution` - Get task execution status
 *
 * @example
 * ```ts
 * import type { TasksRouterClient } from 'durable-execution-orpc-utils/client'
 * import { createORPCClient } from '@orpc/client'
 *
 * import type { tasksRouter } from './executor-server'
 *
 * // Create typed client
 * const client: TasksRouterClient<typeof tasksRouter> =
 *   createORPCClient(link)
 *
 * // Enqueue a task
 * const executionId = await client.enqueueTask({
 *   taskId: 'sendEmail',
 *   input: { to: 'user@example.com', subject: 'Hello' },
 *   options: { delayMs: 5000 } // Optional delay
 * })
 *
 * // Check execution status
 * const execution = await client.getTaskExecution({
 *   taskId: 'sendEmail',
 *   executionId
 * })
 * ```
 */
export type TasksRouterClient<
  TTasksRouter extends TasksRouter<AnyTasks, Context, Context, ErrorMap, Meta>,
  TClientContext extends ClientContext = Record<never, never>,
> =
  TTasksRouter extends TasksRouter<infer TTasks, Context, Context, infer TErrorMap, Meta>
    ? {
        enqueueTask: ProcedureClient<
          TClientContext,
          Schema<
            { taskId: keyof TTasks & string; input: unknown; options?: TaskEnqueueOptions },
            { taskId: keyof TTasks & string; input: unknown; options?: TaskEnqueueOptions }
          >,
          Schema<string, string>,
          TErrorMap
        >
        getTaskExecution: ProcedureClient<
          TClientContext,
          Schema<
            { taskId: keyof TTasks & string; executionId: string },
            { taskId: keyof TTasks & string; executionId: string }
          >,
          Schema<TaskExecution, TaskExecution>,
          TErrorMap
        >
      }
    : never

/**
 * Type-safe handle for a specific task.
 *
 * Methods:
 * - `enqueue` - Submit task with typed input
 * - `getExecution` - Get execution status
 *
 * @example
 * ```ts
 * // Given a handle for a 'sendEmail' task
 * const emailHandle: TaskClientHandle<
 *   { to: string; subject: string },
 *   { messageId: string }
 * > = handles.sendEmail
 *
 * // Enqueue with type-safe input
 * const executionId = await emailHandle.enqueue({
 *   to: 'user@example.com',
 *   subject: 'Welcome!'
 * })
 *
 * // Get typed execution result
 * const execution = await emailHandle.getExecution(executionId)
 * if (execution.status === 'completed') {
 *   console.log('Message id:', execution.output.messageId)
 * }
 * ```
 */
export type TaskClientHandle<TInput, TOutput> = {
  enqueue: (
    ...rest: undefined extends TInput
      ? [input?: TInput, options?: TaskEnqueueOptions]
      : [input: TInput, options?: TaskEnqueueOptions]
  ) => Promise<string>
  getExecution: (executionId: string) => Promise<TaskExecution<TOutput>>
}

/**
 * Record mapping task ids to their typed client handles.
 *
 * @example
 * ```ts
 * type MyHandles = InferTaskClientHandles<{
 *   sendEmail: Task<{ to: string }, { messageId: string }>,
 *   processPayment: Task<{ amount: number }, { transactionId: string }>
 * }>
 *
 * // MyHandles will be:
 * // {
 * //   sendEmail: TaskClientHandle<{ to: string }, { messageId: string }>,
 * //   processPayment: TaskClientHandle<{ amount: number }, { transactionId: string }>
 * // }
 * ```
 */
export type InferTaskClientHandles<TTasks extends AnyTasks> = {
  [K in keyof TTasks]: TaskClientHandle<InferTaskInput<TTasks[K]>, InferTaskOutput<TTasks[K]>>
}

/**
 * Creates type-safe handles for all tasks on a durable executor server.
 *
 * Each handle provides typed `enqueue` and `getExecution` methods, eliminating the need to specify
 * task ids repeatedly.
 *
 * @example
 * ```ts
 * import { createTaskClientHandles } from 'durable-execution-orpc-utils/client'
 * import { createORPCClient } from '@orpc/client'
 * import { RPCLink } from '@orpc/client/fetch'
 *
 * import { tasks, type tasksRouter } from './executor-server'
 *
 * // Create client for the tasks router
 * const link = new RPCLink({
 *   url: 'http://localhost:3000/rpc',
 *   headers: () => ({ authorization: 'Bearer token' }),
 * })
 * const client: TasksRouterClient<typeof tasksRouter> = createORPCClient(link)
 *
 * // Create handles with optional default options
 * const handles = createTaskClientHandles(
 *   client,
 *   tasks,
 *   {
 *     headers: () => ({ 'x-request-id': generateId() })
 *   }
 * )
 *
 * // Use handles with full type safety
 * const emailId = await handles.sendEmail.enqueue({
 *   to: 'user@example.com',
 *   subject: 'Hello',
 *   body: 'Welcome to our service!'
 * })
 *
 * // Check status
 * const emailExecution = await handles.sendEmail.getExecution(emailId)
 *
 * // Different task, different types
 * const paymentId = await handles.processPayment.enqueue({
 *   amount: 99.99,
 *   currency: 'USD',
 *   customerId: 'cust_123'
 * })
 * ```
 *
 * @param client - The TasksRouterClient connected to your durable executor server
 * @param tasks - The record of tasks available on the server (used for type inference)
 * @param rest - Optional client configuration (headers, auth, etc.) applied to all requests
 * @returns A record where each key is a task id and each value is a TaskClientHandle
 */
export function createTaskClientHandles<
  TTasks extends AnyTasks,
  TTasksRouter extends TasksRouter<TTasks, Context, Context, ErrorMap, Meta>,
  TClientContext extends ClientContext = Record<never, never>,
>(
  client: TasksRouterClient<TTasksRouter, TClientContext>,
  tasks: TTasks,
  ...rest: Record<never, never> extends TClientContext
    ? [options?: FriendlyClientOptions<TClientContext>]
    : [options: FriendlyClientOptions<TClientContext>]
): InferTaskClientHandles<TTasks> {
  const clientOptions = rest.length > 0 ? rest[0]! : undefined
  return Object.fromEntries(
    Object.keys(tasks).map((taskIdString) => {
      const taskId = taskIdString as keyof TTasks & string
      return [
        taskId,
        {
          enqueue: async (input: unknown, options?: TaskEnqueueOptions) => {
            const enqueueRest = [{ taskId, input, options }, clientOptions] as ClientRest<
              TClientContext,
              {
                taskId: keyof TTasks & string
                input: unknown
                options?: TaskEnqueueOptions
              }
            >
            return await client.enqueueTask(...enqueueRest)
          },
          getExecution: async (executionId: string) => {
            const getExecutionRest = [{ taskId, executionId }, clientOptions] as ClientRest<
              TClientContext,
              { taskId: keyof TTasks & string; executionId: string }
            >
            return await client.getTaskExecution(...getExecutionRest)
          },
        },
      ]
    }),
  ) as InferTaskClientHandles<TTasks>
}
