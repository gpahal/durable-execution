import type { ClientRest, FriendlyClientOptions } from '@orpc/client'
import type { ErrorMap } from '@orpc/contract'
import type { ClientContext, ProcedureClient, Schema } from '@orpc/server'
import {
  type InferTaskInput,
  type InferTaskOutput,
  type TaskEnqueueOptions,
  type TaskExecution,
} from 'durable-execution'

import type { AnyTasks } from './server'

/**
 * A client for task procedures. Two procedures are created:
 * - `enqueueTask` - Enqueues a task
 * - `getTaskExecution` - Gets the execution of a task
 *
 * Calling these procedures will make a request to the server.
 */
export type TasksRouterClient<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyTasks,
> = {
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

/**
 * A handle for a task. It can be used to enqueue a task or get the execution of a task execution
 * by making a request to the server. It is created by the {@link createTaskClientHandles}
 * function.
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
 * A record of task client handles. It is created by the {@link createTaskClientHandles}
 * function.
 */
export type InferTaskClientHandles<TTasks extends AnyTasks> = {
  [K in keyof TTasks]: TaskClientHandle<InferTaskInput<TTasks[K]>, InferTaskOutput<TTasks[K]>>
}

/**
 * Creates a record of task client handles based on a record of tasks received from the server. The
 * client handles can be used to enqueue a task or get the execution of a task execution by making
 * a request to the server.
 *
 * @param client - The client to use.
 * @param tasks - The tasks to create handles for.
 * @param rest - The client options.
 * @returns A record of task client handles.
 */
export function createTaskClientHandles<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyTasks,
>(
  client: TasksRouterClient<TClientContext, TErrorMap, TTasks>,
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
