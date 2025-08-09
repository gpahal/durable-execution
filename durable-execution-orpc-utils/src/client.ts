import type { ClientRest, FriendlyClientOptions } from '@orpc/client'
import type { ErrorMap } from '@orpc/contract'
import type { ClientContext, ProcedureClient, Schema } from '@orpc/server'
import {
  type DurableTaskEnqueueOptions,
  type DurableTaskExecution,
  type InferDurableTaskInput,
  type InferDurableTaskOutput,
} from 'durable-execution'

import type { AnyDurableTasks } from './server'

/**
 * A client for durable task procedures. Two procedures are created:
 * - `enqueueTask` - Enqueues a task
 * - `getTaskExecution` - Gets the execution of a task
 *
 * Calling these procedures will make a request to the server.
 */
export type DurableTasksRouterClient<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyDurableTasks,
> = {
  enqueueTask: ProcedureClient<
    TClientContext,
    Schema<
      { taskId: keyof TTasks & string; input: unknown; options?: DurableTaskEnqueueOptions },
      { taskId: keyof TTasks & string; input: unknown; options?: DurableTaskEnqueueOptions }
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
    Schema<DurableTaskExecution, DurableTaskExecution>,
    TErrorMap
  >
}

/**
 * A handle for a durable task. It can be used to enqueue a task or get the execution of a task
 * execution by making a request to the server. It is created by the
 * {@link createDurableTaskClientHandles} function.
 */
export type DurableTaskClientHandle<TInput, TOutput> = {
  enqueue: (
    ...rest: undefined extends TInput
      ? [input?: TInput, options?: DurableTaskEnqueueOptions]
      : [input: TInput, options?: DurableTaskEnqueueOptions]
  ) => Promise<string>
  getExecution: (executionId: string) => Promise<DurableTaskExecution<TOutput>>
}

/**
 * A record of durable task client handles. It is created by the {@link createDurableTaskClientHandles}
 * function.
 */
export type InferDurableTaskClientHandles<TTasks extends AnyDurableTasks> = {
  [K in keyof TTasks]: DurableTaskClientHandle<
    InferDurableTaskInput<TTasks[K]>,
    InferDurableTaskOutput<TTasks[K]>
  >
}

/**
 * Creates a record of durable task client handles based on a record of durable tasks received from
 * the server. The client handles can be used to enqueue a task or get the execution of a task
 * execution by making a request to the server.
 *
 * @param client - The client to use.
 * @param tasks - The tasks to create handles for.
 * @param rest - The client options.
 * @returns A record of durable task client handles.
 */
export function createDurableTaskClientHandles<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyDurableTasks,
>(
  client: DurableTasksRouterClient<TClientContext, TErrorMap, TTasks>,
  tasks: TTasks,
  ...rest: Record<never, never> extends TClientContext
    ? [options?: FriendlyClientOptions<TClientContext>]
    : [options: FriendlyClientOptions<TClientContext>]
): InferDurableTaskClientHandles<TTasks> {
  const clientOptions = rest.length > 0 ? rest[0]! : undefined
  return Object.fromEntries(
    Object.keys(tasks).map((taskIdString) => {
      const taskId = taskIdString as keyof TTasks & string
      return [
        taskId,
        {
          enqueue: async (input: unknown, options?: DurableTaskEnqueueOptions) => {
            const enqueueRest = [{ taskId, input, options }, clientOptions] as ClientRest<
              TClientContext,
              {
                taskId: keyof TTasks & string
                input: unknown
                options?: DurableTaskEnqueueOptions
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
  ) as InferDurableTaskClientHandles<TTasks>
}
