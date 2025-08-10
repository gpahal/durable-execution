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
  type DurableExecutor,
  type Task,
  type TaskCommonOptions,
  type TaskEnqueueOptions,
  type TaskExecution,
} from 'durable-execution'

import { getErrorMessage } from '@gpahal/std/errors'

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
 */
export type AnyTasks = Record<string, Task<unknown, unknown>>

function createEnqueueTaskProcedure<
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
): DecoratedProcedure<
  TInitialContext,
  TCurrentContext,
  Schema<
    {
      taskId: string
      input: unknown
      options?: TaskEnqueueOptions
    },
    {
      taskId: string
      input: unknown
      options?: TaskEnqueueOptions
    }
  >,
  Schema<string, string>,
  TErrorMap,
  TMeta
> {
  return osBuilder
    .input(
      type<{
        taskId: string
        input: unknown
        options?: TaskEnqueueOptions
      }>(),
    )
    .output(type<string>())
    .handler(async ({ input }) => {
      try {
        const handle = await executor.enqueueTask({ id: input.taskId }, input.input, input.options)
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
): DecoratedProcedure<
  TInitialContext,
  TCurrentContext,
  Schema<
    {
      taskId: string
      executionId: string
    },
    {
      taskId: string
      executionId: string
    }
  >,
  Schema<TaskExecution, TaskExecution>,
  TErrorMap,
  TMeta
> {
  return osBuilder
    .input(
      type<{
        taskId: string
        executionId: string
      }>(),
    )
    .output(type<TaskExecution>())
    .handler(async ({ input }) => {
      try {
        const handle = await executor.getTaskHandle({ id: input.taskId }, input.executionId)
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
 * Creates a router for task procedures. Two procedures are created:
 * - `enqueueTask` - Enqueues a task
 * - `getTaskExecution` - Gets the execution of a task
 *
 * @param osBuilder - The ORPC builder to use.
 * @param executor - The durable executor to use.
 * @returns A router for task procedures.
 */
export function createTasksRouter<
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
): {
  enqueueTask: DecoratedProcedure<
    TInitialContext,
    TCurrentContext,
    Schema<
      { taskId: string; input: unknown; options?: TaskEnqueueOptions },
      { taskId: string; input: unknown; options?: TaskEnqueueOptions }
    >,
    Schema<string, string>,
    TErrorMap,
    TMeta
  >
  getTaskExecution: DecoratedProcedure<
    TInitialContext,
    TCurrentContext,
    Schema<{ taskId: string; executionId: string }, { taskId: string; executionId: string }>,
    Schema<TaskExecution, TaskExecution>,
    TErrorMap,
    TMeta
  >
} {
  return {
    enqueueTask: createEnqueueTaskProcedure(osBuilder, executor),
    getTaskExecution: createGetTaskExecutionProcedure(osBuilder, executor),
  }
}

/**
 * Converts a client procedure to a task. This is useful when you want to use a client procedure as
 * a task on the server. The `run` function of the task will call the client procedure.
 *
 * @param executor - The durable executor to use.
 * @param taskOptions - The options to use.
 * @param procedure - The procedure to convert.
 * @param rest - The client options.
 * @returns A task.
 */
export function convertClientProcedureToTask<
  TClientContext extends ClientContext,
  TInputSchema extends AnySchema,
  TOutputSchema extends AnySchema,
  TErrorMap extends ErrorMap,
>(
  executor: DurableExecutor,
  taskOptions: TaskCommonOptions,
  procedure: ProcedureClient<TClientContext, TInputSchema, TOutputSchema, TErrorMap>,
  ...rest: Record<never, never> extends TClientContext
    ? [options?: FriendlyClientOptions<TClientContext>]
    : [options: FriendlyClientOptions<TClientContext>]
): Task<InferSchemaInput<TInputSchema>, InferSchemaOutput<TOutputSchema>> {
  return executor.task({
    ...taskOptions,
    run: async (_, input) => {
      const context = rest.length > 0 ? rest[0]! : undefined
      const procedureRest = [input, context] as ClientRest<
        TClientContext,
        InferSchemaInput<TInputSchema>
      >
      const { error, data, isSuccess } = await safe(procedure(...procedureRest))
      if (error) {
        const orpcError = toORPCError(error)
        switch (orpcError.code) {
          case 'NOT_FOUND': {
            throw new DurableExecutionNotFoundError(orpcError.message)
          }
          case 'INTERNAL_SERVER_ERROR': {
            throw new DurableExecutionError(orpcError.message, false, true)
          }
          default: {
            throw new DurableExecutionError(orpcError.message, false)
          }
        }
      } else if (isSuccess) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return
        return data as InferSchemaOutput<TOutputSchema>
      } else {
        throw new DurableExecutionError('Unknown error', false)
      }
    },
  })
}
