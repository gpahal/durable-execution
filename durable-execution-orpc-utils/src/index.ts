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
  type DurableTask,
  type DurableTaskCommonOptions,
  type DurableTaskEnqueueOptions,
  type DurableTaskExecution,
  type InferDurableTaskInput,
  type InferDurableTaskOutput,
} from 'durable-execution'

import { getErrorMessage } from '@gpahal/std/errors'

export type AnyDurableTasks = Record<string, DurableTask<unknown, unknown>>

export type DurableTaskEnqueueInput<TInput> = {
  input: TInput
  options?: DurableTaskEnqueueOptions
}

function createEnqueueTaskORPCProcedure<
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
    DurableTaskEnqueueInput<unknown> & {
      taskId: string
    },
    DurableTaskEnqueueInput<unknown> & {
      taskId: string
    }
  >,
  Schema<string, string>,
  TErrorMap,
  TMeta
> {
  return osBuilder
    .input(
      type<
        DurableTaskEnqueueInput<unknown> & {
          taskId: string
        }
      >(),
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

function createGetTaskExecutionORPCProcedure<
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
  Schema<DurableTaskExecution, DurableTaskExecution>,
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
    .output(type<DurableTaskExecution>())
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

export function createDurableTaskORPCRouter<
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
      DurableTaskEnqueueInput<unknown> & { taskId: string },
      DurableTaskEnqueueInput<unknown> & { taskId: string }
    >,
    Schema<string, string>,
    TErrorMap,
    TMeta
  >
  getTaskExecution: DecoratedProcedure<
    TInitialContext,
    TCurrentContext,
    Schema<{ taskId: string; executionId: string }, { taskId: string; executionId: string }>,
    Schema<DurableTaskExecution, DurableTaskExecution>,
    TErrorMap,
    TMeta
  >
} {
  return {
    enqueueTask: createEnqueueTaskORPCProcedure(osBuilder, executor),
    getTaskExecution: createGetTaskExecutionORPCProcedure(osBuilder, executor),
  }
}

export type DurableTaskORPCRouterClient<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyDurableTasks,
> = {
  enqueueTask: ProcedureClient<
    TClientContext,
    Schema<
      DurableTaskEnqueueInput<unknown> & { taskId: keyof TTasks & string },
      DurableTaskEnqueueInput<unknown> & { taskId: keyof TTasks & string }
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

export type DurableTaskORPCHandle<TInput, TOutput> = {
  enqueue: (
    ...rest: undefined extends TInput
      ? [input?: TInput, options?: DurableTaskEnqueueOptions]
      : [input: TInput, options?: DurableTaskEnqueueOptions]
  ) => Promise<string>
  getExecution: (executionId: string) => Promise<DurableTaskExecution<TOutput>>
}

export type InferDurableTaskORPCHandles<TTasks extends AnyDurableTasks> = {
  [K in keyof TTasks]: DurableTaskORPCHandle<
    InferDurableTaskInput<TTasks[K]>,
    InferDurableTaskOutput<TTasks[K]>
  >
}

export function createDurableTaskORPCHandles<
  TClientContext extends ClientContext,
  TErrorMap extends ErrorMap,
  TTasks extends AnyDurableTasks,
>(
  client: DurableTaskORPCRouterClient<TClientContext, TErrorMap, TTasks>,
  tasks: TTasks,
  ...rest: Record<never, never> extends TClientContext
    ? [options?: FriendlyClientOptions<TClientContext>]
    : [options: FriendlyClientOptions<TClientContext>]
): InferDurableTaskORPCHandles<TTasks> {
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
              DurableTaskEnqueueInput<unknown> & { taskId: keyof TTasks & string }
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
  ) as InferDurableTaskORPCHandles<TTasks>
}

export function procedureClientTask<
  TClientContext extends ClientContext,
  TInputSchema extends AnySchema,
  TOutputSchema extends AnySchema,
  TErrorMap extends ErrorMap,
>(
  executor: DurableExecutor,
  options: DurableTaskCommonOptions,
  procedure: ProcedureClient<TClientContext, TInputSchema, TOutputSchema, TErrorMap>,
  ...rest: Record<never, never> extends TClientContext
    ? [options?: FriendlyClientOptions<TClientContext>]
    : [options: FriendlyClientOptions<TClientContext>]
): DurableTask<InferSchemaInput<TInputSchema>, InferSchemaOutput<TOutputSchema>> {
  return executor.task({
    ...options,
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
