import {
  actionGeneric,
  mutationGeneric,
  queryGeneric,
  type ApiFromModules,
  type Expand,
  type FunctionReference,
} from 'convex/server'
import type { GenericId } from 'convex/values'
import {
  createCancelSignal,
  DurableExecutionCancelledError,
  DurableExecutionError,
  LoggerInternal,
  zLogger,
  zLogLevel,
  type CancelSignal,
  type Logger,
  type LogLevel,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from 'durable-execution'
import { z } from 'zod'

import { sleep, sleepWithJitter } from '@gpahal/std/promises'

import {
  taskExecutionDBValueToStorageValue,
  taskExecutionStorageUpdateToDBUpdateRequest,
  taskExecutionStorageValueToDBInsertValue,
  type TaskExecutionDBUpdate,
  type TaskExecutionDBValue,
} from '../common'
import type { Mounts } from '../component/_generated/api'
import type { TaskExecutionDBInsertValue } from '../component/schema'

/**
 * A type that represents the opaque IDs of a Convex document.
 *
 * @internal
 */
export type OpaqueIds<T> =
  T extends GenericId<infer _T>
    ? string
    : T extends Array<infer U>
      ? Array<OpaqueIds<U>>
      : T extends object
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T

/**
 * A type that represents the internal API of a component.
 *
 * @internal
 */
export type ComponentInternalApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    'public',
    infer FArgs,
    infer FReturnType,
    infer FComponentPath
  >
    ? FunctionReference<FType, 'internal', OpaqueIds<FArgs>, OpaqueIds<FReturnType>, FComponentPath>
    : ComponentInternalApi<API[mod]>
}>

/**
 * The type of the task executions storage component.
 */
export type TaskExecutionsStorageComponent = ComponentInternalApi<Mounts>

/**
 * Converts a task executions storage component to a public api implementation.
 *
 * @example
 * ```ts
 * import { convertDurableExecutionStorageComponentToPublicApiImpl } from 'durable-execution-storage-convex'
 *
 * import { components } from './_generated/api'
 *
 * export const {
 *   insertMany,
 *   getById,
 *   getManyById,
 *   getBySleepingTaskUniqueId,
 *   updateManyById,
 *   updateByIdAndInsertChildrenIfUpdated,
 *   updateManyByIdAndInsertChildrenIfUpdated,
 *   updateByStatusAndStartAtLessThanAndReturn,
 *   updateByStatusAndOCFPStatusAndACCZeroAndReturn,
 *   updateByCloseStatusAndReturn,
 *   updateByIsSleepingTaskAndExpiresAtLessThan,
 *   updateByOCFPExpiresAt,
 *   updateByCloseExpiresAt,
 *   updateByExecutorIdAndNPCAndReturn,
 *   getByParentExecutionId,
 *   updateByParentExecutionIdAndIsFinished,
 *   updateAndDecrementParentACCByIsFinishedAndCloseStatus,
 *   deleteById,
 *   deleteAll,
 * } = convertDurableExecutionStorageComponentToPublicApiImpl(
 *   components.taskExecutionsStorage,
 *   'SUPER_SECRET',
 * )
 * ```
 *
 * @param component - The task executions storage component.
 * @param authSecret - The auth secret to use for the public API.
 * @returns The public API implementation.
 */
export function convertDurableExecutionStorageComponentToPublicApiImpl(
  component: TaskExecutionsStorageComponent,
  authSecret: string,
) {
  const verifyArgs = <T>(args: { authSecret: string; args: T }) => {
    if (!args?.authSecret) {
      throw new Error('Invalid auth secret')
    }
    if (args.authSecret !== authSecret) {
      throw new Error('Invalid auth secret')
    }
    return args.args
  }

  return {
    insertMany: mutationGeneric(
      async (
        ctx,
        args: { authSecret: string; args: { executions: Array<TaskExecutionDBInsertValue> } },
      ) => {
        await ctx.runMutation(component.lib.insertMany, verifyArgs(args))
      },
    ),
    getById: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { executionId: string; filters: TaskExecutionStorageGetByIdFilters }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getById,
          verifyArgs(args),
        )) as TaskExecutionDBValue | null
      },
    ),
    getManyById: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: Array<{ executionId: string; filters: TaskExecutionStorageGetByIdFilters }>
          }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getManyById,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue | null>
      },
    ),
    getBySleepingTaskUniqueId: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { sleepingTaskUniqueId: string }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getBySleepingTaskUniqueId,
          verifyArgs(args),
        )) as TaskExecutionDBValue | null
      },
    ),
    updateManyById: mutationGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: Array<{
              executionId: string
              filters: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionDBUpdate
            }>
          }
        },
      ) => {
        await ctx.runMutation(component.lib.updateManyById, verifyArgs(args))
      },
    ),
    updateByIdAndInsertChildrenIfUpdated: mutationGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            executionId: string
            filters: TaskExecutionStorageGetByIdFilters
            update: TaskExecutionStorageUpdate
            childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionDBInsertValue>
          }
        },
      ) => {
        await ctx.runMutation(component.lib.updateByIdAndInsertChildrenIfUpdated, verifyArgs(args))
      },
    ),
    updateManyByIdAndInsertChildrenIfUpdated: mutationGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: Array<{
              executionId: string
              filters: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionStorageUpdate
              childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionDBInsertValue>
            }>
          }
        },
      ) => {
        await ctx.runMutation(
          component.lib.updateManyByIdAndInsertChildrenIfUpdated,
          verifyArgs(args),
        )
      },
    ),
    updateByStatusAndStartAtLessThanAndReturn: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            shard: number
            status: TaskExecutionStatus
            startAtLessThan: number
            update: TaskExecutionStorageUpdate
            updateExpiresAtWithStartedAt: number
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByStatusAndStartAtLessThanAndReturn,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue>
      },
    ),
    updateByStatusAndOCFPStatusAndACCZeroAndReturn: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            shard: number
            status: TaskExecutionStatus
            ocfpStatus: TaskExecutionOnChildrenFinishedProcessingStatus
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByStatusAndOCFPStatusAndACCZeroAndReturn,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue>
      },
    ),
    updateByCloseStatusAndReturn: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            shard: number
            closeStatus: TaskExecutionCloseStatus
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByCloseStatusAndReturn,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue>
      },
    ),
    updateByIsSleepingTaskAndExpiresAtLessThan: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            isSleepingTask: boolean
            expiresAtLessThan: number
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByIsSleepingTaskAndExpiresAtLessThan,
          verifyArgs(args),
        )) as number
      },
    ),
    updateByOCFPExpiresAt: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            ocfpExpiresAtLessThan: number
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByOCFPExpiresAt,
          verifyArgs(args),
        )) as number
      },
    ),
    updateByCloseExpiresAt: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { closeExpiresAtLessThan: number; update: TaskExecutionDBUpdate; limit: number }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByCloseExpiresAt,
          verifyArgs(args),
        )) as number
      },
    ),
    updateByExecutorIdAndNPCAndReturn: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            shard: number
            executorId: string
            npc: boolean
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateByExecutorIdAndNPCAndReturn,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue>
      },
    ),
    getByParentExecutionId: queryGeneric(
      async (ctx, args: { authSecret: string; args: { parentExecutionId: string } }) => {
        return (await ctx.runQuery(
          component.lib.getByParentExecutionId,
          verifyArgs(args),
        )) as Array<TaskExecutionDBValue>
      },
    ),
    updateByParentExecutionIdAndIsFinished: mutationGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { parentExecutionId: string; isFinished: boolean; update: TaskExecutionDBUpdate }
        },
      ) => {
        await ctx.runMutation(
          component.lib.updateByParentExecutionIdAndIsFinished,
          verifyArgs(args),
        )
      },
    ),
    updateAndDecrementParentACCByIsFinishedAndCloseStatus: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            shard: number
            isFinished: boolean
            closeStatus: TaskExecutionCloseStatus
            update: TaskExecutionDBUpdate
            limit: number
          }
        },
      ) => {
        return (await ctx.runAction(
          component.lib.updateAndDecrementParentACCByIsFinishedAndCloseStatus,
          verifyArgs(args),
        )) as number
      },
    ),
    deleteById: mutationGeneric(
      async (ctx, args: { authSecret: string; args: { executionId: string } }) => {
        await ctx.runMutation(component.lib.deleteById, verifyArgs(args))
      },
    ),
    deleteAll: actionGeneric(async (ctx, args: { authSecret: string; args: undefined }) => {
      await ctx.runAction(component.lib.deleteAll, verifyArgs(args))
    }),
  }
}

export type DurableExecutionStoragePublicApiImpl = ReturnType<
  typeof convertDurableExecutionStorageComponentToPublicApiImpl
>

export type DurableExecutionStoragePublicApi = ApiFromModules<{
  lib: DurableExecutionStoragePublicApiImpl
}>['lib']

/**
 * A type that can be used to interact with a Convex database.
 *
 * It can be a ConvexClient or ConvexHttpClient client or any object that implements the required
 * methods.
 */
export type AnyConvexClient = {
  /**
   * Fetch a query result once.
   *
   * @param query - A `FunctionReference` for the public query to run.
   * @param args - An arguments object for the query.
   * @returns A promise of the query's result.
   */
  query: <Query extends FunctionReference<'query'>>(
    query: Query,
    args: Query['_args'],
  ) => Promise<Awaited<Query['_returnType']>>
  /**
   * Run a mutation.
   *
   * @param mutation - A `FunctionReference` for the public mutation to run.
   * @param args - An arguments object for the mutation.
   * @returns A promise of the mutation's result.
   */
  mutation: <Mutation extends FunctionReference<'mutation'>>(
    mutation: Mutation,
    args: Mutation['_args'],
  ) => Promise<Awaited<Mutation['_returnType']>>
  /**
   * Run an action.
   *
   * @param action - A `FunctionReference` for the public action to run.
   * @param args - An arguments object for the action.
   * @returns A promise of the action's result.
   */
  action: <Action extends FunctionReference<'action'>>(
    action: Action,
    args: Action['_args'],
  ) => Promise<Awaited<Action['_returnType']>>
}

/**
 * A type that can be used to batch requests to a Convex database.
 */
export type BatchRequest<T, R> = {
  data: T
  resolve: (value: R) => void
  reject: (reason?: unknown) => void
}

/**
 * A schema for the options of the Convex task executions storage.
 */
export const zConvexTaskExecutionsStorageOptions = z
  .object({
    totalShards: z
      .number()
      .min(1)
      .max(64)
      .nullish()
      .transform((val) => val ?? 1),
    shards: z.array(z.number().min(0)).nullish(),
    enableTestMode: z
      .boolean()
      .nullish()
      .transform((val) => val ?? false),
    logger: zLogger.nullish(),
    logLevel: zLogLevel.nullish(),
  })
  .transform((val) => {
    let finalShards: Array<number>
    if (!val.shards) {
      finalShards = Array.from({ length: val.totalShards }, (_, i) => i)
    } else if (val.shards.some((shard) => shard < 0 || shard >= val.totalShards)) {
      throw new Error('Shards must be an array of numbers between 0 and totalShards')
    } else {
      const shardsSet = new Set(val.shards)
      finalShards = [...shardsSet].sort((a, b) => a - b)
    }

    return {
      ...val,
      shards: finalShards,
    }
  })

/**
 * A task executions storage implementation that uses a Convex database.
 */
export class ConvexTaskExecutionsStorage implements TaskExecutionsStorage {
  private readonly convexClient: AnyConvexClient
  private readonly authSecret: string
  private readonly publicApi: DurableExecutionStoragePublicApi
  private readonly totalShards: number
  private readonly shards: Array<number>
  private readonly enableTestMode: boolean
  private readonly logger: LoggerInternal
  private readonly shutdownSignal: CancelSignal
  private readonly cancelShutdownSignal: () => void
  private insertManyRequests: Array<
    BatchRequest<
      {
        executions: Array<TaskExecutionStorageValue>
      },
      void
    >
  >
  private getByIdRequests: Array<
    BatchRequest<
      {
        executionId: string
        filters: TaskExecutionStorageGetByIdFilters
      },
      TaskExecutionStorageValue | undefined
    >
  >
  private updateByIdRequests: Array<
    BatchRequest<
      {
        executionId: string
        filters: TaskExecutionStorageGetByIdFilters
        update: TaskExecutionStorageUpdate
      },
      void
    >
  >
  private updateByIdAndInsertChildrenIfUpdatedRequests: Array<
    BatchRequest<
      {
        executionId: string
        filters: TaskExecutionStorageGetByIdFilters
        update: TaskExecutionStorageUpdate
        childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>
      },
      void
    >
  >
  private backgroundProcessesPromises: Set<Promise<void>>
  private backgroundPromises: Set<Promise<void>>
  private timingStats: Map<string, { count: number; meanMs: number }>
  private perSecondConvexCallStats: Map<number, number>

  /**
   * Creates a new task executions storage instance.
   *
   * @param convexClient - A Convex client that can be used to interact with the database.
   * @param authSecret - The auth secret to use for the public API.
   * @param publicApi - The public API to use. See
   *   {@link convertDurableExecutionStorageComponentToPublicApiImpl} for more details.
   * @param options - An options object.
   * @param options.totalShards - The total number of shards to use.
   * @param options.shards - The shards to use. If not provided, all shards will be used.
   * @param options.enableTestMode - Whether to enable test mode.
   * @param options.logger - The logger to use.
   * @param options.logLevel - The log level to use.
   */
  constructor(
    convexClient: AnyConvexClient,
    authSecret: string,
    publicApi: DurableExecutionStoragePublicApi,
    options: {
      totalShards?: number
      shards?: Array<number>
      enableTestMode?: boolean
      logger?: Logger
      logLevel?: LogLevel
    } = {},
  ) {
    const parsedOptions = zConvexTaskExecutionsStorageOptions.safeParse(options)
    if (!parsedOptions.success) {
      throw DurableExecutionError.nonRetryable(
        `Invalid options: ${z.prettifyError(parsedOptions.error)}`,
      )
    }

    this.convexClient = convexClient
    this.authSecret = authSecret
    this.publicApi = publicApi
    this.totalShards = parsedOptions.data.totalShards
    this.shards = parsedOptions.data.shards
    this.enableTestMode = parsedOptions.data.enableTestMode
    this.logger = new LoggerInternal(
      parsedOptions.data.logger,
      parsedOptions.data.logLevel,
      'ConvexTaskExecutionsStorage',
    )

    this.insertManyRequests = []
    this.getByIdRequests = []
    this.updateByIdRequests = []
    this.updateByIdAndInsertChildrenIfUpdatedRequests = []

    const [cancelSignal, cancel] = createCancelSignal()
    this.shutdownSignal = cancelSignal
    this.cancelShutdownSignal = cancel

    this.backgroundProcessesPromises = new Set()
    this.backgroundPromises = new Set()
    this.timingStats = new Map()
    this.perSecondConvexCallStats = new Map()
  }

  private throwIfShutdown(): void {
    if (this.shutdownSignal.isCancelled()) {
      throw DurableExecutionError.nonRetryable('Durable executor shutdown')
    }
  }

  private addBackgroundProcessesPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundProcessesPromises.add(
      wrappedPromise().finally(() => this.backgroundProcessesPromises.delete(promise)),
    )
  }

  private addBackgroundPromise(promise: Promise<void>): void {
    const wrappedPromise = async () => {
      await promise
    }

    this.backgroundPromises.add(
      wrappedPromise().finally(() => this.backgroundPromises.delete(promise)),
    )
  }

  /**
   * Run a function with timing stats.
   *
   * @param key - The key to use for the timing stats.
   * @param fn - The function to run.
   * @returns The result of the function.
   */
  async withTimingStats<T>(key: string, fn: () => T | Promise<T>): Promise<T> {
    const start = performance.now()
    const result = await fn()
    const durationMs = performance.now() - start

    if (!this.timingStats.has(key)) {
      this.timingStats.set(key, { count: 0, meanMs: 0 })
    }
    const timingStat = this.timingStats.get(key)!
    timingStat.count++
    timingStat.meanMs = (timingStat.meanMs * (timingStat.count - 1) + durationMs) / timingStat.count
    return result
  }

  private addConvexCalls(count = 1): void {
    const second = Math.floor(Date.now() / 1000)
    this.perSecondConvexCallStats.set(
      second,
      (this.perSecondConvexCallStats.get(second) ?? 0) + count,
    )
  }

  startBackgroundProcesses(): void {
    this.throwIfShutdown()

    this.logger.info('Starting background processes')
    this.addBackgroundProcessesPromise(
      this.startBackgroundProcessesInternal().catch((error) => {
        console.error('Background processes exited with error', error)
      }),
    )
    this.logger.info('Started background processes')
  }

  private async startBackgroundProcessesInternal(): Promise<void> {
    await Promise.all([
      this.insertManyBackgroundProcess(),
      this.getByIdBackgroundProcess(),
      this.updateByIdBackgroundProcess(),
      this.updateByIdAndInsertChildrenIfUpdatedBackgroundProcess(),
    ])
  }

  async shutdown(): Promise<void> {
    this.logger.info('Shutting down convex task executions storage')
    if (!this.shutdownSignal.isCancelled()) {
      this.cancelShutdownSignal()
    }
    this.logger.info('Convex task executions storage cancelled')

    if (this.backgroundProcessesPromises.size > 0 || this.backgroundPromises.size > 0) {
      this.logger.info('Stopping background processes and promises')
      await Promise.all([...this.backgroundProcessesPromises, ...this.backgroundPromises])
      this.backgroundProcessesPromises.clear()
      this.backgroundPromises.clear()
    }
    this.logger.info('Background processes and promises stopped')

    this.logger.info('Stopping active requests')
    for (const request of this.insertManyRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }
    for (const request of this.getByIdRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }
    for (const request of this.updateByIdRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }
    for (const request of this.updateByIdAndInsertChildrenIfUpdatedRequests) {
      request.reject(DurableExecutionError.nonRetryable('Durable executor shutdown'))
    }

    this.insertManyRequests = []
    this.getByIdRequests = []
    this.updateByIdRequests = []
    this.updateByIdAndInsertChildrenIfUpdatedRequests = []

    this.logger.info('Active requests stopped')
    this.logger.info('Convex task executions storage shut down')
  }

  protected async runBackgroundProcess<T, R>(
    processName: string,
    getRequests: () => Array<BatchRequest<T, R>>,
    unsetRequests: () => void,
    batchSize: number,
    backgroundProcessIntraBatchSleepMs: number,
    singleRequestsBatchProcessFn: (requests: Array<T>) => Promise<Array<R>>,
  ): Promise<void> {
    let consecutiveErrors = 0
    const maxConsecutiveErrors = 10

    const originalBackgroundProcessIntraBatchSleepMs = backgroundProcessIntraBatchSleepMs

    const runBackgroundProcessSingleBatch = async (
      requests: Array<BatchRequest<T, R>>,
    ): Promise<void> => {
      try {
        const results = await singleRequestsBatchProcessFn(requests.map((request) => request.data))
        for (const [i, request] of requests.entries()) {
          request.resolve(results[i]!)
        }

        consecutiveErrors = 0
        backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs
      } catch (error) {
        for (const request of requests) {
          request.reject(error)
        }

        if (error instanceof DurableExecutionCancelledError && this.shutdownSignal.isCancelled()) {
          return
        }

        consecutiveErrors++
        console.error(`Error in ${processName}: consecutive_errors=${consecutiveErrors}`, error)

        if (consecutiveErrors >= maxConsecutiveErrors) {
          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.125,
            backgroundProcessIntraBatchSleepMs * 2.5,
          )
        }
      }
    }

    await sleepWithJitter(backgroundProcessIntraBatchSleepMs)
    let skippedBatchCount = 0
    while (!this.shutdownSignal.isCancelled()) {
      try {
        const requests = getRequests()
        if (requests.length === 0) {
          await sleep(backgroundProcessIntraBatchSleepMs)
          backgroundProcessIntraBatchSleepMs = Math.min(
            backgroundProcessIntraBatchSleepMs * 1.125,
            originalBackgroundProcessIntraBatchSleepMs * 2,
          )
          skippedBatchCount = 0
          continue
        } else {
          backgroundProcessIntraBatchSleepMs = originalBackgroundProcessIntraBatchSleepMs
          if (skippedBatchCount === 0 && requests.length < batchSize / 10) {
            await sleep(originalBackgroundProcessIntraBatchSleepMs)
            skippedBatchCount++
          } else {
            skippedBatchCount = 0
          }
        }

        unsetRequests()
        this.addConvexCalls(Math.ceil(requests.length / batchSize))
        for (let i = 0; i < requests.length; i += batchSize) {
          this.addBackgroundPromise(
            runBackgroundProcessSingleBatch(requests.slice(i, i + batchSize)),
          )
        }
      } catch (error) {
        console.error(`Error in ${processName}`, error)
      }
    }
  }

  private async insertManyBackgroundProcess(): Promise<void> {
    await this.runBackgroundProcess(
      'insertMany',
      () => this.insertManyRequests,
      () => {
        this.insertManyRequests = []
      },
      100,
      25,
      async (requests) => {
        await this.withTimingStats('insertMany', () =>
          this.convexClient.mutation(this.publicApi.insertMany, {
            authSecret: this.authSecret,
            args: {
              executions: requests
                .flatMap((request) => request.executions)
                .map((value) =>
                  taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
                ),
            },
          }),
        )
        return Array.from({ length: requests.length }).fill(undefined) as Array<void>
      },
    )
  }

  private async getByIdBackgroundProcess(): Promise<void> {
    await this.runBackgroundProcess(
      'getById',
      () => this.getByIdRequests,
      () => {
        this.getByIdRequests = []
      },
      100,
      25,
      async (requests) => {
        const dbValues = await this.withTimingStats('getManyById', () =>
          this.convexClient.query(this.publicApi.getManyById, {
            authSecret: this.authSecret,
            args: {
              requests,
            },
          }),
        )
        return dbValues.map((dbValue) =>
          dbValue ? taskExecutionDBValueToStorageValue(dbValue) : undefined,
        )
      },
    )
  }

  private async updateByIdBackgroundProcess(): Promise<void> {
    await this.runBackgroundProcess(
      'updateById',
      () => this.updateByIdRequests,
      () => {
        this.updateByIdRequests = []
      },
      100,
      10,
      async (requests) => {
        await this.withTimingStats('updateManyById', () =>
          this.convexClient.mutation(this.publicApi.updateManyById, {
            authSecret: this.authSecret,
            args: {
              requests: requests.map((request) => ({
                executionId: request.executionId,
                filters: request.filters,
                update: taskExecutionStorageUpdateToDBUpdateRequest(request.update),
              })),
            },
          }),
        )
        return Array.from({ length: requests.length }).fill(undefined) as Array<void>
      },
    )
  }

  private async updateByIdAndInsertChildrenIfUpdatedBackgroundProcess(): Promise<void> {
    await this.runBackgroundProcess(
      'updateByIdAndInsertChildrenIfUpdated',
      () => this.updateByIdAndInsertChildrenIfUpdatedRequests,
      () => {
        this.updateByIdAndInsertChildrenIfUpdatedRequests = []
      },
      50,
      10,
      async (requests) => {
        await this.withTimingStats('updateManyByIdAndInsertChildrenIfUpdated', () =>
          this.convexClient.mutation(this.publicApi.updateManyByIdAndInsertChildrenIfUpdated, {
            authSecret: this.authSecret,
            args: {
              requests: requests.map((request) => ({
                executionId: request.executionId,
                filters: request.filters,
                update: taskExecutionStorageUpdateToDBUpdateRequest(request.update),
                childrenTaskExecutionsToInsertIfAnyUpdated:
                  request.childrenTaskExecutionsToInsertIfAnyUpdated.map((value) =>
                    taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
                  ),
              })),
            },
          }),
        )
        return Array.from({ length: requests.length }).fill(undefined) as Array<void>
      },
    )
  }

  async insertMany(executions: Array<TaskExecutionStorageValue>): Promise<void> {
    this.throwIfShutdown()

    if (executions.length === 0) {
      return
    }

    if (executions.length <= 1) {
      const promise = new Promise<void>((resolve, reject) => {
        this.insertManyRequests.push({ data: { executions }, resolve, reject })
      })
      await promise
      return
    }

    this.addConvexCalls()
    await this.withTimingStats('insertMany', () =>
      this.convexClient.mutation(this.publicApi.insertMany, {
        authSecret: this.authSecret,
        args: {
          executions: executions.map((value) =>
            taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
          ),
        },
      }),
    )
  }

  getById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
  ): Promise<TaskExecutionStorageValue | undefined> {
    this.throwIfShutdown()

    const promise = new Promise<TaskExecutionStorageValue | undefined>((resolve, reject) => {
      this.getByIdRequests.push({ data: { executionId, filters }, resolve, reject })
    })
    return promise
  }

  async getBySleepingTaskUniqueId(
    sleepingTaskUniqueId: string,
  ): Promise<TaskExecutionStorageValue | undefined> {
    this.throwIfShutdown()

    this.addConvexCalls()
    const result = await this.withTimingStats('getBySleepingTaskUniqueId', () =>
      this.convexClient.query(this.publicApi.getBySleepingTaskUniqueId, {
        authSecret: this.authSecret,
        args: {
          sleepingTaskUniqueId,
        },
      }),
    )

    return result ? taskExecutionDBValueToStorageValue(result) : undefined
  }

  updateById(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    this.throwIfShutdown()

    const promise = new Promise<void>((resolve, reject) => {
      this.updateByIdRequests.push({
        data: {
          executionId,
          filters,
          update,
        },
        resolve,
        reject,
      })
    })
    return promise
  }

  async updateByIdAndInsertChildrenIfUpdated(
    executionId: string,
    filters: TaskExecutionStorageGetByIdFilters,
    update: TaskExecutionStorageUpdate,
    childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionStorageValue>,
  ): Promise<void> {
    this.throwIfShutdown()

    if (childrenTaskExecutionsToInsertIfAnyUpdated.length === 0) {
      await this.updateById(executionId, filters, update)
      return
    }

    if (childrenTaskExecutionsToInsertIfAnyUpdated.length <= 1) {
      const promise = new Promise<void>((resolve, reject) => {
        this.updateByIdAndInsertChildrenIfUpdatedRequests.push({
          data: { executionId, filters, update, childrenTaskExecutionsToInsertIfAnyUpdated },
          resolve,
          reject,
        })
      })
      await promise
      return
    }

    this.addConvexCalls()
    await this.withTimingStats('updateByIdAndInsertChildrenIfUpdated', () =>
      this.convexClient.mutation(this.publicApi.updateByIdAndInsertChildrenIfUpdated, {
        authSecret: this.authSecret,
        args: {
          executionId,
          filters,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
          childrenTaskExecutionsToInsertIfAnyUpdated:
            childrenTaskExecutionsToInsertIfAnyUpdated.map((value) =>
              taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
            ),
        },
      }),
    )
  }

  async updateByStatusAndStartAtLessThanAndReturn(
    status: TaskExecutionStatus,
    startAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    updateExpiresAtWithStartedAt: number,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    this.throwIfShutdown()

    this.addConvexCalls(this.shards.length)
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.withTimingStats('updateByStatusAndStartAtLessThanAndReturn', () =>
          this.convexClient.action(this.publicApi.updateByStatusAndStartAtLessThanAndReturn, {
            authSecret: this.authSecret,
            args: {
              shard,
              status,
              startAtLessThan,
              update: taskExecutionStorageUpdateToDBUpdateRequest(update),
              updateExpiresAtWithStartedAt,
              limit: Math.ceil(limit / this.shards.length),
            },
          }),
        ),
      ),
    )

    return dbValuesArr
      .flat()
      .map((dbValue) =>
        taskExecutionDBValueToStorageValue(dbValue, update, updateExpiresAtWithStartedAt),
      )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn(
    status: TaskExecutionStatus,
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    this.throwIfShutdown()

    this.addConvexCalls(this.shards.length)
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.withTimingStats('updateByStatusAndOCFPStatusAndACCZeroAndReturn', () =>
          this.convexClient.action(this.publicApi.updateByStatusAndOCFPStatusAndACCZeroAndReturn, {
            authSecret: this.authSecret,
            args: {
              shard,
              status,
              ocfpStatus: onChildrenFinishedProcessingStatus,
              update: taskExecutionStorageUpdateToDBUpdateRequest(update),
              limit: Math.ceil(limit / this.shards.length),
            },
          }),
        ),
      ),
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async updateByCloseStatusAndReturn(
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    this.throwIfShutdown()

    this.addConvexCalls(this.shards.length)
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.withTimingStats('updateByCloseStatusAndReturn', () =>
          this.convexClient.action(this.publicApi.updateByCloseStatusAndReturn, {
            authSecret: this.authSecret,
            args: {
              shard,
              closeStatus,
              update: taskExecutionStorageUpdateToDBUpdateRequest(update),
              limit: Math.ceil(limit / this.shards.length),
            },
          }),
        ),
      ),
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async updateByIsSleepingTaskAndExpiresAtLessThan(
    isSleepingTask: boolean,
    expiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    this.throwIfShutdown()

    this.addConvexCalls()
    return await this.withTimingStats('updateByIsSleepingTaskAndExpiresAtLessThan', () =>
      this.convexClient.action(this.publicApi.updateByIsSleepingTaskAndExpiresAtLessThan, {
        authSecret: this.authSecret,
        args: {
          isSleepingTask,
          expiresAtLessThan,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
          limit: Math.ceil(limit / this.shards.length),
        },
      }),
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan(
    onChildrenFinishedProcessingExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    this.throwIfShutdown()

    this.addConvexCalls()
    return await this.withTimingStats('updateByOCFPExpiresAt', () =>
      this.convexClient.action(this.publicApi.updateByOCFPExpiresAt, {
        authSecret: this.authSecret,
        args: {
          ocfpExpiresAtLessThan: onChildrenFinishedProcessingExpiresAtLessThan,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
          limit,
        },
      }),
    )
  }

  async updateByCloseExpiresAtLessThan(
    closeExpiresAtLessThan: number,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    this.throwIfShutdown()

    this.addConvexCalls()
    return await this.withTimingStats('updateByCloseExpiresAt', () =>
      this.convexClient.action(this.publicApi.updateByCloseExpiresAt, {
        authSecret: this.authSecret,
        args: {
          closeExpiresAtLessThan,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
          limit,
        },
      }),
    )
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn(
    executorId: string,
    needsPromiseCancellation: boolean,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<Array<TaskExecutionStorageValue>> {
    this.throwIfShutdown()

    this.addConvexCalls(this.shards.length)
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.withTimingStats('updateByExecutorIdAndNPCAndReturn', () =>
          this.convexClient.action(this.publicApi.updateByExecutorIdAndNPCAndReturn, {
            authSecret: this.authSecret,
            args: {
              shard,
              executorId,
              npc: needsPromiseCancellation,
              update: taskExecutionStorageUpdateToDBUpdateRequest(update),
              limit,
            },
          }),
        ),
      ),
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async getByParentExecutionId(
    parentExecutionId: string,
  ): Promise<Array<TaskExecutionStorageValue>> {
    this.throwIfShutdown()

    this.addConvexCalls()
    const dbValues = await this.withTimingStats('getByParentExecutionId', () =>
      this.convexClient.query(this.publicApi.getByParentExecutionId, {
        authSecret: this.authSecret,
        args: {
          parentExecutionId,
        },
      }),
    )

    return dbValues.map((dbValue) => taskExecutionDBValueToStorageValue(dbValue))
  }

  async updateByParentExecutionIdAndIsFinished(
    parentExecutionId: string,
    isFinished: boolean,
    update: TaskExecutionStorageUpdate,
  ): Promise<void> {
    this.throwIfShutdown()

    this.addConvexCalls()
    await this.withTimingStats('updateByParentExecutionIdAndIsFinished', () =>
      this.convexClient.mutation(this.publicApi.updateByParentExecutionIdAndIsFinished, {
        authSecret: this.authSecret,
        args: {
          parentExecutionId,
          isFinished,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
        },
      }),
    )
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus(
    isFinished: boolean,
    closeStatus: TaskExecutionCloseStatus,
    update: TaskExecutionStorageUpdate,
    limit: number,
  ): Promise<number> {
    this.throwIfShutdown()

    this.addConvexCalls(this.shards.length)
    const updatedCounts = await Promise.all(
      this.shards.map((shard) =>
        this.withTimingStats('updateAndDecrementParentACCByIsFinishedAndCloseStatus', () =>
          this.convexClient.action(
            this.publicApi.updateAndDecrementParentACCByIsFinishedAndCloseStatus,
            {
              authSecret: this.authSecret,
              args: {
                shard,
                isFinished,
                closeStatus,
                update: taskExecutionStorageUpdateToDBUpdateRequest(update),
                limit: Math.ceil(limit / this.shards.length),
              },
            },
          ),
        ),
      ),
    )
    return updatedCounts.reduce((a, b) => a + b, 0)
  }

  async deleteById(executionId: string): Promise<void> {
    this.throwIfShutdown()

    if (!this.enableTestMode) {
      return
    }

    await this.withTimingStats('deleteById', () =>
      this.convexClient.mutation(this.publicApi.deleteById, {
        authSecret: this.authSecret,
        args: {
          executionId,
        },
      }),
    )
  }

  async deleteAll(): Promise<void> {
    this.throwIfShutdown()

    if (!this.enableTestMode) {
      return
    }

    await this.withTimingStats('deleteAll', () =>
      this.convexClient.action(this.publicApi.deleteAll, {
        authSecret: this.authSecret,
        args: undefined,
      }),
    )
  }

  /**
   * Get timing stats for monitoring and debugging.
   *
   * @returns The timing stats.
   */
  getTimingStats(): Record<
    string,
    {
      count: number
      meanMs: number
    }
  > {
    return Object.fromEntries(
      [...this.timingStats.entries()].map(([key, value]) => [
        key,
        { count: value.count, meanMs: value.meanMs },
      ]),
    )
  }

  /**
   * Returns the per second convex calls stats.
   *
   * @returns The per second convex calls stats.
   */
  getPerSecondConvexCallsStats(): {
    total: number
    mean: number
    median: number
    min: number
    max: number
  } {
    const values = [...this.perSecondConvexCallStats.values()].sort()
    if (values.length === 0) {
      return {
        total: 0,
        mean: 0,
        median: 0,
        min: 0,
        max: 0,
      }
    }

    const totalCalls = values.reduce((a, b) => a + b, 0)
    return {
      total: totalCalls,
      mean: totalCalls / values.length,
      median: values[Math.floor(values.length / 2)]!,
      min: Math.min(...values),
      max: Math.max(...values),
    }
  }
}

function taskExecutionStorageValueToDBInsertValueWithShard(
  value: TaskExecutionStorageValue,
  totalShards: number,
): TaskExecutionDBInsertValue {
  const shard = Math.floor(Math.random() * totalShards)
  return taskExecutionStorageValueToDBInsertValue(value, shard)
}
