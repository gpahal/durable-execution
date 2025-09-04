import {
  actionGeneric,
  mutationGeneric,
  queryGeneric,
  type ApiFromModules,
  type Expand,
  type FunctionReference,
  type GenericActionCtx,
} from 'convex/server'
import type { GenericId } from 'convex/values'
import {
  DurableExecutionError,
  type TaskExecutionCloseStatus,
  type TaskExecutionOnChildrenFinishedProcessingStatus,
  type TaskExecutionsStorage,
  type TaskExecutionStatus,
  type TaskExecutionStorageGetByIdFilters,
  type TaskExecutionStorageUpdate,
  type TaskExecutionStorageValue,
} from 'durable-execution'
import { Either, Schema } from 'effect'

import {
  taskExecutionDBValueToStorageValue,
  taskExecutionStorageUpdateToDBUpdateRequest,
  taskExecutionStorageValueToDBInsertValue,
  type TaskExecutionDBUpdateRequest,
  type TaskExecutionDBValue,
} from '../common'
import type { Mounts } from '../component/_generated/api'
import type { Id } from '../component/_generated/dataModel'
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
 *   getManyById,
 *   getManyBySleepingTaskUniqueId,
 *   updateManyById,
 *   updateManyByIdAndInsertChildrenIfUpdated,
 *   updateByStatusAndStartAtLessThanAndReturn,
 *   updateByStatusAndOCFPStatusAndACCZeroAndReturn,
 *   updateByCloseStatusAndReturn,
 *   updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
 *   updateByOCFPExpiresAt,
 *   updateByCloseExpiresAt,
 *   updateByExecutorIdAndNPCAndReturn,
 *   getManyByParentExecutionId,
 *   updateManyByParentExecutionIdAndIsFinished,
 *   updateAndDecrementParentACCByIsFinishedAndCloseStatus,
 *   deleteById,
 *   deleteAll
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

  const actionWithLock = async <T>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ctx: GenericActionCtx<any>,
    key: string,
    fn: () => Promise<T>,
    defaultValue: T,
  ): Promise<T> => {
    const lockId = (await ctx.runMutation(component.lib.acquireLock, { key })) as Id<'locks'>
    if (!lockId) {
      return defaultValue
    }

    try {
      return await fn()
    } finally {
      await ctx.runMutation(component.lib.releaseLock, { id: lockId })
    }
  }

  return {
    insertMany: mutationGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { executions: ReadonlyArray<TaskExecutionDBInsertValue> }
        },
      ) => {
        await ctx.runMutation(
          component.lib.insertMany,
          verifyArgs(args) as {
            executions: Array<TaskExecutionDBInsertValue>
          },
        )
      },
    ),
    getManyById: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: ReadonlyArray<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
            }>
          }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getManyById,
          verifyArgs(args) as {
            requests: Array<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
            }>
          },
        )) as Array<TaskExecutionDBValue | null>
      },
    ),
    getManyBySleepingTaskUniqueId: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { requests: ReadonlyArray<{ sleepingTaskUniqueId: string }> }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getManyBySleepingTaskUniqueId,
          verifyArgs(args) as {
            requests: Array<{ sleepingTaskUniqueId: string }>
          },
        )) as Array<TaskExecutionDBValue | null>
      },
    ),
    updateManyById: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: ReadonlyArray<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionDBUpdateRequest
            }>
          }
        },
      ) => {
        await ctx.runMutation(
          component.lib.updateManyById,
          verifyArgs(args) as {
            requests: Array<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionDBUpdateRequest
            }>
          },
        )
      },
    ),
    updateManyByIdAndInsertChildrenIfUpdated: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: ReadonlyArray<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionDBUpdateRequest
              childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionDBInsertValue>
            }>
          }
        },
      ) => {
        await ctx.runMutation(
          component.lib.updateManyByIdAndInsertChildrenIfUpdated,
          verifyArgs(args) as {
            requests: Array<{
              executionId: string
              filters?: TaskExecutionStorageGetByIdFilters
              update: TaskExecutionDBUpdateRequest
              childrenTaskExecutionsToInsertIfAnyUpdated: Array<TaskExecutionDBInsertValue>
            }>
          },
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
            update: TaskExecutionDBUpdateRequest
            updateExpiresAtWithStartedAt: number
            limit: number
          }
        },
      ) => {
        return await actionWithLock<Array<TaskExecutionDBValue>>(
          ctx,
          `updateByStatusAndStartAtLessThanAndReturn_${args.args.shard}_${args.args.status}`,
          () => {
            return ctx.runMutation(
              component.lib.updateByStatusAndStartAtLessThanAndReturn,
              verifyArgs(args),
            )
          },
          [],
        )
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
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<Array<TaskExecutionDBValue>>(
          ctx,
          `updateByStatusAndOCFPStatusAndACCZeroAndReturn_${args.args.shard}_${args.args.status}_${args.args.ocfpStatus}`,
          () => {
            return ctx.runMutation(
              component.lib.updateByStatusAndOCFPStatusAndACCZeroAndReturn,
              verifyArgs(args),
            )
          },
          [],
        )
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
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<Array<TaskExecutionDBValue>>(
          ctx,
          `updateByCloseStatusAndReturn_${args.args.shard}_${args.args.closeStatus}`,
          () => {
            return ctx.runMutation(component.lib.updateByCloseStatusAndReturn, verifyArgs(args))
          },
          [],
        )
      },
    ),
    updateByStatusAndIsSleepingTaskAndExpiresAtLessThan: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            status: TaskExecutionStatus
            isSleepingTask: boolean
            expiresAtLessThan: number
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<number>(
          ctx,
          `updateByStatusAndIsSleepingTaskAndExpiresAtLessThan_${args.args.status}_${args.args.isSleepingTask}`,
          () => {
            return ctx.runMutation(
              component.lib.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
              verifyArgs(args),
            )
          },
          0,
        )
      },
    ),
    updateByOCFPExpiresAt: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            ocfpExpiresAtLessThan: number
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<number>(
          ctx,
          `updateByOCFPExpiresAt`,
          () => {
            return ctx.runMutation(component.lib.updateByOCFPExpiresAt, verifyArgs(args))
          },
          0,
        )
      },
    ),
    updateByCloseExpiresAt: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            closeExpiresAtLessThan: number
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<number>(
          ctx,
          `updateByCloseExpiresAt`,
          () => {
            return ctx.runMutation(component.lib.updateByCloseExpiresAt, verifyArgs(args))
          },
          0,
        )
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
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<Array<TaskExecutionDBValue>>(
          ctx,
          `updateByExecutorIdAndNPCAndReturn_${args.args.shard}_${args.args.executorId}_${args.args.npc}`,
          () => {
            return ctx.runMutation(
              component.lib.updateByExecutorIdAndNPCAndReturn,
              verifyArgs(args),
            )
          },
          [],
        )
      },
    ),
    getManyByParentExecutionId: queryGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: { requests: ReadonlyArray<{ parentExecutionId: string }> }
        },
      ) => {
        return (await ctx.runQuery(
          component.lib.getManyByParentExecutionId,
          verifyArgs(args) as {
            requests: Array<{ parentExecutionId: string }>
          },
        )) as Array<Array<TaskExecutionDBValue>>
      },
    ),
    updateManyByParentExecutionIdAndIsFinished: actionGeneric(
      async (
        ctx,
        args: {
          authSecret: string
          args: {
            requests: ReadonlyArray<{
              parentExecutionId: string
              isFinished: boolean
              update: TaskExecutionDBUpdateRequest
            }>
          }
        },
      ) => {
        await ctx.runMutation(
          component.lib.updateManyByParentExecutionIdAndIsFinished,
          verifyArgs(args) as {
            requests: Array<{
              parentExecutionId: string
              isFinished: boolean
              update: TaskExecutionDBUpdateRequest
            }>
          },
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
            update: TaskExecutionDBUpdateRequest
            limit: number
          }
        },
      ) => {
        return await actionWithLock<number>(
          ctx,
          `updateAndDecrementParentACCByIsFinishedAndCloseStatus_${args.args.shard}_${args.args.isFinished}_${args.args.closeStatus}`,
          () => {
            return ctx.runMutation(
              component.lib.updateAndDecrementParentACCByIsFinishedAndCloseStatus,
              verifyArgs(args),
            )
          },
          0,
        )
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
export const ConvexTaskExecutionsStorageOptionsSchema = Schema.Struct({
  totalShards: Schema.Int.pipe(
    Schema.between(1, 64),
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => 1),
  ),
  shards: Schema.Array(Schema.Int.pipe(Schema.greaterThanOrEqualTo(0))).pipe(
    Schema.optionalWith({ nullable: true }),
  ),
  enableTestMode: Schema.Boolean.pipe(
    Schema.optionalWith({ nullable: true }),
    Schema.withDecodingDefault(() => false),
  ),
}).pipe(
  Schema.filter((val) => {
    if (val.shards?.some((shard) => shard < 0 || shard >= val.totalShards)) {
      return `Shards must be an array of numbers between 0 and totalShards (${val.totalShards})`
    }
    return true
  }),
  Schema.transform(
    Schema.Struct({
      totalShards: Schema.Int,
      shards: Schema.Array(Schema.Int),
      enableTestMode: Schema.Boolean,
    }),
    {
      strict: true,
      encode: (val) => ({
        ...val,
      }),
      decode: (val) => {
        let finalShards: Array<number>
        if (!val.shards) {
          finalShards = Array.from({ length: val.totalShards }, (_, i) => i)
        } else {
          const shardsSet = new Set(val.shards)
          finalShards = [...shardsSet].sort((a, b) => a - b)
        }
        return {
          ...val,
          shards: finalShards,
        }
      },
    },
  ),
)

/**
 * A task executions storage implementation that uses a Convex database.
 */
export class ConvexTaskExecutionsStorage implements TaskExecutionsStorage {
  private readonly convexClient: AnyConvexClient
  private readonly authSecret: string
  private readonly publicApi: DurableExecutionStoragePublicApi
  private readonly totalShards: number
  private readonly shards: ReadonlyArray<number>
  private readonly enableTestMode: boolean

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
   */
  constructor(
    convexClient: AnyConvexClient,
    authSecret: string,
    publicApi: DurableExecutionStoragePublicApi,
    options: {
      totalShards?: number
      shards?: Array<number>
      enableTestMode?: boolean
    } = {},
  ) {
    const parsedOptions = Schema.decodeEither(ConvexTaskExecutionsStorageOptionsSchema)(options)
    if (Either.isLeft(parsedOptions)) {
      throw DurableExecutionError.nonRetryable(`Invalid options: ${parsedOptions.left.message}`)
    }

    this.convexClient = convexClient
    this.authSecret = authSecret
    this.publicApi = publicApi
    this.totalShards = parsedOptions.right.totalShards
    this.shards = parsedOptions.right.shards
    this.enableTestMode = parsedOptions.right.enableTestMode
  }

  async insertMany(executions: ReadonlyArray<TaskExecutionStorageValue>): Promise<void> {
    if (executions.length === 0) {
      return
    }

    await this.convexClient.mutation(this.publicApi.insertMany, {
      authSecret: this.authSecret,
      args: {
        executions: executions.map((value) =>
          taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
        ),
      },
    })
  }

  async getManyById(
    requests: ReadonlyArray<{ executionId: string; filters?: TaskExecutionStorageGetByIdFilters }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    const dbValues = await this.convexClient.query(this.publicApi.getManyById, {
      authSecret: this.authSecret,
      args: {
        requests,
      },
    })
    return dbValues.map((dbValue) =>
      dbValue ? taskExecutionDBValueToStorageValue(dbValue) : undefined,
    )
  }

  async getManyBySleepingTaskUniqueId(
    requests: ReadonlyArray<{ sleepingTaskUniqueId: string }>,
  ): Promise<Array<TaskExecutionStorageValue | undefined>> {
    const dbValues = await this.convexClient.query(this.publicApi.getManyBySleepingTaskUniqueId, {
      authSecret: this.authSecret,
      args: {
        requests,
      },
    })
    return dbValues.map((dbValue) =>
      dbValue ? taskExecutionDBValueToStorageValue(dbValue) : undefined,
    )
  }

  async updateManyById(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.convexClient.action(this.publicApi.updateManyById, {
      authSecret: this.authSecret,
      args: {
        requests: requests.map((request) => ({
          ...request,
          update: taskExecutionStorageUpdateToDBUpdateRequest(request.update),
        })),
      },
    })
  }

  async updateManyByIdAndInsertChildrenIfUpdated(
    requests: ReadonlyArray<{
      executionId: string
      filters?: TaskExecutionStorageGetByIdFilters
      update: TaskExecutionStorageUpdate
      childrenTaskExecutionsToInsertIfAnyUpdated: ReadonlyArray<TaskExecutionStorageValue>
    }>,
  ): Promise<void> {
    await this.convexClient.action(this.publicApi.updateManyByIdAndInsertChildrenIfUpdated, {
      authSecret: this.authSecret,
      args: {
        requests: requests.map((request) => ({
          ...request,
          update: taskExecutionStorageUpdateToDBUpdateRequest(request.update),
          childrenTaskExecutionsToInsertIfAnyUpdated:
            request.childrenTaskExecutionsToInsertIfAnyUpdated.map((value) =>
              taskExecutionStorageValueToDBInsertValueWithShard(value, this.totalShards),
            ),
        })),
      },
    })
  }

  async updateByStatusAndStartAtLessThanAndReturn({
    status,
    startAtLessThan,
    update,
    updateExpiresAtWithStartedAt,
    limit,
  }: {
    status: TaskExecutionStatus
    startAtLessThan: number
    update: TaskExecutionStorageUpdate
    updateExpiresAtWithStartedAt: number
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.convexClient.action(this.publicApi.updateByStatusAndStartAtLessThanAndReturn, {
          authSecret: this.authSecret,
          args: {
            shard,
            status,
            startAtLessThan,
            update: taskExecutionStorageUpdateToDBUpdateRequest(update),
            updateExpiresAtWithStartedAt,
            limit,
          },
        }),
      ),
    )

    return dbValuesArr
      .flat()
      .map((dbValue) =>
        taskExecutionDBValueToStorageValue(dbValue, update, updateExpiresAtWithStartedAt),
      )
  }

  async updateByStatusAndOnChildrenFinishedProcessingStatusAndActiveChildrenCountZeroAndReturn({
    status,
    onChildrenFinishedProcessingStatus,
    update,
    limit,
  }: {
    status: TaskExecutionStatus
    onChildrenFinishedProcessingStatus: TaskExecutionOnChildrenFinishedProcessingStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.convexClient.action(this.publicApi.updateByStatusAndOCFPStatusAndACCZeroAndReturn, {
          authSecret: this.authSecret,
          args: {
            shard,
            status,
            ocfpStatus: onChildrenFinishedProcessingStatus,
            update: taskExecutionStorageUpdateToDBUpdateRequest(update),
            limit,
          },
        }),
      ),
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async updateByCloseStatusAndReturn({
    closeStatus,
    update,
    limit,
  }: {
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
        this.convexClient.action(this.publicApi.updateByCloseStatusAndReturn, {
          authSecret: this.authSecret,
          args: {
            shard,
            closeStatus,
            update: taskExecutionStorageUpdateToDBUpdateRequest(update),
            limit,
          },
        }),
      ),
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async updateByStatusAndIsSleepingTaskAndExpiresAtLessThan({
    status,
    isSleepingTask,
    expiresAtLessThan,
    update,
    limit,
  }: {
    status: TaskExecutionStatus
    isSleepingTask: boolean
    expiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.convexClient.action(
      this.publicApi.updateByStatusAndIsSleepingTaskAndExpiresAtLessThan,
      {
        authSecret: this.authSecret,
        args: {
          status,
          isSleepingTask,
          expiresAtLessThan,
          update: taskExecutionStorageUpdateToDBUpdateRequest(update),
          limit,
        },
      },
    )
  }

  async updateByOnChildrenFinishedProcessingExpiresAtLessThan({
    onChildrenFinishedProcessingExpiresAtLessThan,
    update,
    limit,
  }: {
    onChildrenFinishedProcessingExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.convexClient.action(this.publicApi.updateByOCFPExpiresAt, {
      authSecret: this.authSecret,
      args: {
        ocfpExpiresAtLessThan: onChildrenFinishedProcessingExpiresAtLessThan,
        update: taskExecutionStorageUpdateToDBUpdateRequest(update),
        limit,
      },
    })
  }

  async updateByCloseExpiresAtLessThan({
    closeExpiresAtLessThan,
    update,
    limit,
  }: {
    closeExpiresAtLessThan: number
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    return await this.convexClient.action(this.publicApi.updateByCloseExpiresAt, {
      authSecret: this.authSecret,
      args: {
        closeExpiresAtLessThan,
        update: taskExecutionStorageUpdateToDBUpdateRequest(update),
        limit,
      },
    })
  }

  async updateByExecutorIdAndNeedsPromiseCancellationAndReturn({
    executorId,
    needsPromiseCancellation,
    update,
    limit,
  }: {
    executorId: string
    needsPromiseCancellation: boolean
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<Array<TaskExecutionStorageValue>> {
    const dbValuesArr = await Promise.all(
      this.shards.map((shard) =>
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
    )

    return dbValuesArr.flat().map((dbValue) => taskExecutionDBValueToStorageValue(dbValue, update))
  }

  async getManyByParentExecutionId(
    requests: ReadonlyArray<{ parentExecutionId: string }>,
  ): Promise<Array<Array<TaskExecutionStorageValue>>> {
    const dbValuesArr = await this.convexClient.query(this.publicApi.getManyByParentExecutionId, {
      authSecret: this.authSecret,
      args: {
        requests,
      },
    })

    return dbValuesArr.map((dbValues) =>
      dbValues.map((dbValue) => taskExecutionDBValueToStorageValue(dbValue)),
    )
  }

  async updateManyByParentExecutionIdAndIsFinished(
    requests: ReadonlyArray<{
      parentExecutionId: string
      isFinished: boolean
      update: TaskExecutionStorageUpdate
    }>,
  ): Promise<void> {
    await this.convexClient.action(this.publicApi.updateManyByParentExecutionIdAndIsFinished, {
      authSecret: this.authSecret,
      args: {
        requests: requests.map((request) => ({
          ...request,
          update: taskExecutionStorageUpdateToDBUpdateRequest(request.update),
        })),
      },
    })
  }

  async updateAndDecrementParentActiveChildrenCountByIsFinishedAndCloseStatus({
    isFinished,
    closeStatus,
    update,
    limit,
  }: {
    isFinished: boolean
    closeStatus: TaskExecutionCloseStatus
    update: TaskExecutionStorageUpdate
    limit: number
  }): Promise<number> {
    const updatedCounts = await Promise.all(
      this.shards.map((shard) =>
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
    )
    return updatedCounts.reduce((a, b) => a + b, 0)
  }

  async deleteById({ executionId }: { executionId: string }): Promise<void> {
    if (!this.enableTestMode) {
      return
    }

    await this.convexClient.mutation(this.publicApi.deleteById, {
      authSecret: this.authSecret,
      args: {
        executionId,
      },
    })
  }

  async deleteAll(): Promise<void> {
    if (!this.enableTestMode) {
      return
    }

    await this.convexClient.action(this.publicApi.deleteAll, {
      authSecret: this.authSecret,
      args: undefined,
    })
  }
}

function taskExecutionStorageValueToDBInsertValueWithShard(
  value: TaskExecutionStorageValue,
  totalShards: number,
): TaskExecutionDBInsertValue {
  const shard = Math.floor(Math.random() * totalShards)
  return taskExecutionStorageValueToDBInsertValue(value, shard)
}
