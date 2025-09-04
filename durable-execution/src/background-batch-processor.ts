import { Deferred, Effect, Either, Ref } from 'effect'

import { getErrorMessage } from '@gpahal/std/errors'

import { makeBackgroundProcessor } from './background-processor'
import { convertErrorToDurableExecutionError, DurableExecutionError } from './errors'
import { makeFiberPool, type FiberPool } from './fiber-pool'

export type BatchRequest<TInput, TOutput> = {
  input: TInput
  weight: number
  deferred: Deferred.Deferred<TOutput, DurableExecutionError>
}

export type BackgroundBatchProcessorOptions<TInput, TOutput> = {
  readonly executorId: string
  readonly processName: string
  readonly enableBatching: boolean
  readonly intraBatchSleepMs: number
  readonly batchWeight: number
  readonly processBatchRequests: (
    requests: ReadonlyArray<TInput>,
  ) => undefined extends TOutput
    ? Effect.Effect<ReadonlyArray<TOutput> | undefined | null | void, unknown>
    : null extends TOutput
      ? Effect.Effect<ReadonlyArray<TOutput> | undefined | null | void, unknown>
      : void extends TOutput
        ? Effect.Effect<ReadonlyArray<TOutput> | undefined | null | void, unknown>
        : Effect.Effect<ReadonlyArray<TOutput>, unknown>
  readonly skipInitialSleep?: boolean
  readonly shutdownTimeoutMs?: number
}

type BackgroundBatchProcessorState<TInput, TOutput> = {
  readonly pendingRequestsBatches: Array<{
    requests: Array<BatchRequest<TInput, TOutput>>
    weight: number
  }>
  readonly skippedBatchCount: number
}

const processBatchRequestsIteration = Effect.fn(function* <TInput, TOutput>(
  options: BackgroundBatchProcessorOptions<TInput, TOutput>,
  stateRef: Ref.Ref<BackgroundBatchProcessorState<TInput, TOutput>>,
  fiberPool: FiberPool,
) {
  const pendingRequestBatches = yield* Ref.modify(stateRef, (state) => {
    const shouldSkipBatch =
      state.pendingRequestsBatches.length === 0 ||
      (state.pendingRequestsBatches.length === 1 &&
        state.skippedBatchCount < 3 &&
        state.pendingRequestsBatches[0]!.weight < options.batchWeight / 3)
    return [
      shouldSkipBatch ? [] : state.pendingRequestsBatches.map((batch) => batch.requests),
      shouldSkipBatch
        ? {
            pendingRequestsBatches: state.pendingRequestsBatches,
            skippedBatchCount: state.skippedBatchCount + 1,
          }
        : {
            pendingRequestsBatches: [],
            skippedBatchCount: 0,
          },
    ]
  })
  if (pendingRequestBatches.length === 0) {
    return
  }

  yield* fiberPool.fork(
    Effect.forEach(
      pendingRequestBatches,
      (batch: Array<BatchRequest<TInput, TOutput>>) =>
        Effect.gen(function* () {
          const resultEither = yield* options
            .processBatchRequests(batch.map((r) => r.input))
            .pipe(Effect.mapError(convertErrorToDurableExecutionError), Effect.either)
          if (Either.isLeft(resultEither)) {
            return yield* Effect.fail(resultEither.left)
          }

          const result = resultEither.right
          if (result == null) {
            return yield* Effect.forEach(
              batch,
              (r) => Deferred.succeed(r.deferred, undefined as TOutput),
              {
                concurrency: 'unbounded',
                discard: true,
              },
            )
          } else if (Array.isArray(result)) {
            return yield* result.length !== batch.length
              ? Effect.fail(
                  DurableExecutionError.nonRetryable(
                    `Invalid number of results returned from processBatchRequests [returned=${result.length}] [expected=${batch.length}]`,
                  ),
                )
              : Effect.forEach(
                  batch,
                  (r, i) => Deferred.succeed(r.deferred, result[i]! as TOutput),
                  {
                    concurrency: 'unbounded',
                    discard: true,
                  },
                )
          } else {
            return yield* Effect.fail(
              DurableExecutionError.nonRetryable(
                `Invalid return type returned from processBatchRequests [returnType=${typeof result}]`,
              ),
            )
          }
        }).pipe(
          Effect.tapError((error) =>
            Effect.gen(function* () {
              const deferredError = DurableExecutionError.nonRetryable(
                `Error in background batch processor [processName=${options.processName}]: ${getErrorMessage(error)}`,
              )
              yield* Effect.forEach(batch, (r) => Deferred.fail(r.deferred, deferredError), {
                concurrency: 'unbounded',
                discard: true,
              })
              yield* Effect.logError('Error in background batch processor').pipe(
                Effect.annotateLogs('service', 'background-batch-processor'),
                Effect.annotateLogs('executorId', options.executorId),
                Effect.annotateLogs('processName', options.processName),
                Effect.annotateLogs('error', error),
              )
            }),
          ),
          Effect.catchAll(() => Effect.void),
        ),
      {
        concurrency: 25,
        discard: true,
      },
    ),
  )
})

const processRequestWithoutBatching = Effect.fn(
  <TInput, TOutput>({
    options,
    input,
  }: {
    options: BackgroundBatchProcessorOptions<TInput, TOutput>
    input: TInput
  }) =>
    Effect.gen(function* () {
      const resultEither = yield* options
        .processBatchRequests([input])
        .pipe(Effect.mapError(convertErrorToDurableExecutionError), Effect.either)
      if (Either.isLeft(resultEither)) {
        return yield* Effect.fail(resultEither.left)
      }

      const result = resultEither.right
      if (result == null) {
        return undefined as TOutput
      } else if (Array.isArray(result)) {
        if (result.length !== 1) {
          return yield* Effect.fail(
            DurableExecutionError.nonRetryable(
              `Invalid number of results returned from processBatchRequests [returned=${result.length}] [expected=1]`,
            ),
          )
        }
        return result[0] as TOutput
      } else {
        return yield* Effect.fail(
          DurableExecutionError.nonRetryable(
            `Invalid return type returned from processBatchRequests [returnType=${typeof result}]`,
          ),
        )
      }
    }),
)

export const makeBackgroundBatchProcessor = Effect.fn(
  <TInput, TOutput>(options: BackgroundBatchProcessorOptions<TInput, TOutput>) =>
    options.enableBatching
      ? Effect.acquireRelease(
          Effect.gen(function* () {
            const stateRef = yield* Ref.make<BackgroundBatchProcessorState<TInput, TOutput>>({
              pendingRequestsBatches: [],
              skippedBatchCount: 0,
            })

            const fiberPool = yield* makeFiberPool({
              executorId: options.executorId,
              processName: 'BackgroundBatchProcessor',
              shutdownTimeoutMs: options.shutdownTimeoutMs,
            })

            const { isRunning, start, shutdown } = yield* makeBackgroundProcessor({
              executorId: options.executorId,
              processName: options.processName,
              intraIterationSleepMs: options.intraBatchSleepMs,
              iterationEffect: processBatchRequestsIteration(options, stateRef, fiberPool).pipe(
                Effect.map(() => ({ hasMore: false })),
              ),
              skipInitialSleep: options.skipInitialSleep,
              shutdownTimeoutMs: options.shutdownTimeoutMs,
            })

            const addBatchRequest = Effect.fn(function* (request: BatchRequest<TInput, TOutput>) {
              yield* Ref.update(stateRef, (state) => {
                if (state.pendingRequestsBatches.length === 0) {
                  return {
                    ...state,
                    pendingRequestsBatches: [
                      {
                        requests: [request],
                        weight: request.weight,
                      },
                    ],
                  }
                } else {
                  let lastBatch = state.pendingRequestsBatches.at(-1)!
                  if (lastBatch.weight + request.weight > options.batchWeight) {
                    state.pendingRequestsBatches.push({
                      requests: [request],
                      weight: request.weight,
                    })
                  } else {
                    lastBatch.requests.push(request)
                    lastBatch.weight += request.weight
                  }
                  lastBatch = state.pendingRequestsBatches.at(-1)!
                  return {
                    ...state,
                    pendingRequestsBatches: state.pendingRequestsBatches,
                  }
                }
              })
            })

            const processRequest = Effect.fn(function* ({
              input,
              weight = 1,
              enableBatching = true,
            }: {
              input: TInput
              weight?: number
              enableBatching?: boolean
            }) {
              if (!enableBatching || !(yield* isRunning)) {
                return yield* processRequestWithoutBatching({ options, input })
              }

              const request = {
                input,
                weight: weight == null ? 1 : weight,
                deferred: yield* Deferred.make<TOutput, DurableExecutionError>(),
              }
              yield* addBatchRequest(request)
              return yield* Deferred.await(request.deferred)
            })

            const shutdownWrapper = Effect.all([shutdown, fiberPool.shutdown])

            return { processRequest, isRunning, start, shutdown: shutdownWrapper }
          }),
          (backgroundBatchProcessor) => backgroundBatchProcessor.shutdown,
        )
      : Effect.sync(() => {
          return {
            processRequest: Effect.fn(function* ({
              input,
            }: {
              input: TInput
              weight?: number
              enableBatching?: boolean
            }) {
              return yield* processRequestWithoutBatching({ options, input })
            }),
            isRunning: Effect.succeed(true),
            start: Effect.void,
            shutdown: Effect.void,
          }
        }),
)

export type BackgroundBatchProcessor<TInput, TOutput> = Effect.Effect.Success<
  ReturnType<typeof makeBackgroundBatchProcessor<TInput, TOutput>>
>
