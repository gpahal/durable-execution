import { Chunk, Deferred, Effect, Ref } from 'effect'

import { makeBackgroundProcessor } from './background-processor'
import { convertErrorToDurableExecutionError, DurableExecutionError } from './errors'

export type BatchRequest<TInput, TOutput> = {
  input: TInput
  weight: number
  deferred: Deferred.Deferred<TOutput, DurableExecutionError>
}

export type BackgroundBatchProcessorOptions<TInput, TOutput> = {
  readonly processName: string
  readonly intraBatchSleepMs: number
  readonly batchWeight: number
  readonly processBatchRequests: (
    requests: ReadonlyArray<BatchRequest<TInput, TOutput>>,
  ) => undefined extends TOutput
    ? Effect.Effect<ReadonlyArray<TOutput> | undefined | null | void, unknown>
    : Effect.Effect<ReadonlyArray<TOutput>, unknown>
}

type BackgroundBatchProcessorState<TInput, TOutput> = {
  readonly pendingRequestsBatches: Chunk.Chunk<{
    requests: Chunk.Chunk<BatchRequest<TInput, TOutput>>
    weight: number
  }>
  readonly skippedBatchCount: number
}

const processBatchRequestsIteration = Effect.fn(function* <TInput, TOutput>(
  options: BackgroundBatchProcessorOptions<TInput, TOutput>,
  stateRef: Ref.Ref<BackgroundBatchProcessorState<TInput, TOutput>>,
) {
  const pendingRequestBatches = yield* Ref.modify(stateRef, (state) => {
    const shouldSkipBatch =
      state.pendingRequestsBatches.length === 0 ||
      (state.pendingRequestsBatches.length === 1 &&
        state.skippedBatchCount < 5 &&
        Chunk.unsafeHead(state.pendingRequestsBatches).weight < options.batchWeight / 3)
    return [
      shouldSkipBatch
        ? []
        : Chunk.toArray(state.pendingRequestsBatches).map((batch) => Chunk.toArray(batch.requests)),
      shouldSkipBatch
        ? {
            pendingRequestsBatches: state.pendingRequestsBatches,
            skippedBatchCount: state.skippedBatchCount + 1,
          }
        : {
            pendingRequestsBatches: Chunk.empty(),
            skippedBatchCount: 0,
          },
    ]
  })
  if (pendingRequestBatches.length === 0) {
    return
  }

  return yield* Effect.forEach(
    pendingRequestBatches,
    (batch: Array<BatchRequest<TInput, TOutput>>) =>
      Effect.gen(function* () {
        const result = yield* options
          .processBatchRequests(batch)
          .pipe(
            Effect.mapError((error) =>
              convertErrorToDurableExecutionError(
                error,
                `Error in background batch processor ${options.processName}`,
              ),
            ),
          )
        if (result == null) {
          return yield* Effect.forEach(
            batch,
            (r) => Deferred.succeed(r.deferred, undefined as TOutput),
            {
              concurrency: 'unbounded',
            },
          )
        } else if (Array.isArray(result)) {
          if (result.length !== batch.length) {
            const error = DurableExecutionError.nonRetryable(
              `Error in background batch processor ${options.processName}: processBatchRequests returned ${result.length} results but expected ${batch.length}`,
            )
            yield* Effect.forEach(batch, (r) => Deferred.fail(r.deferred, error), {
              concurrency: 'unbounded',
            })
            return yield* Effect.fail(error)
          } else {
            return yield* Effect.forEach(
              batch,
              (r, i) => Deferred.succeed(r.deferred, result[i]! as TOutput),
              {
                concurrency: 'unbounded',
              },
            )
          }
        } else {
          const error = DurableExecutionError.nonRetryable(
            `Error in background batch processor ${options.processName}: invalid return type ${typeof result}`,
          )
          yield* Effect.forEach(batch, (r) => Deferred.fail(r.deferred, error), {
            concurrency: 'unbounded',
          })
          return yield* Effect.fail(error)
        }
      }).pipe(
        Effect.tapError((error) =>
          Effect.gen(function* () {
            yield* Effect.forEach(batch, (r) => Deferred.fail(r.deferred, error), {
              concurrency: 'unbounded',
            })
            yield* Effect.logError(error.message).pipe(Effect.annotateLogs('error', error))
          }),
        ),
        Effect.catchAll(() => Effect.void),
      ),
    {
      concurrency: 10,
      discard: true,
    },
  )
})

export const makeBackgroundBatchProcessor = Effect.fn(
  <TInput, TOutput>(options: BackgroundBatchProcessorOptions<TInput, TOutput>) =>
    Effect.acquireRelease(
      Effect.gen(function* () {
        const stateRef = yield* Ref.make<BackgroundBatchProcessorState<TInput, TOutput>>({
          pendingRequestsBatches: Chunk.empty(),
          skippedBatchCount: 0,
        })

        const { wakeup, shutdown } = yield* makeBackgroundProcessor({
          processName: options.processName,
          intraIterationSleepMs: options.intraBatchSleepMs,
          iterationEffect: processBatchRequestsIteration(options, stateRef).pipe(
            Effect.map(() => ({ hasMore: false })),
          ),
        })

        const addBatchRequest = Effect.fn(function* (request: BatchRequest<TInput, TOutput>) {
          const shouldWakeup = yield* Ref.modify(stateRef, (state) => {
            if (state.pendingRequestsBatches.length === 0) {
              return [
                request.weight >= 0.75 * options.batchWeight,
                {
                  ...state,
                  pendingRequestsBatches: Chunk.of({
                    requests: Chunk.of(request),
                    weight: request.weight,
                  }),
                },
              ]
            } else {
              const lastBatch = Chunk.unsafeLast(state.pendingRequestsBatches)
              return [
                state.pendingRequestsBatches.length > 1 ||
                  lastBatch.weight + request.weight >= 0.75 * options.batchWeight,
                {
                  ...state,
                  pendingRequestsBatches:
                    lastBatch.weight + request.weight > options.batchWeight
                      ? Chunk.append(state.pendingRequestsBatches, {
                          requests: Chunk.of(request),
                          weight: request.weight,
                        })
                      : Chunk.append(Chunk.dropRight(state.pendingRequestsBatches, 1), {
                          requests: Chunk.append(lastBatch.requests, request),
                          weight: lastBatch.weight + request.weight,
                        }),
                },
              ]
            }
          })

          if (shouldWakeup) {
            yield* wakeup
          }
        })

        return { addBatchRequest, wakeup, shutdown }
      }),
      (backgroundBatchProcessor) => backgroundBatchProcessor.shutdown,
    ),
)

export type BackgroundBatchProcessor = Effect.Effect.Success<
  ReturnType<typeof makeBackgroundBatchProcessor>
>
