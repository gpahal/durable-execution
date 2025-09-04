import { Cause, Duration, Effect, Either, Exit, Fiber, Ref, Schedule, Scope } from 'effect'

import { convertErrorToDurableExecutionError, DurableExecutionCancelledError } from './errors'
import { getDurationWithJitter } from './utils'

const BACKGROUND_PROCESSOR_MAX_CONSECUTIVE_ERRORS_COUNT = 3

export type BackgroundProcessorOptions = {
  readonly executorId: string
  readonly processName: string
  readonly intraIterationSleepMs: number
  readonly iterationEffect: Effect.Effect<{ hasMore: boolean }, unknown>
  readonly skipInitialSleep?: boolean
  readonly shutdownTimeoutMs?: number
}

type BackgroundProcessorState = {
  readonly isStarted: boolean
  readonly isShutdown: boolean
}

const runBackgroundProcess = Effect.fn(
  (options: BackgroundProcessorOptions, stateRef: Ref.Ref<BackgroundProcessorState>) =>
    Effect.gen(function* () {
      const localStateRef = yield* Ref.make({
        consecutiveErrorsCount: 0,
        intraIterationSleepMs: options.intraIterationSleepMs,
      })

      if (!options.skipInitialSleep) {
        yield* Effect.sleep(getDurationWithJitter(options.intraIterationSleepMs * 0.1, 0.5))
      }
      yield* Effect.gen(function* () {
        const result = yield* options.iterationEffect.pipe(
          Effect.catchAll((error) =>
            Effect.gen(function* () {
              const durableExecutionError = convertErrorToDurableExecutionError(error)
              if (durableExecutionError instanceof DurableExecutionCancelledError) {
                const state = yield* Ref.get(stateRef)
                if (state.isShutdown) {
                  return { hasMore: false }
                }
              }

              return yield* Effect.fail(durableExecutionError)
            }),
          ),
          Effect.either,
        )

        if (Either.isLeft(result)) {
          yield* Effect.logError('Error in iteration effect').pipe(
            Effect.annotateLogs('service', 'background-processor'),
            Effect.annotateLogs('executorId', options.executorId),
            Effect.annotateLogs('processName', options.processName),
            Effect.annotateLogs('error', result.left),
          )

          const localState = yield* Ref.updateAndGet(localStateRef, (state) => {
            const consecutiveErrorsCount = state.consecutiveErrorsCount + 1
            const intraIterationSleepMs =
              consecutiveErrorsCount >= BACKGROUND_PROCESSOR_MAX_CONSECUTIVE_ERRORS_COUNT
                ? Math.min(state.intraIterationSleepMs * 1.25, options.intraIterationSleepMs * 5)
                : state.intraIterationSleepMs
            return {
              consecutiveErrorsCount,
              intraIterationSleepMs,
            }
          })

          yield* Effect.sleep(getDurationWithJitter(localState.intraIterationSleepMs, 0.1))
          return
        }

        yield* Ref.set(localStateRef, {
          consecutiveErrorsCount: 0,
          intraIterationSleepMs: options.intraIterationSleepMs,
        })
        if (result.right.hasMore) {
          yield* Effect.yieldNow()
        } else {
          const state = yield* Ref.get(stateRef)
          if (state.isShutdown) {
            return
          }

          yield* Effect.sleep(getDurationWithJitter(options.intraIterationSleepMs, 0.1))
        }
      }).pipe(
        Effect.catchAll((error) => {
          return Effect.logError('Uncaught error in iteration effect').pipe(
            Effect.annotateLogs('service', 'background-processor'),
            Effect.annotateLogs('executorId', options.executorId),
            Effect.annotateLogs('processName', options.processName),
            Effect.annotateLogs('error', error),
          )
        }),
        Effect.repeat(
          Schedule.recurUntilEffect(() =>
            Ref.get(stateRef).pipe(Effect.map((state) => state.isShutdown)),
          ),
        ),
      )
    }).pipe(
      Effect.catchAllCause((cause) => {
        if (!Cause.isInterruptedOnly(cause)) {
          return Effect.logError('Uncaught error in background processor').pipe(
            Effect.annotateLogs('service', 'background-processor'),
            Effect.annotateLogs('executorId', options.executorId),
            Effect.annotateLogs('processName', options.processName),
            Effect.annotateLogs('cause', cause),
          )
        }
        return Effect.void
      }),
    ),
)

export const makeBackgroundProcessor = Effect.fn((options: BackgroundProcessorOptions) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const stateRef = yield* Ref.make<BackgroundProcessorState>({
        isStarted: false,
        isShutdown: false,
      })

      const runBackgroundProcessFiberRef = yield* Ref.make<
        Fiber.RuntimeFiber<void, never> | undefined
      >(undefined)
      const scopeRef = yield* Ref.make<Scope.CloseableScope | undefined>(undefined)

      const isRunning = Effect.gen(function* () {
        return yield* Ref.get(stateRef).pipe(
          Effect.map((state) => state.isStarted && !state.isShutdown),
        )
      })

      const start = Effect.gen(function* () {
        const isStarted = yield* Ref.modify(stateRef, (state) => {
          return state.isStarted || state.isShutdown
            ? [false, state]
            : [true, { ...state, isStarted: true }]
        })
        if (!isStarted) {
          return
        }

        const scope = yield* Scope.make()
        yield* Ref.set(scopeRef, scope)

        const fiber = yield* runBackgroundProcess(options, stateRef).pipe(Effect.forkIn(scope))
        yield* Ref.set(runBackgroundProcessFiberRef, fiber)
      })

      const shutdown = Effect.gen(function* () {
        const isAlreadyShutdown = yield* Ref.modify(stateRef, (state) =>
          state.isShutdown ? [true, state] : [false, { ...state, isShutdown: true }],
        )
        if (isAlreadyShutdown) {
          return
        }

        const runBackgroundProcessFiber = yield* Ref.get(runBackgroundProcessFiberRef)
        if (runBackgroundProcessFiber != null) {
          yield* Fiber.join(runBackgroundProcessFiber).pipe(
            Effect.interruptible,
            Effect.timeout(Duration.millis(options.shutdownTimeoutMs ?? 15_000)),
            Effect.tapError(() =>
              Effect.logWarning('Background processor shutdown timed out').pipe(
                Effect.annotateLogs('service', 'background-processor'),
                Effect.annotateLogs('executorId', options.executorId),
                Effect.annotateLogs('processName', options.processName),
              ),
            ),
            Effect.ignore,
          )
        }

        const scope = yield* Ref.get(scopeRef)
        if (scope != null) {
          yield* Scope.close(scope, Exit.void)
          yield* Ref.set(scopeRef, undefined)
        }
      })

      return { start, isRunning, shutdown }
    }),
    (backgroundProcessor) => backgroundProcessor.shutdown,
  ),
)

export type BackgroundProcessor = Effect.Effect.Success<ReturnType<typeof makeBackgroundProcessor>>
