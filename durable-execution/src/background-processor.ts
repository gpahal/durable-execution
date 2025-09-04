import { Cause, Effect, Either, Fiber, Ref, Schedule } from 'effect'

import { getErrorMessage } from '@gpahal/std/errors'

import { convertErrorToDurableExecutionError, DurableExecutionCancelledError } from './errors'
import { makeSleepWithWakeupController, type SleepWithWakeupController } from './utils'

const BACKGROUND_PROCESSOR_MAX_CONSECUTIVE_ERRORS_COUNT = 3

export type BackgroundProcessorOptions = {
  readonly processName: string
  readonly intraIterationSleepMs: number
  readonly iterationEffect: Effect.Effect<{ hasMore: boolean }, unknown>
}

type BackgroundProcessorState = {
  readonly isShutdown: boolean
}

const runBackgroundProcess = Effect.fn(
  (
    options: BackgroundProcessorOptions,
    stateRef: Ref.Ref<BackgroundProcessorState>,
    sleepWithWakeupController: SleepWithWakeupController,
  ) =>
    Effect.gen(function* () {
      const localStateRef = yield* Ref.make({
        consecutiveErrorsCount: 0,
        intraIterationSleepMs: options.intraIterationSleepMs,
      })

      yield* sleepWithWakeupController.sleep(options.intraIterationSleepMs, { jitterRatio: 0.5 })
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
          yield* Effect.logError(
            `Error in background processor ${options.processName}: ${result.left.message}`,
          ).pipe(Effect.annotateLogs('error', result.left))

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

          yield* sleepWithWakeupController.sleep(localState.intraIterationSleepMs, {
            jitterRatio: 0.1,
          })
          return
        }

        yield* Ref.set(localStateRef, {
          consecutiveErrorsCount: 0,
          intraIterationSleepMs: options.intraIterationSleepMs,
        })
        if (result.right.hasMore) {
          yield* Effect.sleep(5)
        } else {
          const state = yield* Ref.get(stateRef)
          if (state.isShutdown) {
            return
          }

          yield* sleepWithWakeupController.sleep(options.intraIterationSleepMs, {
            jitterRatio: 0.1,
          })
        }
      }).pipe(
        Effect.catchAll((error) => {
          return Effect.logError(
            `Error in background processor ${options.processName}: ${getErrorMessage(error)}`,
          ).pipe(Effect.annotateLogs('error', error))
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
          return Effect.logError(`Error in background processor ${options.processName}`).pipe(
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
        isShutdown: false,
      })

      const sleepWithWakeupController = yield* makeSleepWithWakeupController()
      const runBackgroundProcessFiber = yield* runBackgroundProcess(
        options,
        stateRef,
        sleepWithWakeupController,
      ).pipe(Effect.fork)

      const shutdown = Effect.gen(function* () {
        yield* Ref.set(stateRef, { isShutdown: true })
        yield* sleepWithWakeupController.wakeup
        yield* Fiber.join(runBackgroundProcessFiber)
      }).pipe(
        Effect.timeout(15_000),
        Effect.tapError(() =>
          Effect.gen(function* () {
            yield* Fiber.interrupt(runBackgroundProcessFiber)
            yield* Effect.logError(`Background processor ${options.processName} timed out`)
          }),
        ),
        Effect.ignore,
      )

      return { wakeup: sleepWithWakeupController.wakeup, shutdown }
    }),
    (backgroundProcessor) => backgroundProcessor.shutdown,
  ),
)

export type BackgroundProcessor = Effect.Effect.Success<ReturnType<typeof makeBackgroundProcessor>>
