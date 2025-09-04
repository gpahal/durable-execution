import { Clock, Effect, Exit, Fiber, HashMap, Ref, Scope } from 'effect'

import { DurableExecutionError } from './errors'

export type FiberPoolOptions = {
  readonly executorId: string
  readonly processName: string
  readonly shutdownTimeoutMs?: number
}

export const makeFiberPool = Effect.fn((options: FiberPoolOptions) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const stateRef = yield* Ref.make({
        isShutdown: false,
        counter: 0,
      })
      const fibersRef = yield* Ref.make<
        HashMap.HashMap<number, Fiber.Fiber<unknown, unknown> | undefined>
      >(HashMap.empty())

      const scope = yield* Scope.make()

      const fork = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        Effect.gen(function* () {
          const counter = yield* Ref.modify(stateRef, (state) => {
            if (state.isShutdown) {
              return [-1, state]
            }
            return [state.counter, { ...state, counter: state.counter + 1 }]
          })
          if (counter < 0) {
            return yield* Effect.fail(DurableExecutionError.nonRetryable('Fiber pool shutdown'))
          }

          yield* Ref.update(fibersRef, (fibers) => HashMap.set(fibers, counter, undefined))
          const fiber = yield* effect
            .pipe(
              Effect.ensuring(Ref.update(fibersRef, (fibers) => HashMap.remove(fibers, counter))),
            )
            .pipe(Effect.forkIn(scope))
          yield* Ref.update(fibersRef, (fibers) => {
            if (!HashMap.has(fibers, counter)) {
              return fibers
            }
            return HashMap.set(fibers, counter, fiber)
          })
          return fiber
        })

      const shutdown = Effect.gen(function* () {
        const isAlreadyShutdown = yield* Ref.modify(stateRef, (state) =>
          state.isShutdown ? [true, state] : [false, { ...state, isShutdown: true }],
        )
        if (isAlreadyShutdown) {
          return
        }

        const deadline = (yield* Clock.currentTimeMillis) + (options.shutdownTimeoutMs ?? 15_000)
        while ((yield* Clock.currentTimeMillis) < deadline) {
          const fibersMap = yield* Ref.get(fibersRef)
          if (HashMap.size(fibersMap) === 0) {
            break
          }

          const definedFibers = [...HashMap.values(fibersMap)].filter(
            (f): f is Fiber.Fiber<unknown, unknown> => f != null,
          )

          if (definedFibers.length > 0) {
            const remainingMs = Math.max(0, deadline - (yield* Clock.currentTimeMillis))
            yield* Fiber.joinAll(definedFibers).pipe(
              Effect.interruptible,
              Effect.timeout(remainingMs),
              Effect.tapError(() =>
                Effect.logWarning('Fiber pool shutdown timed out').pipe(
                  Effect.annotateLogs('service', 'fiber-pool'),
                  Effect.annotateLogs('executorId', options.executorId),
                  Effect.annotateLogs('processName', options.processName),
                ),
              ),
              Effect.ignore,
            )
          } else {
            yield* Effect.yieldNow()
          }
        }

        yield* Scope.close(scope, Exit.void)
      })

      return { fork, shutdown }
    }),
    (fiberPool) => fiberPool.shutdown,
  ),
)

export type FiberPool = Effect.Effect.Success<ReturnType<typeof makeFiberPool>>
