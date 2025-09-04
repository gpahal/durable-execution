import { Effect, Fiber, HashMap, Ref } from 'effect'

import { DurableExecutionError } from './errors'

export type FiberPoolOptions = {
  readonly processName: string
  readonly finalizerTimeoutMs: number
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
          const fiber = yield* Effect.fork(
            effect.pipe(
              Effect.ensuring(Ref.update(fibersRef, (fibers) => HashMap.remove(fibers, counter))),
            ),
          )
          yield* Ref.update(fibersRef, (fibers) => {
            if (!HashMap.has(fibers, counter)) {
              return fibers
            }
            return HashMap.set(fibers, counter, fiber)
          })
          return fiber
        })

      const shutdown = Effect.gen(function* () {
        yield* Ref.update(stateRef, (state) => ({ ...state, isShutdown: true }))

        const deadline = Date.now() + options.finalizerTimeoutMs
        while (Date.now() < deadline) {
          const fibersMap = yield* Ref.get(fibersRef)
          if (HashMap.size(fibersMap) === 0) {
            break
          }

          const definedFibers = [...HashMap.values(fibersMap)].filter(
            (f): f is Fiber.Fiber<unknown, unknown> => f != null,
          )

          if (definedFibers.length > 0) {
            const remainingMs = Math.max(0, deadline - Date.now())
            yield* Fiber.joinAll(definedFibers).pipe(Effect.timeout(remainingMs), Effect.ignore)
          } else {
            yield* Effect.yieldNow()
          }
        }

        const leftoverFibers = [...HashMap.values(yield* Ref.get(fibersRef))].filter(
          (f): f is Fiber.Fiber<unknown, unknown> => f != null,
        )
        if (leftoverFibers.length > 0) {
          yield* Fiber.interruptAll(leftoverFibers)
          yield* Effect.logError(`Fiber pool ${options.processName} timed out`)
        }
      })

      return { fork, shutdown }
    }),
    (fiberPool) => fiberPool.shutdown,
  ),
)

export type FiberPool = Effect.Effect.Success<ReturnType<typeof makeFiberPool>>
