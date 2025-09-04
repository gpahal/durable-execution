import { Effect, Exit, Fiber, Ref } from 'effect'
import { DurableExecutionCancelledError } from 'src'

import { makeBackgroundProcessor } from '../src/background-processor'

describe('backgroundProcessor', () => {
  it('should handle iteration effect errors', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const iterationCount = yield* Ref.make(0)

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.gen(function* () {
              yield* Ref.update(iterationCount, (n) => n + 1)
              return yield* Effect.fail(new Error('Test error'))
            }),
          })

          yield* processor.start

          yield* Effect.sleep('1000 millis')

          const count = yield* Ref.get(iterationCount)
          expect(count).toBeGreaterThanOrEqual(3)
        }),
      ),
    )
  })

  it('should handle iteration effect cancelled errors', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const iterationCount = yield* Ref.make(0)

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.gen(function* () {
              yield* Ref.update(iterationCount, (n) => n + 1)
              return yield* Effect.fail(new DurableExecutionCancelledError('Test error'))
            }),
          })

          yield* processor.start

          yield* Effect.sleep('500 millis')

          const count = yield* Ref.get(iterationCount)
          expect(count).toBeGreaterThanOrEqual(3)
        }),
      ),
    )
  })

  it('should prevent double start', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const iterationCount = yield* Ref.make(0)

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.gen(function* () {
              yield* Ref.update(iterationCount, (n) => n + 1)
              return { hasMore: false }
            }),
          })

          yield* processor.start
          yield* processor.start

          yield* Effect.sleep('250 millis')

          const count = yield* Ref.get(iterationCount)
          expect(count).toBeGreaterThanOrEqual(1)
          expect(count).toBeLessThanOrEqual(3)
        }),
      ),
    )
  })

  it('should not start after shutdown', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const iterationCount = yield* Ref.make(0)

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.gen(function* () {
              yield* Ref.update(iterationCount, (n) => n + 1)
              return { hasMore: false }
            }),
          })

          yield* processor.shutdown
          yield* processor.start

          yield* Effect.sleep('200 millis')

          const count = yield* Ref.get(iterationCount)
          expect(count).toBe(0)
        }),
      ),
    )
  })

  it('should handle shutdown timeout and interrupt', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const isStartedRef = yield* Ref.make(false)
          const shouldBlock = yield* Ref.make(true)

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-timeout-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.gen(function* () {
              yield* Ref.set(isStartedRef, true)
              const block = yield* Ref.get(shouldBlock)
              if (block) {
                yield* Effect.sleep('30 seconds')
              }
              return { hasMore: false }
            }),
            shutdownTimeoutMs: 2500,
          })

          yield* processor.start

          yield* Effect.gen(function* () {
            while (true) {
              const isStarted = yield* Ref.get(isStartedRef)
              if (isStarted) break
              yield* Effect.sleep('10 millis')
            }
          })

          const shutdownFiber = yield* processor.shutdown.pipe(Effect.forkScoped)

          yield* Effect.sleep('100 millis')

          const shutdownResult = yield* Fiber.await(shutdownFiber)
          expect(Exit.isExit(shutdownResult)).toBe(true)
        }),
      ),
    )
  })

  it('should run iterations with hasMore flag', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const iterationCount = yield* Ref.make(0)
          const maxIterations = 3

          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 50,
            iterationEffect: Effect.gen(function* () {
              const count = yield* Ref.updateAndGet(iterationCount, (n) => n + 1)
              return { hasMore: count < maxIterations }
            }),
          })

          yield* processor.start

          yield* Effect.sleep('500 millis')

          const count = yield* Ref.get(iterationCount)
          expect(count).toBeGreaterThanOrEqual(maxIterations)
        }),
      ),
    )
  })

  it('should check isRunning status correctly', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const processor = yield* makeBackgroundProcessor({
            executorId: 'test-executor-id',
            processName: 'test-processor',
            intraIterationSleepMs: 100,
            iterationEffect: Effect.succeed({ hasMore: false }),
          })

          const runningBefore = yield* processor.isRunning
          expect(runningBefore).toBe(false)

          yield* processor.start

          const runningAfter = yield* processor.isRunning
          expect(runningAfter).toBe(true)

          yield* processor.shutdown

          const runningAfterShutdown = yield* processor.isRunning
          expect(runningAfterShutdown).toBe(false)
        }),
      ),
    )
  })
})
