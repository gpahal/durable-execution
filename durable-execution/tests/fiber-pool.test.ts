import { Clock, Effect, Exit, Fiber, Ref } from 'effect'

import { DurableExecutionError } from '../src/errors'
import { makeFiberPool } from '../src/fiber-pool'

describe('fiberPool', () => {
  it('should fork and track fibers', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-pool',
          })

          const counter = yield* Ref.make(0)

          const fiber1 = yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('100 millis')
              yield* Ref.update(counter, (n) => n + 1)
              return 'result1'
            }),
          )

          const fiber2 = yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('50 millis')
              yield* Ref.update(counter, (n) => n + 10)
              return 'result2'
            }),
          )

          yield* Effect.sleep('150 millis')

          const result1 = yield* Fiber.await(fiber1)
          const result2 = yield* Fiber.await(fiber2)

          expect(Exit.isSuccess(result1)).toBe(true)
          expect(Exit.isSuccess(result2)).toBe(true)
          if (Exit.isSuccess(result1)) {
            expect(result1.value).toBe('result1')
          }
          if (Exit.isSuccess(result2)) {
            expect(result2.value).toBe('result2')
          }

          const count = yield* Ref.get(counter)
          expect(count).toBe(11)

          yield* pool.shutdown
        }),
      ),
    )
  })

  it('should prevent forking after shutdown', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-pool',
          })

          yield* pool.shutdown

          const result = yield* pool.fork(Effect.succeed('test')).pipe(Effect.either)

          expect(result._tag).toBe('Left')
          if (result._tag === 'Left') {
            expect(result.left).toBeInstanceOf(DurableExecutionError)
            expect(result.left.message).toContain('Fiber pool shutdown')
          }
        }),
      ),
    )
  })

  it('should handle fiber removal on completion', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-pool',
          })

          const completed = yield* Ref.make(false)

          const fiber = yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('50 millis')
              yield* Ref.set(completed, true)
            }),
          )

          yield* Effect.sleep('100 millis')

          const result = yield* Fiber.await(fiber)
          expect(Exit.isSuccess(result)).toBe(true)

          const isCompleted = yield* Ref.get(completed)
          expect(isCompleted).toBe(true)

          yield* pool.shutdown
        }),
      ),
    )
  })

  it('should wait for undefined fibers during shutdown', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-pool',
          })

          const startedRef = yield* Ref.make(false)
          const completedRef = yield* Ref.make(false)

          yield* pool.fork(
            Effect.gen(function* () {
              yield* Ref.set(startedRef, true)
              yield* Effect.yieldNow()
              yield* Effect.sleep('10 millis')
              yield* Ref.set(completedRef, true)
            }),
          )

          const shutdownFiber = yield* pool.shutdown.pipe(Effect.fork)

          yield* Effect.sleep('100 millis')

          const shutdownResult = yield* Fiber.await(shutdownFiber)
          expect(Exit.isSuccess(shutdownResult)).toBe(true)

          const completed = yield* Ref.get(completedRef)
          expect(completed).toBe(true)
        }),
      ),
    )
  })

  it('should interrupt leftover fibers after timeout', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-timeout-pool',
            shutdownTimeoutMs: 1000,
          })

          const interruptedRef = yield* Ref.make(false)

          yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('30 seconds')
            }).pipe(Effect.onInterrupt(() => Ref.set(interruptedRef, true))),
          )

          const start = yield* Clock.currentTimeMillis
          yield* pool.shutdown
          const elapsed = (yield* Clock.currentTimeMillis) - start

          expect(elapsed).toBeLessThan(5000)

          const wasInterrupted = yield* Ref.get(interruptedRef)
          expect(wasInterrupted).toBe(true)
        }),
      ),
    )
  })

  it('should handle multiple fibers with mixed completion times', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-mixed-pool',
            shutdownTimeoutMs: 1000,
          })

          const results = yield* Ref.make<Array<string>>([])

          yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('100 millis')
              yield* Ref.update(results, (arr) => [...arr, 'slow'])
            }),
          )

          yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('50 millis')
              yield* Ref.update(results, (arr) => [...arr, 'fast'])
            }),
          )

          yield* pool.fork(Effect.never)

          yield* Effect.sleep('150 millis')

          const shutdownStart = yield* Clock.currentTimeMillis
          yield* pool.shutdown
          const shutdownElapsed = (yield* Clock.currentTimeMillis) - shutdownStart

          expect(shutdownElapsed).toBeLessThan(5000)

          const finalResults = yield* Ref.get(results)
          expect(finalResults).toContain('fast')
          expect(finalResults).toContain('slow')
        }),
      ),
    )
  })

  it('should handle concurrent fiber updates safely', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-concurrent-pool',
          })

          const counter = yield* Ref.make(0)

          const fibers = yield* Effect.all(
            Array.from({ length: 10 }, (_, i) =>
              pool.fork(
                Effect.gen(function* () {
                  yield* Effect.sleep(`${10 * (i + 1)} millis`)
                  yield* Ref.update(counter, (n) => n + 1)
                  return i
                }),
              ),
            ),
          )

          yield* Effect.sleep('200 millis')

          const results = yield* Effect.all(fibers.map((f) => Fiber.await(f)))

          for (let i = 0; i < 10; i++) {
            const result = results[i]!
            expect(Exit.isSuccess(result)).toBe(true)
            if (Exit.isSuccess(result)) {
              expect(result.value).toBe(i)
            }
          }

          const count = yield* Ref.get(counter)
          expect(count).toBe(10)

          yield* pool.shutdown
        }),
      ),
    )
  })

  it('should handle shutdown with no active fibers', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-empty-pool',
          })

          const start = Date.now()
          yield* pool.shutdown
          const elapsed = Date.now() - start

          expect(elapsed).toBeLessThan(1000)
        }),
      ),
    )
  })

  it('should handle fiber that completes during shutdown', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const pool = yield* makeFiberPool({
            executorId: 'test-executor-id',
            processName: 'test-racing-pool',
          })

          const completedRef = yield* Ref.make(false)

          yield* pool.fork(
            Effect.gen(function* () {
              yield* Effect.sleep('100 millis')
              yield* Ref.set(completedRef, true)
            }),
          )
          yield* pool.shutdown

          const completed = yield* Ref.get(completedRef)
          expect(completed).toBe(true)
        }),
      ),
    )
  })
})
