import { Effect, Either } from 'effect'

import { makeBackgroundBatchProcessor } from '../src/background-batch-processor'
import { DurableExecutionError } from '../src/errors'

describe('backgroundBatchProcessor', () => {
  describe('batching enabled', () => {
    it('should process batch requests and return array results', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-batch-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (inputs: ReadonlyArray<number>) =>
                Effect.succeed(inputs.map((x) => x * 2)),
            })

            yield* processor.start

            const results = yield* Effect.all([
              processor.processRequest({ input: 5 }),
              processor.processRequest({ input: 10 }),
              processor.processRequest({ input: 15 }),
            ])

            expect(results).toEqual([10, 20, 30])

            yield* processor.shutdown
          }),
        ),
      )
    })

    it('should handle invalid return type from processBatchRequests', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-invalid-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) =>
                Effect.succeed('hello' as unknown as ReadonlyArray<number>),
            })

            yield* processor.start

            const result = yield* processor.processRequest({ input: 5 }).pipe(Effect.either)
            expect(Either.isLeft(result)).toBe(true)
            assert(Either.isLeft(result))
            expect(result.left.message).toContain('Invalid return type')

            yield* processor.shutdown
          }),
        ),
      )
    })

    it('should handle batch weight overflow and create new batch', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const batchSizes: Array<number> = []

            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-overflow-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (inputs: ReadonlyArray<number>) => {
                batchSizes.push(inputs.length)
                return Effect.succeed(inputs.map((x) => x * 2))
              },
            })

            yield* processor.start

            yield* Effect.all([
              processor.processRequest({ input: 1, weight: 60 }),
              processor.processRequest({ input: 2, weight: 30 }),
              processor.processRequest({ input: 3, weight: 30 }),
              processor.processRequest({ input: 4, weight: 30 }),
            ])

            expect(batchSizes.length).toBeGreaterThan(1)

            yield* processor.shutdown
          }),
        ),
      )
    })

    it('should handle null return from processBatchRequests', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-null-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) => Effect.succeed(null),
            })

            yield* processor.start

            const result = yield* processor.processRequest({ input: 5 })
            expect(result).toBeUndefined()

            yield* processor.shutdown
          }),
        ),
      )
    })

    it('should handle array result with wrong length', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-wrong-length-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) => Effect.succeed([1, 2, 3]),
            })

            yield* processor.start

            const result = yield* processor.processRequest({ input: 5 }).pipe(Effect.either)
            expect(result._tag).toBe('Left')
            if (result._tag === 'Left') {
              expect(result.left.message).toContain('Invalid number of results')
            }

            yield* processor.shutdown
          }),
        ),
      )
    })
  })

  describe('batching disabled', () => {
    it('should process requests without batching', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-no-batch-processor',
              enableBatching: false,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (inputs: ReadonlyArray<number>) =>
                Effect.succeed(inputs.map((x) => x * 3)),
            })

            const result = yield* processor.processRequest({ input: 7 })
            expect(result).toBe(21)
          }),
        ),
      )
    })

    it('should handle wrong array length in non-batched mode', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-wrong-length-processor',
              enableBatching: false,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) => Effect.succeed([1, 2, 3]),
            })

            const result = yield* processor.processRequest({ input: 7 }).pipe(Effect.either)
            expect(result._tag).toBe('Left')
            if (result._tag === 'Left') {
              expect(result.left.message).toContain('Invalid number of results')
            }
          }),
        ),
      )
    })

    it('should handle invalid return type in non-batched mode', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-invalid-single-processor',
              enableBatching: false,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) =>
                Effect.succeed('hello' as unknown as ReadonlyArray<string>),
            })

            const result = yield* processor.processRequest({ input: 7 }).pipe(Effect.either)
            expect(result._tag).toBe('Left')
            if (result._tag === 'Left') {
              expect(result.left.message).toContain('Invalid return type')
            }
          }),
        ),
      )
    })

    it('should handle null return value in non-batched mode', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-null-processor',
              enableBatching: false,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) => Effect.succeed(null),
            })

            const result = yield* processor.processRequest({ input: 7 })
            expect(result).toBeUndefined()
          }),
        ),
      )
    })
  })

  describe('error handling', () => {
    it('should handle errors in batch processing', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-error-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) =>
                Effect.fail(new Error('Test error')),
            })

            yield* processor.start

            const result = yield* processor.processRequest({ input: 5 }).pipe(Effect.either)
            expect(result._tag).toBe('Left')
            if (result._tag === 'Left') {
              expect(result.left.message).toContain('Test error')
            }

            yield* processor.shutdown
          }),
        ),
      )
    })

    it('should handle conversion to DurableExecutionError', async () => {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const processor = yield* makeBackgroundBatchProcessor({
              executorId: 'test-executor-id',
              processName: 'test-error-conversion-processor',
              enableBatching: true,
              batchWeight: 100,
              intraBatchSleepMs: 10,
              processBatchRequests: (_inputs: ReadonlyArray<number>) =>
                Effect.fail(new Error('Random error')),
            })

            yield* processor.start

            const result = yield* processor.processRequest({ input: 5 }).pipe(Effect.either)
            expect(result._tag).toBe('Left')
            if (result._tag === 'Left') {
              expect(result.left).toBeInstanceOf(DurableExecutionError)
              expect(result.left.message).toContain('Random error')
            }

            yield* processor.shutdown
          }),
        ),
      )
    })
  })
})
