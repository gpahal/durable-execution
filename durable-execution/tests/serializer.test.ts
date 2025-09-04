import { Effect, Either } from 'effect'

import { createSuperjsonSerializer, DurableExecutionError } from '../src'
import { makeSerializerInternal, SerializerService } from '../src/serializer'

describe('serializer', () => {
  it('should handle serializer errors', async () => {
    let executionCount = 0
    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, {
          serialize: (value) => {
            executionCount++
            if (value === 1) {
              return '1'
            }
            throw new Error('serialize error')
          },
          deserialize: <T>(value: string): T => {
            executionCount++
            if (value === '1') {
              return 1 as T
            }
            throw new Error('deserialize error')
          },
        }),
      ),
    )

    expect(await Effect.runPromise(serializer.serialize(1))).toBe('1')
    expect(await Effect.runPromise(serializer.deserialize('1'))).toBe(1)
    expect(executionCount).toBe(2)

    await expect(Effect.runPromise(serializer.serialize(2))).rejects.toThrow('serialize error')
    await expect(Effect.runPromise(serializer.deserialize('2'))).rejects.toThrow(
      'deserialize error',
    )
    expect(executionCount).toBe(4)
  })

  it('should create superjson serializer', () => {
    const serializer = createSuperjsonSerializer()

    const value = { test: 'value', number: 123, date: new Date() }
    const serialized = serializer.serialize(value) as string
    const deserialized = serializer.deserialize(serialized)

    expect(deserialized).toEqual(value)
  })

  it('should handle size limit in serializer', async () => {
    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, createSuperjsonSerializer()),
      ),
    )

    const largeValue = 'x'.repeat(1000)

    await expect(Effect.runPromise(serializer.serialize(largeValue, 100))).rejects.toThrow(
      'exceeds maximum allowed size',
    )
  })

  it('should handle serialization errors in serializer', async () => {
    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, {
          serialize: () => {
            throw new Error('Serialization failed')
          },
          deserialize: () => {
            throw new Error('Deserialization failed')
          },
        }),
      ),
    )

    await expect(Effect.runPromise(serializer.serialize('test'))).rejects.toThrow(
      'Serialization failed',
    )
    await expect(Effect.runPromise(serializer.deserialize('test'))).rejects.toThrow(
      'Deserialization failed',
    )
  })

  it('should preserve DurableExecutionError in serializer', async () => {
    const originalError = DurableExecutionError.nonRetryable('Original error')

    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, {
          serialize: () => {
            throw originalError
          },
          deserialize: () => {
            throw originalError
          },
        }),
      ),
    )

    const result1 = await serializer.serialize('test').pipe(Effect.either, Effect.runPromise)
    expect(Either.isLeft(result1)).toBe(true)
    assert(Either.isLeft(result1))
    expect(result1.left).toStrictEqual(originalError)

    const result2 = await serializer.deserialize('test').pipe(Effect.either, Effect.runPromise)
    expect(Either.isLeft(result2)).toBe(true)
    assert(Either.isLeft(result2))
    expect(result2.left).toStrictEqual(originalError)
  })

  it('should handle undefined max size in serializer', async () => {
    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, createSuperjsonSerializer()),
      ),
    )

    const value = 'x'.repeat(1000)
    const result = await serializer.serialize(value).pipe(Effect.runPromise)

    expect(result).toBeDefined()
    expect(result.length).toBeGreaterThan(1000)
  })

  it('should handle early size estimation edge case in serializer', async () => {
    const serializer = await Effect.runPromise(
      makeSerializerInternal.pipe(
        Effect.provideService(SerializerService, createSuperjsonSerializer()),
      ),
    )

    const value = 'a'.repeat(100)
    const serialized = await serializer.serialize(value).pipe(Effect.runPromise)

    const maxSize = Math.floor(serialized.length * 2.5)

    const result = await serializer.serialize(value, maxSize).pipe(Effect.runPromise)
    expect(result).toBeDefined()
    expect(result).toBe(serialized)

    expect(serialized.length * 4).toBeGreaterThan(maxSize)
    expect(Buffer.byteLength(serialized, 'utf8')).toBeLessThanOrEqual(maxSize)
  })
})
