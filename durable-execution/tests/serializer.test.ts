import { createSuperjsonSerializer, DurableExecutionError } from '../src'
import { SerializerInternal, zSerializer } from '../src/serializer'

describe('zSerializer', () => {
  it('should handle serializer validation', () => {
    expect(() =>
      zSerializer.parse({
        serialize: () => 'test',
        deserialize: () => 'test',
      }),
    ).not.toThrow()

    expect(() => zSerializer.parse({})).toThrow()

    expect(() =>
      zSerializer.parse({
        serialize: 1,
        deserialize: () => 'test',
      }),
    ).toThrow()

    expect(() =>
      zSerializer.parse({
        serialize: () => 'test',
        deserialize: 1,
      }),
    ).toThrow()
  })
})

describe('serializer', () => {
  it('should handle serializer errors', () => {
    let executionCount = 0
    const serializer = new SerializerInternal({
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
    })

    expect(serializer.serialize(1)).toBe('1')
    expect(serializer.deserialize('1')).toBe(1)
    expect(executionCount).toBe(2)

    expect(() => serializer.serialize(2)).toThrow('Error serializing value: serialize error')
    expect(() => serializer.deserialize('2')).toThrow(
      'Error deserializing value: deserialize error',
    )
    expect(executionCount).toBe(4)
  })

  it('should create superjson serializer', () => {
    const serializer = createSuperjsonSerializer()

    const value = { test: 'value', number: 123, date: new Date() }
    const serialized = serializer.serialize(value)
    const deserialized = serializer.deserialize(serialized)

    expect(deserialized).toEqual(value)
  })

  it('should handle size limit in serializer', () => {
    const serializer = new SerializerInternal(createSuperjsonSerializer())

    const largeValue = 'x'.repeat(1000)

    expect(() => {
      serializer.serialize(largeValue, 100)
    }).toThrow(DurableExecutionError)

    expect(() => {
      serializer.serialize(largeValue, 100)
    }).toThrow('exceeds maximum allowed size')
  })

  it('should handle serialization errors in serializer', () => {
    const serializer = new SerializerInternal({
      serialize: () => {
        throw new Error('Serialization failed')
      },
      deserialize: () => {
        throw new Error('Deserialization failed')
      },
    })

    expect(() => {
      serializer.serialize('test')
    }).toThrow(DurableExecutionError)

    expect(() => {
      serializer.serialize('test')
    }).toThrow('Error serializing value')

    expect(() => {
      serializer.deserialize('test')
    }).toThrow(DurableExecutionError)

    expect(() => {
      serializer.deserialize('test')
    }).toThrow('Error deserializing value')
  })

  it('should preserve DurableExecutionError in serializer', () => {
    const originalError = DurableExecutionError.nonRetryable('Original error')

    const serializer = new SerializerInternal({
      serialize: () => {
        throw originalError
      },
      deserialize: () => {
        throw originalError
      },
    })

    expect(() => {
      serializer.serialize('test')
    }).toThrow(originalError)

    expect(() => {
      serializer.deserialize('test')
    }).toThrow(DurableExecutionError)

    try {
      serializer.deserialize('test')
    } catch (error) {
      expect(error).toBeInstanceOf(DurableExecutionError)
      expect((error as Error).message).toContain('Original error')
    }
  })

  it('should handle undefined max size in serializer', () => {
    const serializer = new SerializerInternal(createSuperjsonSerializer())

    const value = 'x'.repeat(1000)
    const result = serializer.serialize(value)

    expect(result).toBeDefined()
    expect(result.length).toBeGreaterThan(1000)
  })
})
