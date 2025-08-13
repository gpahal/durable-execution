import { describe, it } from 'vitest'

import { createSuperjsonSerializer, DurableExecutionError, WrappedSerializer } from '../src'

describe('serializer', () => {
  it('should handle wrapper serializer errors', () => {
    let executed = 0
    const serializer = new WrappedSerializer({
      serialize: (value) => {
        executed++
        if (value === 1) {
          return '1'
        }
        throw new Error('serialize error')
      },
      deserialize: <T>(value: string): T => {
        executed++
        if (value === '1') {
          return 1 as T
        }
        throw new Error('deserialize error')
      },
    })

    expect(serializer.serialize(1)).toBe('1')
    expect(serializer.deserialize('1')).toBe(1)
    expect(executed).toBe(2)

    expect(() => serializer.serialize(2)).toThrow('Error serializing value: serialize error')
    expect(() => serializer.deserialize('2')).toThrow(
      'Error deserializing value: deserialize error',
    )
    expect(executed).toBe(4)
  })

  it('should create superjson serializer', () => {
    const serializer = createSuperjsonSerializer()

    const value = { test: 'value', number: 123, date: new Date() }
    const serialized = serializer.serialize(value)
    const deserialized = serializer.deserialize(serialized)

    expect(deserialized).toEqual(value)
  })

  it('should handle size limit in WrappedSerializer', () => {
    const serializer = new WrappedSerializer(createSuperjsonSerializer())

    const largeValue = 'x'.repeat(1000)

    expect(() => {
      serializer.serialize(largeValue, 100)
    }).toThrow(DurableExecutionError)

    expect(() => {
      serializer.serialize(largeValue, 100)
    }).toThrow('exceeds maximum allowed size')
  })

  it('should handle serialization errors in WrappedSerializer', () => {
    const serializer = new WrappedSerializer({
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

  it('should preserve DurableExecutionError in WrappedSerializer', () => {
    const originalError = new DurableExecutionError('Original error', false)

    const serializer = new WrappedSerializer({
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

  it('should handle undefined max size in WrappedSerializer', () => {
    const serializer = new WrappedSerializer(createSuperjsonSerializer())

    const value = 'x'.repeat(1000)
    const result = serializer.serialize(value)

    expect(result).toBeDefined()
    expect(result.length).toBeGreaterThan(1000)
  })
})
