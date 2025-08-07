import { WrappedSerializer } from 'src/serializer'
import { describe, it } from 'vitest'

describe('wrappedSerializer', () => {
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
})
