import { wrapSerializer } from 'src/serializer'
import { describe, it } from 'vitest'

describe('wrapSerializer', () => {
  it('should handle wrapper serializer errors', () => {
    const serializer = wrapSerializer({
      serialize: () => {
        throw new Error('serialize error')
      },
      deserialize: () => {
        throw new Error('deserialize error')
      },
    })

    expect(() => serializer.serialize(1)).toThrow('Error serializing value: serialize error')
    expect(() => serializer.deserialize('1')).toThrow(
      'Error deserializing value: deserialize error',
    )
  })
})
