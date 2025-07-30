import { validateId } from 'src/utils'
import { describe, it } from 'vitest'

describe('validateId', () => {
  it('should validate an valid id', () => {
    expect(validateId('valid_id')).toBeUndefined()
  })

  it('should throw an error if the id is empty', () => {
    expect(() => validateId('')).toThrow('Id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() => validateId('a'.repeat(256))).toThrow('Id cannot be longer than 255 characters')
  })

  it('should throw an error if the id contains invalid characters', () => {
    const invalidIds = ['invalid-id', 'invalid id', '$invalid_id', 'invalid_id ']
    for (const id of invalidIds) {
      expect(() => validateId(id)).toThrow(
        'Id can only contain alphanumeric characters and underscores',
      )
    }
  })
})
