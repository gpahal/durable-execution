import { describe, expect, it } from 'vitest'

import { validateTaskId } from '../src/task-internal'

describe('validate task id', () => {
  it('should validate an valid id', () => {
    expect(validateTaskId('valid_id')).toBeUndefined()
  })

  it('should throw an error if the id is empty', () => {
    expect(() => validateTaskId('')).toThrow('Id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() => validateTaskId('a'.repeat(256))).toThrow('Id cannot be longer than 255 characters')
  })

  it('should throw an error if the id contains invalid characters', () => {
    const invalidIds = ['invalid-id', 'invalid id', '$invalid_id', 'invalid_id ']
    for (const id of invalidIds) {
      expect(() => validateTaskId(id)).toThrow(
        'Id can only contain alphanumeric characters and underscores',
      )
    }
  })
})
