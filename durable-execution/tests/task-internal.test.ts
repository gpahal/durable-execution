import { describe, expect, it } from 'vitest'

import { DurableExecutionError } from '../src'
import { generateTaskExecutionId, validateTaskId } from '../src/task-internal'

describe('validateTaskId', () => {
  it('should validate an valid id', () => {
    expect(validateTaskId('valid_id')).toBeUndefined()
  })

  it('should throw an error if the id is empty', () => {
    expect(() => validateTaskId('')).toThrow('Task id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() => validateTaskId('a'.repeat(256))).toThrow(
      'Task id cannot be longer than 255 characters',
    )
  })

  it('should throw an error if the id contains invalid characters', () => {
    const invalidIds = ['invalid-id', 'invalid id', '$invalid_id', 'invalid_id ']
    for (const id of invalidIds) {
      expect(() => validateTaskId(id)).toThrow(
        'Task id can only contain alphanumeric characters and underscores',
      )
    }
  })

  it('should validate task ids', () => {
    expect(() => validateTaskId('valid_task_id')).not.toThrow()
    expect(() => validateTaskId('validTaskId123')).not.toThrow()
    expect(() => validateTaskId('task123')).not.toThrow()

    expect(() => validateTaskId('')).toThrow(DurableExecutionError)
    expect(() => validateTaskId('task id with spaces')).toThrow(DurableExecutionError)
    expect(() => validateTaskId('task-id-with-hyphens')).toThrow(DurableExecutionError)
    let longId = 'task'
    for (let i = 0; i < 256; i++) {
      longId += 'a'
    }
    expect(() => validateTaskId(longId)).toThrow(DurableExecutionError)
    expect(() => validateTaskId('task@id')).toThrow(DurableExecutionError)
    expect(() => validateTaskId('task#id')).toThrow(DurableExecutionError)
    expect(() => validateTaskId('task%id')).toThrow(DurableExecutionError)
  })
})

describe('generateTaskExecutionId', () => {
  it('should generate unique task execution ids', () => {
    const id1 = generateTaskExecutionId()
    const id2 = generateTaskExecutionId()

    expect(id1).not.toBe(id2)
    expect(id1).toMatch(/^te_/)
    expect(id2).toMatch(/^te_/)
    expect(id1.length).toBeGreaterThan(10)
    expect(id2.length).toBeGreaterThan(10)
  })
})
