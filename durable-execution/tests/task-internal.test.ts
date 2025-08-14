import { DurableExecutionError, type CommonTaskOptions, type TaskEnqueueOptions } from '../src'
import {
  generateTaskExecutionId,
  validateCommonTaskOptions,
  validateEnqueueOptions,
  validateTaskId,
} from '../src/task-internal'

describe('validateCommonTaskOptions', () => {
  it('should handle valid common task options', () => {
    const options: Array<CommonTaskOptions> = [
      {
        id: 'test',
        retryOptions: {
          maxAttempts: 1,
        },
        timeoutMs: 1000,
      },
      {
        id: 'test2',
        retryOptions: {
          maxAttempts: 2,
          delayMultiplier: 2,
        },
        timeoutMs: 2000,
      },
      {
        id: 'test3',
        retryOptions: {
          maxAttempts: 3,
          baseDelayMs: 1000,
          delayMultiplier: 3,
          maxDelayMs: 3000,
        },
        sleepMsBeforeRun: 1000,
        timeoutMs: 3000,
      },
    ]

    for (const option of options) {
      expect(() => validateCommonTaskOptions(option)).not.toThrow()
    }
  })

  it('should throw an error if the id is empty', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: '',
        timeoutMs: 1000,
      }),
    ).toThrow('Task id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'a'.repeat(256),
        timeoutMs: 1000,
      }),
    ).toThrow('Task id cannot be longer than 255 characters')
  })

  it('should throw an error if the timeoutMs is not a number', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        timeoutMs: '1000',
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeout is undefined', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        timeoutMs: undefined,
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeout is negative', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        timeoutMs: -1,
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is not a number', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        sleepMsBeforeRun: '1000',
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is negative', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        sleepMsBeforeRun: -1,
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is not a number', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        retryOptions: {
          // @ts-expect-error - Testing invalid input
          maxAttempts: '1',
        },
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is negative', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        retryOptions: {
          maxAttempts: -1,
        },
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is very large', () => {
    expect(() =>
      validateCommonTaskOptions({
        id: 'test',
        retryOptions: {
          maxAttempts: 10_000,
        },
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid retry options for task test')
  })
})

describe('validateEnqueueOptions', () => {
  it('should handle valid enqueue options', () => {
    const options: Array<TaskEnqueueOptions> = [
      {
        retryOptions: {
          maxAttempts: 1,
        },
      },
      {
        retryOptions: {
          maxAttempts: 2,
          delayMultiplier: 2,
        },
      },
      {
        retryOptions: {
          maxAttempts: 3,
          baseDelayMs: 1000,
          delayMultiplier: 3,
          maxDelayMs: 3000,
        },
      },
    ]

    for (const option of options) {
      expect(() => validateEnqueueOptions('test', option)).not.toThrow()
    }
  })

  it('should throw an error if the retryOptions.maxAttempts is not a number', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        retryOptions: {
          // @ts-expect-error - Testing invalid input
          maxAttempts: '1',
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is negative', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        retryOptions: {
          maxAttempts: -1,
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is very large', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        retryOptions: {
          maxAttempts: 10_000,
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is not a number', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        // @ts-expect-error - Testing invalid input
        sleepMsBeforeRun: '1000',
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is negative', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        sleepMsBeforeRun: -1,
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the timeoutMs is not a number', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        // @ts-expect-error - Testing invalid input
        timeoutMs: '1000',
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeoutMs is negative', () => {
    expect(() =>
      validateEnqueueOptions('test', {
        timeoutMs: -1,
      }),
    ).toThrow('Invalid timeout value for task test')
  })
})

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
