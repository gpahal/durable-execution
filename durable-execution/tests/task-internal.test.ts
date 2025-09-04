import { Effect } from 'effect'

import { type CommonTaskOptions, type TaskEnqueueOptions } from '../src'
import {
  generateTaskExecutionId,
  overrideTaskEnqueueOptions,
  validateCommonTaskOptions,
  validateEnqueueOptions,
  validateTaskId,
} from '../src/task-internal'

const validateCommonTaskOptionsPromise = (options: CommonTaskOptions) => {
  return validateCommonTaskOptions(options).pipe(Effect.runSync)
}

describe('validateCommonTaskOptions', () => {
  it('should handle valid common task options', () => {
    const optionsArr: Array<CommonTaskOptions> = [
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
        id: 'test2',
        retryOptions: {
          maxAttempts: 1,
        },
        sleepMsBeforeRun: 0,
        timeoutMs: 2000,
      },
      {
        id: 'test2',
        retryOptions: {
          maxAttempts: 1,
        },
        sleepMsBeforeRun: 1,
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

    for (const options of optionsArr) {
      expect(() => validateCommonTaskOptionsPromise(options)).not.toThrow()
    }
  })

  it('should throw an error if the id is empty', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: '',
        timeoutMs: 1000,
      }),
    ).toThrow('Task id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'a'.repeat(256),
        timeoutMs: 1000,
      }),
    ).toThrow('Task id cannot be longer than 255 characters')
  })

  it('should throw an error if the timeoutMs is not a number', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        timeoutMs: '1000',
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeout is undefined', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        timeoutMs: undefined,
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeout is negative', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'test',
        timeoutMs: -1,
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is not a number', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'test',
        // @ts-expect-error - Testing invalid input
        sleepMsBeforeRun: '1000',
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is negative', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
        id: 'test',
        sleepMsBeforeRun: -1,
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is not a number', () => {
    expect(() =>
      validateCommonTaskOptionsPromise({
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
      validateCommonTaskOptionsPromise({
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
      validateCommonTaskOptionsPromise({
        id: 'test',
        retryOptions: {
          maxAttempts: 10_000,
        },
        timeoutMs: 1000,
      }),
    ).toThrow('Invalid retry options for task test')
  })
})

const validateEnqueueOptionsPromise = (options: TaskEnqueueOptions) => {
  return validateEnqueueOptions('test', options).pipe(Effect.runSync)
}

describe('validateEnqueueOptions', () => {
  it('should handle valid enqueue options', () => {
    const optionsArr: Array<TaskEnqueueOptions> = [
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

    for (const options of optionsArr) {
      expect(() => validateEnqueueOptionsPromise(options)).not.toThrow()
    }
  })

  it('should throw an error if the retryOptions.maxAttempts is not a number', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        retryOptions: {
          // @ts-expect-error - Testing invalid input
          maxAttempts: '1',
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is negative', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        retryOptions: {
          maxAttempts: -1,
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the retryOptions.maxAttempts is very large', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        retryOptions: {
          maxAttempts: 10_000,
        },
      }),
    ).toThrow('Invalid retry options for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is not a number', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        // @ts-expect-error - Testing invalid input
        sleepMsBeforeRun: '1000',
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the sleepMsBeforeRun is negative', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        sleepMsBeforeRun: -1,
      }),
    ).toThrow('Invalid sleep ms before run for task test')
  })

  it('should throw an error if the timeoutMs is not a number', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        // @ts-expect-error - Testing invalid input
        timeoutMs: '1000',
      }),
    ).toThrow('Invalid timeout value for task test')
  })

  it('should throw an error if the timeoutMs is negative', () => {
    expect(() =>
      validateEnqueueOptionsPromise({
        timeoutMs: -1,
      }),
    ).toThrow('Invalid timeout value for task test')
  })
})

const validateTaskIdPromise = (id: string) => {
  return validateTaskId(id).pipe(Effect.runSync)
}

describe('validateTaskId', () => {
  it('should validate an valid id', () => {
    expect(validateTaskIdPromise('valid_id')).toBeUndefined()
  })

  it('should throw an error if the id is empty', () => {
    expect(() => validateTaskIdPromise('')).toThrow('Task id cannot be empty')
  })

  it('should throw an error if the id is longer than 255 characters', () => {
    expect(() => validateTaskIdPromise('a'.repeat(256))).toThrow(
      'Task id cannot be longer than 255 characters',
    )
  })

  it('should throw an error if the id contains invalid characters', () => {
    const invalidIds = ['invalid-id', 'invalid id', '$invalid_id', 'invalid_id ']
    for (const id of invalidIds) {
      expect(() => validateTaskIdPromise(id)).toThrow(
        'Task id can only contain alphanumeric characters and underscores',
      )
    }
  })

  it('should validate task ids', () => {
    expect(() => validateTaskIdPromise('valid_task_id')).not.toThrow()
    expect(() => validateTaskIdPromise('validTaskId123')).not.toThrow()
    expect(() => validateTaskIdPromise('task123')).not.toThrow()

    expect(() => validateTaskIdPromise('')).toThrow('Task id cannot be empty')
    expect(() => validateTaskIdPromise('task id with spaces')).toThrow(
      'Task id can only contain alphanumeric characters and underscores',
    )
    expect(() => validateTaskIdPromise('task-id-with-hyphens')).toThrow(
      'Task id can only contain alphanumeric characters and underscores',
    )
    let longId = 'task'
    for (let i = 0; i < 256; i++) {
      longId += 'a'
    }
    expect(() => validateTaskIdPromise(longId)).toThrow(
      'Task id cannot be longer than 255 characters',
    )
    expect(() => validateTaskIdPromise('task@id')).toThrow(
      'Task id can only contain alphanumeric characters and underscores',
    )
    expect(() => validateTaskIdPromise('task#id')).toThrow(
      'Task id can only contain alphanumeric characters and underscores',
    )
    expect(() => validateTaskIdPromise('task%id')).toThrow(
      'Task id can only contain alphanumeric characters and underscores',
    )
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

describe('overrideTaskEnqueueOptions', () => {
  it('should override retry options when provided', () => {
    const existingOptions = {
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: 1000,
      timeoutMs: 2000,
    }

    const overrideOptions = {
      retryOptions: { maxAttempts: 3, baseDelayMs: 500 },
    }

    const result = overrideTaskEnqueueOptions(existingOptions, overrideOptions)

    expect(result.retryOptions).toEqual({ maxAttempts: 3, baseDelayMs: 500 })
    expect(result.sleepMsBeforeRun).toBe(1000)
    expect(result.timeoutMs).toBe(2000)
  })

  it('should use existing options when override is null', () => {
    const existingOptions = {
      retryOptions: { maxAttempts: 2 },
      sleepMsBeforeRun: 500,
      timeoutMs: 1500,
    }

    const result = overrideTaskEnqueueOptions(existingOptions)

    expect(result).toEqual(existingOptions)
  })

  it('should override all options when provided', () => {
    const existingOptions = {
      retryOptions: { maxAttempts: 1 },
      sleepMsBeforeRun: 1000,
      timeoutMs: 2000,
    }

    const overrideOptions = {
      retryOptions: { maxAttempts: 3 },
      sleepMsBeforeRun: 2000,
      timeoutMs: 3000,
    }

    const result = overrideTaskEnqueueOptions(existingOptions, overrideOptions)

    expect(result).toEqual({
      retryOptions: { maxAttempts: 3 },
      sleepMsBeforeRun: 2000,
      timeoutMs: 3000,
    })
  })
})
