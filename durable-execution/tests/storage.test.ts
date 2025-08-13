import { describe, expect, it } from 'vitest'

import {
  getTaskExecutionStorageValueParentExecutionError,
  type TaskExecutionStorageValue,
} from '../src/storage'

describe('storage', () => {
  it('should handle unknown task execution status', () => {
    // @ts-expect-error - Testing invalid input
    const execution = {
      status: 'unknown_status',
    } as TaskExecutionStorageValue

    expect(() => {
      const error = getTaskExecutionStorageValueParentExecutionError(execution)
      expect(error).toBeDefined()
    }).not.toThrow()
  })

  it('should handle parent execution error with direct error', () => {
    const execution = {
      error: {
        message: 'Direct error',
        errorType: 'generic',
        isRetryable: true,
      },
    } as TaskExecutionStorageValue

    const error = getTaskExecutionStorageValueParentExecutionError(execution)
    expect(error.message).toBe('Direct error')
  })

  it('should handle parent execution error with finalize task error', () => {
    const execution = {
      finalizeTaskExecutionError: {
        message: 'Finalize error',
        errorType: 'generic',
        isRetryable: true,
      },
      finalizeTaskExecution: {
        taskId: 'finalize-task',
      },
    } as TaskExecutionStorageValue

    const error = getTaskExecutionStorageValueParentExecutionError(execution)
    expect(error.message).toContain('Finalize task with id finalize-task failed')
    expect(error.message).toContain('Finalize error')
  })

  it('should handle parent execution error with children task errors', () => {
    const execution = {
      childrenTaskExecutionsErrors: [
        {
          taskId: 'child-1',
          error: {
            message: 'Child 1 error',
            errorType: 'generic',
            isRetryable: true,
          },
        },
        {
          taskId: 'child-2',
          error: {
            message: 'Child 2 error',
            errorType: 'generic',
            isRetryable: true,
          },
        },
      ],
    } as TaskExecutionStorageValue

    const error = getTaskExecutionStorageValueParentExecutionError(execution)
    expect(error.message).toContain('Children task errors:')
    expect(error.message).toContain('Child task with id child-1 failed: Child 1 error')
    expect(error.message).toContain('Child task with id child-2 failed: Child 2 error')
  })

  it('should handle parent execution error with no specific error', () => {
    const execution = {} as TaskExecutionStorageValue

    const error = getTaskExecutionStorageValueParentExecutionError(execution)
    expect(error.message).toBe('Unknown durable execution error')
  })
})
