import { describe, expect, it } from 'vitest'

import {
  DurableExecutionCancelledError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from '../src/errors'

describe('errorEdgeCases', () => {
  it('should handle DurableExecutionNotFoundError getErrorType', () => {
    const error = new DurableExecutionNotFoundError('Not found')
    expect(error.getErrorType()).toBe('not_found')
    expect(error.message).toBe('Not found')
    expect(error.isRetryable).toBe(false)
  })

  it('should handle DurableExecutionTimedOutError getErrorType', () => {
    const error = new DurableExecutionTimedOutError('Timed out')
    expect(error.getErrorType()).toBe('timed_out')
    expect(error.message).toBe('Timed out')
    expect(error.isRetryable).toBe(true)
  })

  it('should handle DurableExecutionCancelledError getErrorType', () => {
    const error = new DurableExecutionCancelledError('Cancelled')
    expect(error.getErrorType()).toBe('cancelled')
    expect(error.message).toBe('Cancelled')
    expect(error.isRetryable).toBe(false)
  })

  it('should handle DurableExecutionCancelledError with default message', () => {
    const error = new DurableExecutionCancelledError()
    expect(error.getErrorType()).toBe('cancelled')
    expect(error.message).toBe('Task cancelled')
    expect(error.isRetryable).toBe(false)
  })
})
