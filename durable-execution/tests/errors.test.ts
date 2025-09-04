import {
  DurableExecutionCancelledError,
  DurableExecutionError,
  DurableExecutionNotFoundError,
  DurableExecutionTimedOutError,
} from '../src'

describe('errors', () => {
  it('should handle DurableExecutionError.retryable', () => {
    let error = DurableExecutionError.retryable('Error')
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(true)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Error')

    error = DurableExecutionError.retryable('Error', { isInternal: true })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(true)
    expect(error.isInternal).toBe(true)
    expect(error.message).toBe('Error')

    error = new DurableExecutionError('Error', { isRetryable: true })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(true)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Error')

    error = new DurableExecutionError('Error', { isRetryable: true, isInternal: true })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(true)
    expect(error.isInternal).toBe(true)
    expect(error.message).toBe('Error')
  })

  it('should handle DurableExecutionError.nonRetryable', () => {
    let error = DurableExecutionError.nonRetryable('Error')
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Error')

    error = DurableExecutionError.nonRetryable('Error', { isInternal: true })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(true)
    expect(error.message).toBe('Error')

    error = new DurableExecutionError('Error', { isRetryable: false })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Error')

    error = new DurableExecutionError('Error', { isRetryable: false, isInternal: true })
    expect(error.getErrorType()).toBe('generic')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(true)
    expect(error.message).toBe('Error')
  })

  it('should handle DurableExecutionNotFoundError', () => {
    const error = new DurableExecutionNotFoundError('Not found')
    expect(error.getErrorType()).toBe('not_found')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Not found')
  })

  it('should handle DurableExecutionTimedOutError', () => {
    const error = new DurableExecutionTimedOutError('Timed out')
    expect(error.getErrorType()).toBe('timed_out')
    expect(error.isRetryable).toBe(true)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Timed out')
  })

  it('should handle DurableExecutionCancelledError', () => {
    const error = new DurableExecutionCancelledError('Cancelled')
    expect(error.getErrorType()).toBe('cancelled')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Cancelled')
  })

  it('should handle DurableExecutionCancelledError with default message', () => {
    const error = new DurableExecutionCancelledError()
    expect(error.getErrorType()).toBe('cancelled')
    expect(error.isRetryable).toBe(false)
    expect(error.isInternal).toBe(false)
    expect(error.message).toBe('Task execution cancelled')
  })
})
