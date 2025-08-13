import { describe, expect, it } from 'vitest'

import { createConsoleLogger, createLoggerWithDebugDisabled } from '../src/logger'

describe('logger', () => {
  it('should create console logger', () => {
    const logger = createConsoleLogger('test')
    expect(logger).toBeDefined()
    expect(typeof logger.debug).toBe('function')
    expect(typeof logger.info).toBe('function')
    expect(typeof logger.error).toBe('function')
  })

  it('should create logger with debug disabled', () => {
    let executionCount = 0
    const originalLogger = {
      debug: () => {
        executionCount++
      },
      info: () => {
        executionCount++
      },
      error: () => {
        executionCount++
      },
    }
    const disabledLogger = createLoggerWithDebugDisabled(originalLogger)

    expect(disabledLogger).toBeDefined()
    expect(typeof disabledLogger.debug).toBe('function')
    expect(typeof disabledLogger.info).toBe('function')
    expect(typeof disabledLogger.error).toBe('function')

    expect(() => disabledLogger.debug('test')).not.toThrow()
    expect(() => disabledLogger.info('test')).not.toThrow()
    expect(() => disabledLogger.error('test')).not.toThrow()

    expect(executionCount).toBe(2)
  })
})
