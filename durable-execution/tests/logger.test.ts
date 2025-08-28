import { createConsoleLogger, LoggerInternal, zLogger } from '../src/logger'

describe('zLogger', () => {
  it('should handle logger validation', () => {
    expect(() =>
      zLogger.parse({
        debug: () => {
          // Do nothing
        },
        info: () => {
          // Do nothing
        },
        error: () => {
          // Do nothing
        },
      }),
    ).not.toThrow()

    expect(() => zLogger.parse({})).toThrow()

    expect(() =>
      zLogger.parse({
        debug: 1,
        info: () => 'test',
        error: () => 'test',
      }),
    ).toThrow()

    expect(() =>
      zLogger.parse({
        debug: () => 'test',
        info: 1,
        error: () => 'test',
      }),
    ).toThrow()

    expect(() =>
      zLogger.parse({
        debug: () => 'test',
        info: () => 'test',
        error: 1,
      }),
    ).toThrow()
  })
})

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

    const disabledLogger = new LoggerInternal(originalLogger, 'info')
    expect(disabledLogger).toBeDefined()
    expect(typeof disabledLogger.debug).toBe('function')
    expect(typeof disabledLogger.info).toBe('function')
    expect(typeof disabledLogger.error).toBe('function')

    expect(() => disabledLogger.debug('test')).not.toThrow()
    expect(() => disabledLogger.info('test')).not.toThrow()
    expect(() => disabledLogger.error('test')).not.toThrow()
    expect(executionCount).toBe(2)
  })

  it('should create logger with debug enabled', () => {
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

    const enabledLogger = new LoggerInternal(originalLogger, 'debug')
    expect(enabledLogger).toBeDefined()
    expect(typeof enabledLogger.debug).toBe('function')
    expect(typeof enabledLogger.info).toBe('function')
    expect(typeof enabledLogger.error).toBe('function')

    expect(() => enabledLogger.debug('test')).not.toThrow()
    expect(() => enabledLogger.info('test')).not.toThrow()
    expect(() => enabledLogger.error('test')).not.toThrow()
    expect(executionCount).toBe(3)
  })

  it('should execute console logger methods', () => {
    const originalConsoleDebug = console.debug
    const originalConsoleInfo = console.info
    const originalConsoleError = console.error

    let debugCalled = false
    let infoCalled = false
    let errorCalled = false

    console.debug = () => {
      debugCalled = true
    }
    console.info = () => {
      infoCalled = true
    }
    console.error = () => {
      errorCalled = true
    }

    try {
      const logger = createConsoleLogger('test-logger')
      logger.debug('debug message')
      logger.info('info message')
      logger.error('error message')

      expect(debugCalled).toBe(true)
      expect(infoCalled).toBe(true)
      expect(errorCalled).toBe(true)
    } finally {
      console.debug = originalConsoleDebug
      console.info = originalConsoleInfo
      console.error = originalConsoleError
    }
  })
})
