import z from 'zod'

/**
 * A schema for a logger.
 *
 * @category Logger
 */
export const zLogger = z.object({
  debug: z.custom<Logger['debug']>((val) => {
    if (typeof val !== 'function') {
      return false
    }
    return true
  }),
  info: z.custom<Logger['info']>((val) => {
    if (typeof val !== 'function') {
      return false
    }
    return true
  }),
  error: z.custom<Logger['error']>((val) => {
    if (typeof val !== 'function') {
      return false
    }
    return true
  }),
})

/**
 * Logger interface for durable execution system events and diagnostics.
 *
 * The logger receives important events from the executor including task lifecycle
 * events, errors, background process activity, and debug information.
 *
 * ## Log Levels
 *
 * - **debug**: Detailed diagnostic information (only when debug enabled)
 * - **info**: General operational messages (task enqueued, started, completed)
 * - **error**: Error conditions and failures
 *
 * @example
 * ```ts
 * // Custom logger implementation
 * const customLogger: Logger = {
 *   debug: (msg) => console.debug(`[DEBUG] ${msg}`),
 *   info: (msg) => console.info(`[INFO] ${msg}`),
 *   error: (msg, error) => console.error(`[ERROR] ${msg}`, error)
 * }
 *
 * const executor = new DurableExecutor(storage, {
 *   logger: customLogger,
 *   logLevel: 'debug'  // Enable debug logging
 * })
 * ```
 *
 * @category Logger
 */
export type Logger = {
  debug: (message: string) => void
  info: (message: string) => void
  error: (message: string, error?: unknown) => void
}

/**
 * A schema for a log level.
 *
 * @category Logger
 */
export const zLogLevel = z.enum(['debug', 'info', 'error'])

/**
 * Log levels for the logger. It's used to determine which messages to log. Any message with a level
 * more than or equal to the current level will be logged. The order of the levels is:
 * - `debug`
 * - `info`
 * - `error`
 *
 * @category Logger
 */
export type LogLevel = 'debug' | 'info' | 'error'

const levels: Record<LogLevel, number> = {
  debug: 1,
  info: 2,
  error: 3,
}

/**
 * Create a simple console-based logger with service name prefixing.
 *
 * This is the default logger used by the executor when none is specified. Each log message is
 * prefixed with the log level and service name.
 *
 * @example
 * ```ts
 * const logger = createConsoleLogger('MyService')
 * logger.info('Task completed')  // Output: "INFO  [MyService] Task completed"
 * logger.error('Task failed')    // Output: "ERROR [MyService] Task failed"
 *
 * // Use with executor
 * const executor = new DurableExecutor(storage, {
 *   logger: createConsoleLogger('TaskWorker')
 * })
 * ```
 *
 * @param name - Service name to include in log messages (e.g., 'DurableExecutor')
 * @returns A Logger instance that outputs to console
 *
 * @category Logger
 */
export function createConsoleLogger(name: string): Logger {
  return {
    debug: (message) => {
      const datetime = new Date().toISOString()
      console.debug(`DEBUG [${name}] [${datetime}] ${message}`)
    },
    info: (message) => {
      const datetime = new Date().toISOString()
      console.info(`INFO  [${name}] [${datetime}] ${message}`)
    },
    error: (message, error) => {
      const datetime = new Date().toISOString()
      console.error(`ERROR [${name}] [${datetime}] ${message}`, error)
    },
  }
}

/**
 * Internal logger class that can be used to log messages.
 *
 * This class is used to log messages in the executor. It is used by the executor to log messages
 * to the console.
 *
 * @category Logger
 * @internal
 */
export class LoggerInternal {
  private readonly logger: Logger
  private readonly level: number

  constructor(logger?: Logger | null, level?: LogLevel | null, name?: string | null) {
    this.logger = logger ?? createConsoleLogger(name ?? 'DurableExecutor')
    this.level = levels[level ?? 'info'] || levels.info
  }

  debug(message: string): void {
    if (this.level <= levels.debug) {
      this.logger.debug(message)
    }
  }

  info(message: string): void {
    if (this.level <= levels.info) {
      this.logger.info(message)
    }
  }

  error(message: string, error?: unknown): void {
    if (this.level <= levels.error) {
      if (error) {
        this.logger.error(message, error)
      } else {
        this.logger.error(message)
      }
    }
  }
}
